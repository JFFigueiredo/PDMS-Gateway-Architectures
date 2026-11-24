package ipl.pdms.gateway.admission;

import static java.lang.Math.min;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static reactor.core.publisher.Mono.defer;
import static reactor.core.publisher.Mono.delay;
import static reactor.core.publisher.Mono.error;
import static reactor.core.publisher.SignalType.CANCEL;
import static reactor.core.scheduler.Schedulers.parallel;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Component
public class HandoverQueue {
    public static final String DISABLED = "architecture_disabled_or_transitioning";
    private final int capacity;
    private final long maxWaitNanos;
    private final MeterRegistry registry;
    private final Scheduler scheduler;
    private final LongSupplier nanoTime;
    private final AtomicInteger queued = new AtomicInteger();

    @Autowired
    public HandoverQueue(@Value("${pdms.gateway.runtime.handover-queue-capacity:200}") int capacity,
                         @Value("${pdms.gateway.runtime.handover-max-wait:5s}") Duration maxWait,
                         MeterRegistry registry) {
        this(capacity, maxWait, registry, parallel(), System::nanoTime);
    }

    HandoverQueue(int capacity, Duration maxWait, MeterRegistry registry, Scheduler scheduler,
                  LongSupplier nanoTime) {
        if (capacity < 0 || maxWait == null || maxWait.isNegative() || maxWait.isZero()) {
            throw new IllegalArgumentException("Invalid handover queue limits");
        }
        this.capacity = capacity; this.maxWaitNanos = maxWait.toNanos();
        this.registry = registry; this.scheduler = scheduler; this.nanoTime = nanoTime;
        Gauge.builder("pdms_dyn_handover_queued", queued, AtomicInteger::get).register(registry);
    }

    /** A null attempt means admission failed before forwarding; only then is retry safe. */
    public Mono<Void> forward(LongFunction<Mono<Void>> attempt, BooleanSupplier transitioning) {
        return defer(() -> {
            final Mono<Void> immediate = attempt.apply(0);
            if (immediate != null) {
                return immediate;
            }
            if (!transitioning.getAsBoolean()) {
                return error(new Rejected(DISABLED));
            }
            if (queued.getAndIncrement() >= capacity) {
                queued.decrementAndGet();
                return error(new Rejected("handover_queue_full"));
            }
            final Ticket ticket = new Ticket();
            return retry(attempt, transitioning, ticket)
                    .doFinally(signal -> ticket.release(signal == CANCEL
                            ? "cancelled" : "rejected"));
        });
    }

    private Mono<Void> retry(LongFunction<Mono<Void>> attempt, BooleanSupplier transitioning, Ticket ticket) {
        return defer(() -> {
            final long waited = nanoTime.getAsLong() - ticket.started;
            if (waited >= maxWaitNanos) {
                return error(new Rejected("handover_wait_timeout"));
            }
            final Mono<Void> next = attempt.apply(waited);
            if (next != null) {
                ticket.release("forwarded");
                return next;
            }
            if (!transitioning.getAsBoolean()) {
                return error(new Rejected(DISABLED));
            }
            return delay(ofNanos(min(MILLISECONDS.toNanos(10), maxWaitNanos - waited)), scheduler)
                    .then(retry(attempt, transitioning, ticket));
        });
    }

    private final class Ticket {
        final long started = nanoTime.getAsLong();
        final AtomicBoolean released = new AtomicBoolean();
        void release(String result) {
            if (released.compareAndSet(false, true)) {
                queued.decrementAndGet();
                registry.timer("pdms_dyn_handover_wait", "result", result)
                        .record(nanoTime.getAsLong() - started, NANOSECONDS);
            }
        }
    }

    public static final class Rejected extends RuntimeException {
        public Rejected(String reason) {
            super(reason);
        }
    }
}
