package ipl.pdms.gateway.admission;

import static ipl.pdms.gateway.admission.HandoverQueue.DISABLED;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.publisher.Mono.error;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.core.publisher.Mono.never;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

class HandoverQueueTest {
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    final HandoverQueue queue = new HandoverQueue(1, ofSeconds(1), meters, scheduler,
            () -> scheduler.now(NANOSECONDS));

    @AfterEach void close() {
        scheduler.dispose();
        meters.close();
    }
    int queued() {
        return (int) meters.get("pdms_dyn_handover_queued").gauge().value();
    }

    @Test void successfulAdmissionDoesNotWaitOrRetry() {
        AtomicInteger calls = new AtomicInteger();
        queue.forward(waited -> fromRunnable(calls::incrementAndGet), () -> false).block();
        assertEquals(1, calls.get()); assertEquals(0, queued());
    }

    @Test void waitsBeforeForwardingAndReleasesQueueSlotBeforeResponseCompletes() {
        AtomicBoolean ready = new AtomicBoolean(); AtomicInteger forwards = new AtomicInteger();
        var subscription = queue.forward(waited -> ready.get()
                ? fromRunnable(forwards::incrementAndGet).then(never()) : null, () -> true).subscribe();
        assertEquals(1, queued()); assertEquals(0, forwards.get());
        ready.set(true); scheduler.advanceTimeBy(ofMillis(20));
        assertEquals(1, forwards.get()); assertEquals(0, queued());
        assertEquals(1, meters.get("pdms_dyn_handover_wait").tag("result", "forwarded").timer().count());
        subscription.dispose(); assertEquals(0, queued());
    }

    @Test void fullQueueDoesNotForwardAnotherRequest() {
        var first = queue.forward(waited -> null, () -> true).subscribe();
        StepVerifier.create(queue.forward(waited -> null, () -> true))
                .expectErrorMatches(e -> e.getMessage().equals("handover_queue_full")).verify();
        assertEquals(1, queued()); first.dispose(); assertEquals(0, queued());
    }

    @Test void timeoutIsBoundedAndReturnsQueueSlot() {
        AtomicInteger attempts = new AtomicInteger();
        var response = queue.forward(waited -> {
            attempts.incrementAndGet();
            return null;
        }, () -> true);
        StepVerifier.create(response).then(() -> scheduler.advanceTimeBy(ofSeconds(1)))
                .expectErrorMatches(e -> e.getMessage().equals("handover_wait_timeout")).verify();
        assertEquals(0, queued()); int stopped = attempts.get();
        scheduler.advanceTimeBy(ofSeconds(2)); assertEquals(stopped, attempts.get());
    }

    @Test void cancellationRemovesWaiterAndNeverForwardsItLater() {
        AtomicBoolean ready = new AtomicBoolean(); AtomicInteger forwards = new AtomicInteger();
        var first = queue.forward(waited -> ready.get() ? fromRunnable(forwards::incrementAndGet) : null,
                () -> true).subscribe();
        first.dispose(); ready.set(true); scheduler.advanceTimeBy(ofSeconds(2));
        assertEquals(0, forwards.get()); assertEquals(0, queued());
    }

    @Test void failedHandoverRejectsWithoutForwardingOrLeakingWaiter() {
        AtomicBoolean transitioning = new AtomicBoolean(true);
        StepVerifier.create(queue.forward(waited -> null, transitioning::get))
                .then(() -> {
                    transitioning.set(false);
                    scheduler.advanceTimeBy(ofMillis(20));
                })
                .expectErrorMatches(e -> e.getMessage().equals(DISABLED)).verify();
        assertEquals(0, queued());
    }

    @Test void disabledRuntimeNeverQueues() {
        StepVerifier.create(queue.forward(waited -> null, () -> false))
                .expectErrorMatches(e -> e.getMessage().equals(DISABLED)).verify();
        assertEquals(0, queued());
    }

    @Test void forwardingFailureIsNotRetried() {
        AtomicInteger forwards = new AtomicInteger();
        StepVerifier.create(queue.forward(waited -> fromRunnable(forwards::incrementAndGet)
                        .then(error(new IllegalStateException("uncertain write"))), () -> true))
                .expectErrorMessage("uncertain write").verify();
        assertEquals(1, forwards.get()); assertEquals(0, queued());
    }
}
