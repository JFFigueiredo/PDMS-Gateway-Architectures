package ipl.pdms.gateway.admission;

import static ipl.pdms.gateway.admission.AdmissionRejectedException.FULL;
import static ipl.pdms.gateway.admission.AdmissionRejectedException.TIMEOUT;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import ipl.pdms.gateway.config.GatewayClock;
import java.time.Duration;
import java.util.ArrayDeque;
import lombok.RequiredArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

/** Tickets retain permit ownership across cancellation, including cancellation just after a grant. */
@RequiredArgsConstructor
public final class AdmissionLimiter {

    public static final class Ticket {
        private enum State { NEW, WAITING, GRANTED, DONE }

        private State state = State.NEW;
        private MonoSink<Long> sink;
        private long enqueuedNanos;
        private long waitedNanos;
        private volatile Disposable timer;
    }

    private final int maxInFlight;
    private final int queueCapacity;
    private final Duration maxWait;
    private final Scheduler scheduler;
    private final GatewayClock clock;
    private final Object lock = new Object();
    private final ArrayDeque<Ticket> queue = new ArrayDeque<>();
    private int inFlight;


    public Ticket newTicket() {
        return new Ticket();
    }

    /** Emits queue wait in nanoseconds, or fails when admission is refused. */
    public Mono<Long> acquire(Ticket ticket) {
        return Mono.create(sink -> {
            boolean immediate = false;
            boolean full = false;
            synchronized (lock) {
                if (inFlight < maxInFlight) {
                    inFlight++;
                    ticket.state = Ticket.State.GRANTED;
                    immediate = true;
                } else if (queue.size() < queueCapacity) {
                    ticket.state = Ticket.State.WAITING;
                    ticket.sink = sink;
                    ticket.enqueuedNanos = clock.nanoTime();
                    queue.addLast(ticket);
                } else {
                    ticket.state = Ticket.State.DONE;
                    full = true;
                }
            }
            if (immediate) {
                sink.success(0L);
            } else if (full) {
                sink.error(new AdmissionRejectedException(FULL, 0));
            } else {
                ticket.timer = scheduler.schedule(() -> timeout(ticket), maxWait.toNanos(), NANOSECONDS);
            }
        });
    }

    private void timeout(Ticket ticket) {
        long waited;
        synchronized (lock) {
            if (ticket.state != Ticket.State.WAITING) {
                return;
            }
            queue.remove(ticket);
            ticket.state = Ticket.State.DONE;
            waited = clock.nanoTime() - ticket.enqueuedNanos;
        }
        ticket.sink.error(new AdmissionRejectedException(TIMEOUT, waited));
    }

    public void release(Ticket ticket) {
        Ticket next = null;
        synchronized (lock) {
            switch (ticket.state) {
                case GRANTED -> {
                    ticket.state = Ticket.State.DONE;
                    next = queue.pollFirst();
                    if (next != null) {
                        next.state = Ticket.State.GRANTED;
                        next.waitedNanos = clock.nanoTime() - next.enqueuedNanos;
                    } else {
                        inFlight--;
                    }
                }
                case WAITING -> {
                    queue.remove(ticket);
                    ticket.state = Ticket.State.DONE;
                }
                default -> {
                }
            }
        }
        if (next != null) {
            final Disposable timer = next.timer;
            if (timer != null) {
                timer.dispose();
            }
            next.sink.success(next.waitedNanos);
        }
    }

    public int inFlight() {
        synchronized (lock) {
            return inFlight;
        }
    }

    public int queued() {
        synchronized (lock) {
            return queue.size();
        }
    }
}
