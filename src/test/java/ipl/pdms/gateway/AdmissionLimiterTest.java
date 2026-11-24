package ipl.pdms.gateway;

import static ipl.pdms.gateway.admission.AdmissionRejectedException.FULL;
import static ipl.pdms.gateway.admission.AdmissionRejectedException.TIMEOUT;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ipl.pdms.gateway.admission.AdmissionLimiter;
import ipl.pdms.gateway.admission.AdmissionRejectedException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.scheduler.VirtualTimeScheduler;

class AdmissionLimiterTest {

    private FakeClock clock;
    private VirtualTimeScheduler scheduler;
    private AdmissionLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(0);
        scheduler = VirtualTimeScheduler.create();
        limiter = new AdmissionLimiter(2, 2, ofMillis(500), scheduler, clock);
    }

    private void advance(long millis) {
        clock.advanceMillis(millis);
        scheduler.advanceTimeBy(ofMillis(millis));
    }

    private static final class Call {
        final AdmissionLimiter.Ticket ticket;
        final AtomicReference<Long> waited = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        Disposable subscription;

        Call(AdmissionLimiter limiter) {
            ticket = limiter.newTicket();
        }
    }

    private Call start() {
        Call call = new Call(limiter);
        call.subscription = limiter.acquire(call.ticket).subscribe(call.waited::set, call.error::set);
        return call;
    }

    @Test
    void admitsUpToTheLimitAtOnce() {
        Call a = start();
        Call b = start();
        assertEquals(0L, a.waited.get());
        assertEquals(0L, b.waited.get());
        assertEquals(2, limiter.inFlight());
        assertEquals(0, limiter.queued());
    }

    @Test
    void queuesInArrivalOrderAndHandsOverOnRelease() {
        Call a = start();
        Call b = start();
        Call c = start();
        Call d = start();
        assertNull(c.waited.get());
        assertEquals(2, limiter.queued());
        advance(100);
        limiter.release(a.ticket);
        assertEquals(100_000_000L, c.waited.get());
        assertNull(d.waited.get());
        assertEquals(2, limiter.inFlight());
        advance(50);
        limiter.release(b.ticket);
        assertEquals(150_000_000L, d.waited.get());
        limiter.release(c.ticket);
        limiter.release(d.ticket);
        assertEquals(0, limiter.inFlight());
        assertEquals(0, limiter.queued());
    }

    @Test
    void refusesWhenTheQueueIsFull() {
        start();
        start();
        start();
        start();
        Call e = start();
        AdmissionRejectedException rejection = assertInstanceOf(AdmissionRejectedException.class, e.error.get());
        assertEquals(FULL, rejection.reason());
        assertEquals(2, limiter.queued());
        limiter.release(e.ticket);
        assertEquals(2, limiter.inFlight());
        assertEquals(2, limiter.queued());
    }

    @Test
    void refusesAfterTheMaximumWait() {
        start();
        start();
        Call c = start();
        advance(499);
        assertNull(c.error.get());
        advance(1);
        AdmissionRejectedException rejection = assertInstanceOf(AdmissionRejectedException.class, c.error.get());
        assertEquals(TIMEOUT, rejection.reason());
        assertEquals(500_000_000L, rejection.waitedNanos());
        assertEquals(0, limiter.queued());
        assertEquals(2, limiter.inFlight());
    }

    @Test
    void cancellingWhileWaitingFreesTheQueuePlace() {
        start();
        start();
        Call c = start();
        c.subscription.dispose();
        limiter.release(c.ticket);
        assertEquals(0, limiter.queued());
        assertEquals(2, limiter.inFlight());
    }

    @Test
    void aGrantThatReachesACancelledSubscriberIsStillReturnedByRelease() {
        Call a = start();
        start();
        Call c = start();
        c.subscription.dispose();
        limiter.release(a.ticket);
        assertEquals(2, limiter.inFlight());
        limiter.release(c.ticket);
        assertEquals(1, limiter.inFlight());
        assertTrue(limiter.queued() == 0);
    }

    @Test
    void releaseIsIdempotent() {
        Call a = start();
        limiter.release(a.ticket);
        limiter.release(a.ticket);
        assertEquals(0, limiter.inFlight());
        start();
        start();
        assertEquals(2, limiter.inFlight());
    }
}
