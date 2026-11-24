package ipl.pdms.gateway;

import static ipl.pdms.gateway.health.HealthState.Status.DOWN;
import static ipl.pdms.gateway.health.HealthState.Status.UNKNOWN;
import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ipl.pdms.gateway.health.HealthState;
import org.junit.jupiter.api.Test;

class HealthStateTest {

    private static void ok(HealthState s, long now) {
        s.record(true, null, now, 3, 2);
    }

    private static void fail(HealthState s, long now) {
        s.record(false, "HttpTimeoutException", now, 3, 2);
    }

    @Test
    void startsUnknownAndNeedsTwoSuccessesToBeUp() {
        HealthState s = new HealthState();
        assertEquals(UNKNOWN, s.status());
        ok(s, 1);
        assertEquals(UNKNOWN, s.status());
        ok(s, 2);
        assertEquals(UP, s.status());
        assertEquals(2, s.view().lastChangeEpochMs());
    }

    @Test
    void upBackendNeedsThreeConsecutiveFailuresToBeDown() {
        HealthState s = new HealthState();
        ok(s, 1);
        ok(s, 2);
        fail(s, 3);
        fail(s, 4);
        assertEquals(UP, s.status());
        ok(s, 5);
        fail(s, 6);
        fail(s, 7);
        assertEquals(UP, s.status());
        fail(s, 8);
        assertEquals(DOWN, s.status());
        assertEquals(8, s.view().lastChangeEpochMs());
        assertEquals("HttpTimeoutException", s.view().lastError());
    }

    @Test
    void downBackendNeedsTwoConsecutiveSuccessesToRecover() {
        HealthState s = new HealthState();
        fail(s, 1);
        fail(s, 2);
        fail(s, 3);
        assertEquals(DOWN, s.status());
        ok(s, 4);
        fail(s, 5);
        ok(s, 6);
        assertEquals(DOWN, s.status());
        ok(s, 7);
        assertEquals(UP, s.status());
    }

    @Test
    void unknownBackendWithFailuresOnlyBecomesDownAfterTheThreshold() {
        HealthState s = new HealthState();
        fail(s, 1);
        fail(s, 2);
        assertEquals(UNKNOWN, s.status());
        fail(s, 3);
        assertEquals(DOWN, s.status());
    }
}
