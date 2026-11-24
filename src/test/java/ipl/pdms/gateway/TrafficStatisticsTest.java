package ipl.pdms.gateway;

import static ipl.pdms.gateway.config.GatewayProperties.forTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TrafficStatisticsTest {

    @Test
    void r7QuantilesMatchTheFixtureOfTheAnalysisSpec() {
        var clock = new FakeClock(10_000_000L);
        var statistics = new TrafficStatistics(clock, props(100));
        for (var duration : new double[]{20, 0, 30, 10}) {
            statistics.arrival("mix");
            statistics.completion(duration, false);
        }
        clock.advanceMillis(1);
        var snapshot = statistics.snapshot(30);
        assertEquals(15.0, (Double) snapshot.get("p50_ms"), 1e-9);
        assertEquals(29.7, (Double) snapshot.get("p99_ms"), 1e-9);
    }

    @Test
    void emptyAndSingleSampleBoundaries() {
        var clock = new FakeClock(10_000_000L);
        var statistics = new TrafficStatistics(clock, props(100));
        assertNull(statistics.snapshot(30).get("p50_ms"));
        assertNull(statistics.snapshot(30).get("p99_ms"));
        statistics.arrival("mix");
        statistics.completion(7, false);
        clock.advanceMillis(1);
        var snapshot = statistics.snapshot(30);
        assertEquals(7.0, (Double) snapshot.get("p50_ms"), 1e-9);
        assertEquals(7.0, (Double) snapshot.get("p99_ms"), 1e-9);
    }

    @Test
    void windowCountsArrivalsPendingAndErrors() {
        FakeClock clock = new FakeClock(10_000_000L);
        TrafficStatistics statistics = new TrafficStatistics(clock, props(100));
        statistics.arrival("mix");
        statistics.arrival("read");
        clock.advanceMillis(10);
        statistics.completion(5.0, false);
        clock.advanceMillis(10);
        statistics.completion(15.0, true);
        clock.advanceMillis(10);

        Map<String, Object> s = statistics.snapshot(30);
        assertEquals(2, s.get("n"));
        assertEquals(1, s.get("errors"));
        assertEquals(2, s.get("n_arrivals"));
        assertEquals(0, s.get("pending"));
        assertEquals(Map.of("mix", 1, "read", 1), s.get("plan_arrivals"));
        assertEquals(10.0, (Double) s.get("p50_ms"), 1e-9);
        assertTrue((Boolean) s.get("valid"));
    }

    @Test
    void overflowIsCountedAndInvalidatesTheWindowInsteadOfOverwriting() {
        FakeClock clock = new FakeClock(10_000_000L);
        TrafficStatistics statistics = new TrafficStatistics(clock, props(2));
        statistics.arrival("mix");
        statistics.arrival("mix");
        statistics.arrival("mix");
        assertEquals(1, statistics.overflowCount());
        clock.advanceMillis(1);
        Map<String, Object> s = statistics.snapshot(30);
        assertEquals(2, s.get("n_arrivals"));
        assertFalse((Boolean) s.get("valid"));
    }

    private static GatewayProperties props(int capacity) {
        return forTest("x", capacity);
    }
}
