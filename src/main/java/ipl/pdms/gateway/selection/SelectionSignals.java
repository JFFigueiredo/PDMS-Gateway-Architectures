package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static java.lang.Boolean.TRUE;

import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Signals;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SelectionSignals {
    private final RoutingTable routing;
    private final BackendHealth health;
    private final TrafficStatistics stats;

    public Signals read(Configuration config) {
        if (config.source().equals("mock")) {
            final MockSignals m = config.mock();
            return new Signals(m.sampleTimestampEpochMs(), m.samples(), m.p99Ms(), m.arrivalRps(), m.pending(),
                    m.valid(), m.healthy(), m.gatewayPressure(), m.unexpectedErrorRatio());
        }
        final Map<String, Object> window = stats.publishedSnapshot(config.tuning().windowSeconds());
        final Map<String, Boolean> healthy = new LinkedHashMap<>();
        for (final String a : ARCHITECTURES) {
            healthy.put(a, routing.current().knows(a) && health.realisation(a) == UP);
        }
        final Object timestamp = window.get("source_timestamp_epoch_ms");
        final boolean workloadMatches = window.get("plan_arrivals") instanceof Map<?, ?> plans
                && plans.keySet().equals(Set.of(config.workload()));
        return new Signals(timestamp == null ? 0 : ((Number)timestamp).longValue(), ((Number)window.get("n")).intValue(),
                (Double)window.get("p99_ms"), ((Number)window.get("lambda_request_rps")).doubleValue(),
                ((Number)window.get("pending")).intValue(), TRUE.equals(window.get("valid")) && workloadMatches,
                healthy, false);
    }

}
