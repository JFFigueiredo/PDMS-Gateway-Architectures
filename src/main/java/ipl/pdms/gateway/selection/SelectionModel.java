package ipl.pdms.gateway.selection;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.With;

public final class SelectionModel {
    private SelectionModel() {}
    public static final List<String> ARCHITECTURES = List.of("monolith", "microservices", "microservices-async", "faas", "decentralized");
    public static final Set<String> WORKLOADS = Set.of("mix", "read", "write");
    public static final Set<String> CAPABILITIES = Set.of("consent_integrity_anchor", "read_your_writes_snapshot",
            "person_update_persists_embedded_consents", "person_deleted_event");
    public static final Map<String, Set<String>> DECLARED_CAPABILITIES = Map.of(
            "monolith", Set.of("read_your_writes_snapshot", "person_update_persists_embedded_consents", "person_deleted_event"),
            "faas", Set.of("read_your_writes_snapshot", "person_update_persists_embedded_consents", "person_deleted_event"),
            "microservices", Set.of("read_your_writes_snapshot", "person_deleted_event"),
            "microservices-async", Set.of("person_deleted_event"),
            "decentralized", CAPABILITIES);

    public record Tuning(long tickMs, int windowSeconds, int minSamples, long maxAgeMs,
                         double highP99Ms, double recoveryP99Ms, int breachTicks, int opportunityTicks,
                         long dwellMs, double maxUtilization, double improvementFraction,
                         double improvementMs, int maxSwitches, long flapWindowMs, double highErrorRatio) {
        public Tuning(long tickMs, int windowSeconds, int minSamples, long maxAgeMs, double highP99Ms,
                      double recoveryP99Ms, int breachTicks, int opportunityTicks, long dwellMs,
                      double maxUtilization, double improvementFraction, double improvementMs, int maxSwitches, long flapWindowMs) {
            this(tickMs, windowSeconds, minSamples, maxAgeMs, highP99Ms, recoveryP99Ms, breachTicks, opportunityTicks,
                    dwellMs, maxUtilization, improvementFraction, improvementMs, maxSwitches, flapWindowMs, .05);
        }
        public static Tuning defaults() {
            return new Tuning(5000, 30, 1000, 15000, 250, 200, 2, 6, 120000, .85, .20, 10, 3, 600000);
        }
    }
    public record Profile(String evidenceKind, String provenance, double minRateRps, double maxRateRps,
                          double capacityRps, double predictedP99Ms) {}
    public record MockSignals(long sampleTimestampEpochMs, int samples, Double p99Ms, double arrivalRps,
                              int pending, boolean valid, Map<String, Boolean> healthy,
                              boolean gatewayPressure, Double unexpectedErrorRatio) {
        public MockSignals(long sampleTimestampEpochMs, int samples, Double p99Ms, double arrivalRps,
                           int pending, boolean valid, Map<String, Boolean> healthy, boolean gatewayPressure) {
            this(sampleTimestampEpochMs, samples, p99Ms, arrivalRps, pending, valid, healthy, gatewayPressure, null);
        }
        public MockSignals {
            if (healthy != null) {
                healthy = Map.copyOf(healthy);
            }
        }
    }
    @With
    public record Configuration(boolean enabled, boolean autoEvaluate, boolean applyChanges, String source,
                                String workload, Set<String> requiredCapabilities,
                                Map<String, Map<String, Profile>> profiles, Tuning tuning, MockSignals mock) {
        public Configuration {
            if (requiredCapabilities != null) {
                requiredCapabilities = Set.copyOf(requiredCapabilities);
            }
            if (profiles != null) {
                final var copy = new java.util.LinkedHashMap<String, Map<String, Profile>>();
                profiles.forEach((name, values) -> copy.put(name, Map.copyOf(values)));
                profiles = Map.copyOf(copy);
            }
        }
        public static Configuration defaults() {
            return new Configuration(false, false, false, "real", "mix", Set.of(), Map.of(), Tuning.defaults(), null);
        }
    }
    public record Signals(long sampledAtEpochMs, int samples, Double p99Ms, double arrivalRps, int pending,
                          boolean valid, Map<String, Boolean> healthy, boolean gatewayPressure, Double unexpectedErrorRatio) {
        public Signals(long sampledAtEpochMs, int samples, Double p99Ms, double arrivalRps, int pending,
                       boolean valid, Map<String, Boolean> healthy, boolean gatewayPressure) {
            this(sampledAtEpochMs, samples, p99Ms, arrivalRps, pending, valid, healthy, gatewayPressure, null);
        }
    }
    public record Candidate(String architecture, boolean eligible, String reason, Double predictedP99Ms,
                            Double utilization, String evidenceKind) {}
    public record Decision(String action, String reason, String active, String target, String source,
                           String evidenceKind, long evaluatedAtEpochMs, List<Candidate> candidates,
                           int breachCount, int opportunityCount) {}
}
