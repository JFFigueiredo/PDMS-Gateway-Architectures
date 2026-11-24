package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.Tuning.defaults;
import static java.lang.Math.max;

import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Synthetic profiles exercise policy branches; they are not benchmark rankings. */
public final class SelectionScenarios {
    public static final Set<String> NAMES = Set.of("stable", "read-crossover", "write-crossover", "split-crossover",
            "anchoring-required", "outage", "errors", "stale", "no-eligible", "tie", "gateway-pressure");
    private SelectionScenarios() {}
    public static Configuration create(String name, String active, long now) {
        if (!NAMES.contains(name)) {
            throw new SelectionService.ControlException(400, "unknown_scenario");
        }
        final var workload = scenarioWorkload(name);
        final var predictions = predictedLatencies(name);
        final Map<String, Boolean> healthy = new LinkedHashMap<>();
        ARCHITECTURES.forEach(a -> healthy.put(a,
            !name.equals("no-eligible") && (!name.equals("outage") || !a.equals(active))));
        final var mock = new MockSignals(name.equals("stale") ? max(0, now - 60000) : now,
            1200, predictions.get("monolith") == 300 ? 350d : 100d, 10, 0, true,
            healthy, name.equals("gateway-pressure"), name.equals("errors") ? .20 : null);
        return new Configuration(true, false, false, "mock", workload, scenarioCapabilities(name),
            scenarioProfiles(name, workload, predictions), defaults(), mock);
    }

    private static String scenarioWorkload(String name) {
        return switch (name) {
            case "read-crossover", "split-crossover", "stale" -> "read";
            case "write-crossover" -> "write";
            default -> "mix";
        };
    }

    private static Set<String> scenarioCapabilities(String name) {
        return switch (name) {
            case "write-crossover" -> Set.of("person_deleted_event");
            case "split-crossover" -> Set.of("read_your_writes_snapshot");
            case "anchoring-required" -> Set.of("consent_integrity_anchor");
            default -> Set.of();
        };
    }

    private static Map<String, Double> predictedLatencies(String name) {
        final Map<String, Double> predictions = new LinkedHashMap<>(Map.of("monolith", 100d,
            "microservices", 200d, "microservices-async", 180d, "faas", 150d, "decentralized", 250d));
        predictions.putAll(switch (name) {
            case "read-crossover", "stale" -> Map.of("monolith", 300d, "faas", 80d);
            case "write-crossover" -> Map.of("monolith", 300d, "microservices-async", 80d);
            case "split-crossover" -> Map.of("monolith", 300d, "microservices", 80d);
            case "tie" -> Map.of("monolith", 100d, "microservices", 100d, "microservices-async", 100d,
                "faas", 100d, "decentralized", 100d);
            default -> Map.of();
        });
        return predictions;
    }

    private static Map<String, Map<String, Profile>> scenarioProfiles(String name, String workload,
                                                                     Map<String, Double> predictions) {
        final Map<String, Map<String, Profile>> profiles = new LinkedHashMap<>();
        ARCHITECTURES.forEach(a -> profiles.put(a, Map.of(workload,
            new Profile("synthetic", "gateway-fixture/v1/" + name, 0, 100, 100, predictions.get(a)))));
        return profiles;
    }
}
