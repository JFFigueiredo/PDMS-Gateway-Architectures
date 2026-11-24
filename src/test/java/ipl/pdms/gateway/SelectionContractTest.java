package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.DECLARED_CAPABILITIES;
import static ipl.pdms.gateway.selection.SelectionService.validate;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Locale.ROOT;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.experiment.ExperimentControl;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.selection.SelectionModel.Candidate;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import ipl.pdms.gateway.selection.SelectionModel.Signals;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;
import ipl.pdms.gateway.selection.SelectionPolicy;
import ipl.pdms.gateway.selection.SelectionPolicyVersion;
import ipl.pdms.gateway.selection.SelectionScenarios;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SelectionContractTest {
    private static final long NOW = 1_000_000;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path TRACE = Path.of("target", "t12-selection-contract", "decisions.jsonl");

    @BeforeAll static void clearTrace() throws Exception {
        createDirectories(TRACE.getParent());
        deleteIfExists(TRACE);
    }

    private static RoutingSnapshot routing(String active, Set<String> disabled) {
        Map<String, Map<String, String>> backends = new HashMap<>();
        ARCHITECTURES.forEach(a -> backends.put(a, Map.of("default", "http://localhost:8080")));
        return new RoutingSnapshot("routing/1", 0, active, "request", backends, disabled);
    }
    private static Signals signal(MockSignals m, long sampledAt, Map<String, Boolean> healthy, Double errors) {
        return new Signals(sampledAt, m.samples(), m.p99Ms(), m.arrivalRps(), m.pending(), m.valid(), healthy, m.gatewayPressure(), errors);
    }
    private static Signals signal(Configuration c, long sampledAt) {
        MockSignals m = c.mock();
        return signal(m, sampledAt, m.healthy(), m.unexpectedErrorRatio());
    }
    private static Map<String, Boolean> health(Set<String> down) {
        Map<String, Boolean> healthy = new LinkedHashMap<>();
        ARCHITECTURES.forEach(a -> healthy.put(a, !down.contains(a)));
        return healthy;
    }
    private static Configuration with(Configuration c, String source, Set<String> required, Map<String, Map<String, Profile>> profiles, Tuning tuning) {
        Configuration next = new Configuration(c.enabled(), c.autoEvaluate(), c.applyChanges(), source, c.workload(),
                required, profiles, tuning, c.mock());
        validate(next);
        return next;
    }
    private static Profile profile(double min, double max, double capacity, double p99) {
        return new Profile("synthetic", "t12-contract-fixture", min, max, capacity, p99);
    }
    private static Map<String, String> reasons(Decision d) {
        Map<String, String> result = new LinkedHashMap<>();
        d.candidates().forEach(c -> result.put(c.architecture(), c.reason()));
        return result;
    }
    private static Decision record(String stage, Decision d) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("stage", stage); row.put("decision", d);
        writeString(TRACE, JSON.writeValueAsString(row) + "\n", CREATE, APPEND);
        return d;
    }

    @Test void fullTiesRetainEveryActiveArchitectureRegardlessOfFixedOrder() throws Exception {
        Configuration tie = SelectionScenarios.create("tie", "monolith", NOW);
        for (String active : ARCHITECTURES) {
            Decision d = record("tie_active_" + active,
                    new SelectionPolicy().evaluate(tie, signal(tie, NOW), routing(active, Set.of()), NOW));
            assertEquals("hold", d.action()); assertEquals("keep_active", d.reason()); assertEquals(active, d.target());
        }
    }

    @Test void tiedAlternativesFollowTheFixedArchitectureOrderOnBothSwitchPaths() throws Exception {
        Configuration base = SelectionScenarios.create("read-crossover", "monolith", NOW);
        var profiles = new HashMap<>(base.profiles());
        for (String tied : List.of("microservices", "faas", "decentralized")) {
            profiles.put(tied, Map.of("read", profile(0, 100, 100, 80)));
        }
        Configuration performance = with(base, "mock", Set.of(), profiles, base.tuning());
        SelectionPolicy policy = new SelectionPolicy();
        assertEquals("debounce", record("performance_tie_tick1",
                policy.evaluate(performance, signal(performance, NOW), routing("monolith", Set.of()), NOW)).reason());
        Decision switched = record("performance_tie_tick2",
                policy.evaluate(performance, signal(performance, NOW + 5000), routing("monolith", Set.of()), NOW + 5000));
        assertEquals("switch", switched.action()); assertEquals("latency_breach", switched.reason());
        assertEquals("microservices", switched.target());

        Configuration tie = SelectionScenarios.create("tie", "monolith", NOW);
        Signals down = signal(tie.mock(), NOW, health(Set.of("monolith")), null);
        Decision forced = record("forced_tie_any_capability",
                new SelectionPolicy().evaluate(tie, down, routing("monolith", Set.of()), NOW));
        assertEquals("availability_or_requirement", forced.reason()); assertEquals("microservices", forced.target());
        Configuration persistence = with(tie, "mock", Set.of("person_update_persists_embedded_consents"), tie.profiles(), tie.tuning());
        Decision restricted = record("forced_tie_embedded_consent_persistence",
                new SelectionPolicy().evaluate(persistence, down, routing("monolith", Set.of()), NOW));
        assertEquals("availability_or_requirement", restricted.reason()); assertEquals("faas", restricted.target());
        assertEquals(Set.of("faas", "decentralized"), restricted.candidates().stream().filter(Candidate::eligible)
                .map(Candidate::architecture).collect(toSet()));
    }

    @Test void profileSupportAndHeadroomBoundsAreInclusive() throws Exception {
        Configuration base = SelectionScenarios.create("stable", "monolith", NOW);
        Tuning t = base.tuning();
        Tuning half = new Tuning(t.tickMs(), t.windowSeconds(), t.minSamples(), t.maxAgeMs(), t.highP99Ms(), t.recoveryP99Ms(),
                t.breachTicks(), t.opportunityTicks(), t.dwellMs(), .5, t.improvementFraction(), t.improvementMs(), t.maxSwitches(), t.flapWindowMs());
        Map<String, Map<String, Profile>> profiles = Map.of(
                "monolith", Map.of("mix", profile(0, 10, 20, 100)),
                "faas", Map.of("mix", profile(10, 20, 20, 100)),
                "microservices", Map.of("mix", profile(0, 9.999, 20, 100)),
                "microservices-async", Map.of("mix", profile(0, 10, 19.99, 100)),
                "decentralized", Map.of("mix", profile(10.001, 20, 20, 100)));
        Configuration c = with(base, "mock", Set.of(), profiles, half);
        assertEquals(10, c.mock().arrivalRps());
        Decision d = record("inclusive_bounds", new SelectionPolicy().evaluate(c, signal(c, NOW), routing("monolith", Set.of()), NOW));
        assertEquals(Map.of("monolith", "eligible", "faas", "eligible", "microservices", "outside_profile_support",
                "microservices-async", "insufficient_headroom", "decentralized", "outside_profile_support"), reasons(d));
        assertEquals(.5, d.candidates().get(0).utilization());
        assertEquals("keep_active", d.reason()); assertEquals("monolith", d.target());
    }

    @Test void exclusionReasonsFollowTheDeclaredFilterOrder() throws Exception {
        Configuration base = SelectionScenarios.create("stable", "monolith", NOW);
        Map<String, Map<String, Profile>> profiles = Map.of(
                "monolith", Map.of("mix", profile(0, 100, 100, 100)),
                "microservices", Map.of("mix", profile(0, 100, 100, 100)),
                "decentralized", Map.of("mix", profile(0, 5, 5, 100)));
        Configuration snapshot = with(base, "mock", Set.of("read_your_writes_snapshot"), profiles, base.tuning());
        Signals someDown = signal(base.mock(), NOW, health(Set.of("microservices", "microservices-async")), null);
        Decision first = record("precedence_health_profile",
                new SelectionPolicy().evaluate(snapshot, someDown, routing("monolith", Set.of("microservices")), NOW));
        assertEquals(Map.of("monolith", "eligible", "microservices", "disabled", "microservices-async", "unhealthy",
                "faas", "profile_unknown", "decentralized", "outside_profile_support"), reasons(first));

        Configuration anchoring = with(base, "real", Set.of("consent_integrity_anchor"), profiles, base.tuning());
        Signals erroring = signal(base.mock(), NOW, health(Set.of()), .20);
        Decision second = record("precedence_errors_capability_profile",
                new SelectionPolicy().evaluate(anchoring, erroring, routing("monolith", Set.of()), NOW));
        assertEquals(Map.of("monolith", "active_errors", "microservices", "missing_capability", "microservices-async", "missing_capability",
                "faas", "missing_capability", "decentralized", "synthetic_profile"), reasons(second));
        Signals ready = signal(base.mock(), NOW, health(Set.of()), null);
        Decision third = record("precedence_measured_before_support",
                new SelectionPolicy().evaluate(anchoring, ready, routing("monolith", Set.of()), NOW));
        assertEquals("synthetic_profile", reasons(third).get("decentralized"));
        assertEquals("no_eligible_candidate", third.reason()); assertNull(third.target());

        Configuration noLedgerProfile = with(base, "mock", Set.of("consent_integrity_anchor"),
                Map.of("monolith", profiles.get("monolith")), base.tuning());
        Signals downAndErroring = signal(base.mock(), NOW, health(Set.of("monolith")), .20);
        Decision fourth = record("precedence_unhealthy_errors_profile",
                new SelectionPolicy().evaluate(noLedgerProfile, downAndErroring, routing("monolith", Set.of()), NOW));
        assertEquals("unhealthy", reasons(fourth).get("monolith"));
        assertEquals("profile_unknown", reasons(fourth).get("decentralized"));
    }

    @Test void forcedSwitchesBypassObservationTickAndSwitchGuardsButErrorTriggersNeedFreshData() throws Exception {
        Configuration outage = SelectionScenarios.create("outage", "monolith", NOW);
        Tuning t = outage.tuning();
        Tuning oneSwitch = new Tuning(t.tickMs(), t.windowSeconds(), t.minSamples(), t.maxAgeMs(), t.highP99Ms(), t.recoveryP99Ms(),
                t.breachTicks(), t.opportunityTicks(), t.dwellMs(), t.maxUtilization(), t.improvementFraction(), t.improvementMs(), 1, t.flapWindowMs());
        Configuration unhealthy = with(outage, "mock", Set.of(), outage.profiles(), oneSwitch);
        SelectionPolicy policy = new SelectionPolicy();
        policy.applied(NOW - 1000);
        MockSignals m = unhealthy.mock();
        Signals staleFew = new Signals(NOW - 60000, 1, m.p99Ms(), m.arrivalRps(), 0, true, m.healthy(), false, null);
        Decision first = record("forced_unhealthy_stale_undersampled_in_cooldown",
                policy.evaluate(unhealthy, staleFew, routing("monolith", Set.of()), NOW));
        assertEquals("switch", first.action()); assertEquals("availability_or_requirement", first.reason()); assertEquals("faas", first.target());
        assertEquals("availability_or_requirement", record("forced_unhealthy_within_tick",
                policy.evaluate(unhealthy, staleFew, routing("monolith", Set.of()), NOW + 1)).reason());

        Configuration stable = SelectionScenarios.create("stable", "monolith", NOW);
        Configuration anchoring = with(stable, "mock", Set.of("consent_integrity_anchor"), stable.profiles(), oneSwitch);
        Signals staleHealthy = new Signals(NOW - 60000, 1, 100d, 10, 0, true, health(Set.of()), false, null);
        Decision requirement = record("forced_requirement_stale_undersampled_in_cooldown",
                policy.evaluate(anchoring, staleHealthy, routing("monolith", Set.of()), NOW + 2));
        assertEquals("switch", requirement.action()); assertEquals("availability_or_requirement", requirement.reason());
        assertEquals("decentralized", requirement.target());

        Configuration errors = SelectionScenarios.create("errors", "monolith", NOW);
        Signals staleErrors = new Signals(NOW - 60000, 1200, 100d, 10, 0, true, health(Set.of()), false, .20);
        Decision errorHold = record("errors_stale_not_forced",
                new SelectionPolicy().evaluate(errors, staleErrors, routing("monolith", Set.of()), NOW));
        assertEquals("hold", errorHold.action()); assertEquals("stale_or_invalid_signal", errorHold.reason());

        Signals pressured = new Signals(NOW, 1200, 100d, 10, 0, true, m.healthy(), true, null);
        assertEquals("gateway_pressure", record("forced_unhealthy_gateway_pressure",
                new SelectionPolicy().evaluate(unhealthy, pressured, routing("monolith", Set.of()), NOW)).reason());
    }

    @Test void builtInScenarioRowsWithoutOtherAsBuiltAssertionsMatchTheirDeclaredResults() throws Exception {
        Configuration stale = SelectionScenarios.create("stale", "monolith", NOW);
        assertEquals(NOW - 60000, stale.mock().sampleTimestampEpochMs());
        assertEquals("stale_or_invalid_signal", record("scenario_stale_as_built", new SelectionPolicy().evaluate(stale,
                signal(stale, stale.mock().sampleTimestampEpochMs()), routing("monolith", Set.of()), NOW)).reason());
        Configuration outage = SelectionScenarios.create("outage", "monolith", NOW);
        Decision d = record("scenario_outage_as_built",
                new SelectionPolicy().evaluate(outage, signal(outage, NOW), routing("monolith", Set.of()), NOW));
        assertEquals("switch", d.action()); assertEquals("availability_or_requirement", d.reason()); assertEquals("faas", d.target());
    }

    @Test void policyIdentityFixesTieOrderAndDeclaredCapabilitiesAndExcludesContextAndScores() throws Exception {
        assertEquals(Map.of(
                "monolith", Set.of("read_your_writes_snapshot", "person_update_persists_embedded_consents", "person_deleted_event"),
                "faas", Set.of("read_your_writes_snapshot", "person_update_persists_embedded_consents", "person_deleted_event"),
                "microservices", Set.of("read_your_writes_snapshot", "person_deleted_event"),
                "microservices-async", Set.of("person_deleted_event"),
                "decentralized", Set.of("consent_integrity_anchor", "read_your_writes_snapshot",
                        "person_update_persists_embedded_consents", "person_deleted_event")), DECLARED_CAPABILITIES);
        JsonNode document = SelectionPolicyVersion.document(SelectionScenarios.create("read-crossover", "monolith", NOW), false);
        List<String> fields = new ArrayList<>(); document.fieldNames().forEachRemaining(fields::add);
        assertEquals(List.of("allowApplyAtStartup", "applyChanges", "architectureOrder", "autoEvaluate", "capabilities",
                "contract", "enabled", "profiles", "source", "tuning", "workloads"), fields);
        assertEquals("pdms-selection-policy/1", document.path("contract").asText());
        assertEquals(ARCHITECTURES, JSON.convertValue(document.path("architectureOrder"), List.class));
        assertEquals(List.of("mix", "read", "write"), JSON.convertValue(document.path("workloads"), List.class));
        for (String name : ARCHITECTURES) {
            assertEquals(DECLARED_CAPABILITIES.get(name).stream().sorted().toList(),
                    JSON.convertValue(document.path("capabilities").path(name), List.class));
        }
        writeString(TRACE.resolveSibling("policy-document.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(document));
        List<String> scoreInputs = Stream.of(Configuration.class, Profile.class, Tuning.class, MockSignals.class, Signals.class,
                        ExperimentControl.Start.class)
                .flatMap(type -> Stream.of(type.getRecordComponents()).map(c -> type.getSimpleName() + "." + c.getName()))
                .filter(name -> name.toLowerCase(ROOT).contains("score")).toList();
        assertEquals(List.of(), scoreInputs);
    }
}
