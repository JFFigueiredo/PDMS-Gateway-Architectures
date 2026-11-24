package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.DECLARED_CAPABILITIES;
import static ipl.pdms.gateway.selection.SelectionModel.Tuning.defaults;
import static ipl.pdms.gateway.selection.SelectionScenarios.NAMES;
import static ipl.pdms.gateway.selection.SelectionService.validate;
import static java.lang.Double.NaN;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.selection.SelectionModel.Candidate;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import ipl.pdms.gateway.selection.SelectionModel.Signals;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;
import ipl.pdms.gateway.selection.SelectionPolicy;
import ipl.pdms.gateway.selection.SelectionScenarios;
import ipl.pdms.gateway.selection.SelectionService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SelectionPolicyTest {
    private static final long NOW = 1_000_000;
    private final SelectionPolicy policy = new SelectionPolicy();
    private RoutingSnapshot routing(String active, Set<String> disabled) {
        Map<String, Map<String, String>> backends = new HashMap<>();
        ARCHITECTURES.forEach(a -> backends.put(a, Map.of("default", "http://localhost:8080")));
        return new RoutingSnapshot("routing/1", 0, active, "request", backends, disabled);
    }
    private Configuration scenario(String name) {
        return SelectionScenarios.create(name, "monolith", NOW);
    }
    private Signals signal(Configuration c, long now) {
        var m = c.mock();
        return new Signals(now, m.samples(), m.p99Ms(), m.arrivalRps(), m.pending(), m.valid(), m.healthy(), m.gatewayPressure(), m.unexpectedErrorRatio());
    }
    private Decision evaluate(Configuration c, long now) {
        return policy.evaluate(c, signal(c, now), routing("monolith", Set.of()), now);
    }
    private Configuration copy(Configuration c, String source, Set<String> required, Map<String, Map<String, Profile>> profiles, Tuning tuning) {
        return new Configuration(c.enabled(), c.autoEvaluate(), c.applyChanges(), source, c.workload(), required, profiles, tuning, c.mock());
    }
    @Test void everyBuiltInScenarioHasAValidExplicitSyntheticConfiguration() {
        for (String name : NAMES) {
            Configuration c = scenario(name); validate(c);
            assertFalse(c.applyChanges()); assertFalse(c.autoEvaluate());
            assertTrue(c.profiles().values().stream().allMatch(m -> m.values().stream().allMatch(p -> p.evidenceKind().equals("synthetic"))));
        }
    }
    @Test void stableAndEqualCandidatesKeepTheActiveTarget() {
        assertEquals("keep_active", evaluate(scenario("stable"), NOW).reason());
        assertEquals("monolith", new SelectionPolicy().evaluate(scenario("tie"), signal(scenario("tie"), NOW), routing("monolith", Set.of()), NOW).target());
    }
    @Test void sustainedReadBreachSelectsFaasAfterTwoTicks() {
        Configuration c = scenario("read-crossover");
        assertEquals("debounce", evaluate(c, NOW).reason());
        Decision next = evaluate(c, NOW + 5000);
        assertEquals("switch", next.action()); assertEquals("faas", next.target()); assertEquals("latency_breach", next.reason());
    }
    @Test void repeatedCallsWithinATickCannotManufactureDebounce() {
        Configuration c = scenario("read-crossover"); evaluate(c, NOW);
        Decision repeated = evaluate(c, NOW + 1);
        assertEquals("tick_not_due", repeated.reason()); assertEquals(1, repeated.breachCount());
    }
    @Test void writeAndSplitFixturesExerciseDifferentEligibilityAndWinners() {
        Configuration write = scenario("write-crossover"); evaluate(write, NOW);
        assertEquals("microservices-async", evaluate(write, NOW + 5000).target());
        Configuration split = scenario("split-crossover"); SelectionPolicy second = new SelectionPolicy();
        second.evaluate(split, signal(split, NOW), routing("monolith", Set.of()), NOW);
        assertEquals("microservices", second.evaluate(split, signal(split, NOW + 5000), routing("monolith", Set.of()), NOW + 5000).target());
    }
    @Test void anchoringRequirementSelectsOnlyDecentralized() {
        Decision d = evaluate(scenario("anchoring-required"), NOW);
        assertEquals("switch", d.action()); assertEquals("decentralized", d.target());
        assertTrue(d.candidates().stream().filter(Candidate::eligible).allMatch(c -> c.architecture().equals("decentralized")));
    }
    @Test void noEligibleAndGatewayPressureProduceExplicitHolds() {
        assertEquals("no_eligible_candidate", evaluate(scenario("no-eligible"), NOW).reason());
        assertEquals("gateway_pressure", evaluate(scenario("gateway-pressure"), NOW + 5000).reason());
    }
    @Test void staleFutureAndLowSampleSignalsHold() {
        Configuration c = scenario("read-crossover"); Signals s = signal(c, NOW);
        for (long timestamp : new long[]{NOW - 15001, NOW + 1}) {
            assertEquals("stale_or_invalid_signal", policy.evaluate(c,
                    new Signals(timestamp, 1200, 350d, 10, 0, true, s.healthy(), false), routing("monolith", Set.of()), NOW).reason());
        }
        assertEquals("insufficient_samples", policy.evaluate(c,
                new Signals(NOW, 10, 350d, 10, 0, true, s.healthy(), false), routing("monolith", Set.of()), NOW).reason());
    }
    @Test void correctedSyncPublisherMeetsDeleteRequirementWhileAsyncStillLacksImmediateSnapshots() {
        Configuration c = scenario("read-crossover");
        c = copy(c, "mock", Set.of("person_deleted_event", "read_your_writes_snapshot"), c.profiles(), c.tuning());
        Decision d = evaluate(c, NOW);
        assertTrue(d.candidates().stream().filter(x -> x.architecture().equals("microservices")).allMatch(Candidate::eligible));
        assertTrue(d.candidates().stream().filter(x -> x.architecture().equals("microservices-async"))
                .allMatch(x -> x.reason().equals("missing_capability")));
        assertFalse(DECLARED_CAPABILITIES.get("microservices").contains("person_update_persists_embedded_consents"));
    }
    @Test void disabledFastCandidateIsNeverSelected() {
        Configuration c = scenario("read-crossover");
        Decision d = policy.evaluate(c, signal(c, NOW), routing("monolith", Set.of("faas")), NOW);
        assertEquals("disabled", d.candidates().stream().filter(x -> x.architecture().equals("faas")).findFirst().orElseThrow().reason());
    }
    @Test void realModeRejectsSyntheticProfiles() {
        Configuration c = scenario("read-crossover"); c = copy(c, "real", Set.of(), c.profiles(), c.tuning());
        assertEquals("no_eligible_candidate", evaluate(c, NOW).reason());
    }
    @Test void missingActiveProfileHoldsPerformanceDecisions() {
        Configuration c = scenario("read-crossover"); var profiles = new HashMap<>(c.profiles()); profiles.remove("monolith");
        assertEquals("active_profile_unknown", evaluate(copy(c, "mock", Set.of(), profiles, c.tuning()), NOW).reason());
    }
    @Test void unknownCapabilitiesAndInvalidNumbersAreRejected() {
        Configuration c = scenario("stable");
        assertThrows(SelectionService.ControlException.class, () -> validate(copy(c, "mock", Set.of("invented"), c.profiles(), c.tuning())));
        var profiles = Map.of("monolith", Map.of("mix", new Profile("synthetic", "fixture", 0, 100, 100, NaN)));
        assertThrows(SelectionService.ControlException.class, () -> validate(copy(c, "mock", Set.of(), profiles, c.tuning())));
    }
    @Test void cooldownAndFlapBudgetApplyToPerformanceSwitches() {
        Configuration c = scenario("read-crossover"); policy.applied(NOW);
        evaluate(c, NOW + 5000); assertEquals("cooldown", evaluate(c, NOW + 10000).reason());
        var t = c.tuning();
        Tuning noDwell = new Tuning(t.tickMs(), t.windowSeconds(), t.minSamples(), t.maxAgeMs(), t.highP99Ms(), t.recoveryP99Ms(),
                t.breachTicks(), t.opportunityTicks(), 0, t.maxUtilization(), t.improvementFraction(), t.improvementMs(), 1, t.flapWindowMs());
        c = copy(c, "mock", Set.of(), c.profiles(), noDwell);
        assertEquals("flap_budget", evaluate(c, NOW + 15000).reason());
    }
    @Test void oscillationRecoveryAndSwitchGuardsProduceAnExplicitTrace() throws Exception {
        Configuration base = scenario("read-crossover"); Tuning t = base.tuning();
        Tuning tuning = new Tuning(1000, t.windowSeconds(), t.minSamples(), t.maxAgeMs(),
                t.highP99Ms(), t.recoveryP99Ms(), 2, 100, 10000, t.maxUtilization(),
                t.improvementFraction(), t.improvementMs(), 1, 60000);
        Configuration c = copy(base, "mock", Set.of(), base.profiles(), tuning);
        java.nio.file.Path trace = createTempDirectory("pdms-t7-policy");
        writeString(trace.resolve("configuration.json"), new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(c));
        for (int i = 0; i < 4; i++) {
            Decision d = traceLatency(trace, "oscillation", c, "monolith", NOW + i * 1000, i % 2 == 0 ? 350 : 100);
            assertEquals("hold", d.action()); assertEquals("debounce", d.reason());
            assertEquals(i % 2 == 0 ? 1 : 0, d.breachCount());
        }
        assertEquals("debounce", traceLatency(trace, "sustained", c, "monolith", NOW + 4000, 350).reason());
        Decision first = traceLatency(trace, "sustained", c, "monolith", NOW + 5000, 350);
        assertEquals("latency_breach", first.reason()); assertEquals("switch", first.action()); assertEquals("faas", first.target());
        policy.applied(NOW + 5000);
        var reversed = new HashMap<>(c.profiles());
        reversed.put("monolith", Map.of("read", new Profile("synthetic", "T7 reverse fixture", 0, 100, 100, 40)));
        c = copy(c, "mock", Set.of(), reversed, tuning);
        writeString(trace.resolve("reverse-configuration.json"), new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(c));
        assertEquals("debounce", traceLatency(trace, "reverse", c, "faas", NOW + 6000, 350).reason());
        Decision cooldown = traceLatency(trace, "cooldown", c, "faas", NOW + 7000, 350);
        assertEquals("hold", cooldown.action()); assertEquals("cooldown", cooldown.reason());
        Decision budget = traceLatency(trace, "switch_budget", c, "faas", NOW + 15000, 350);
        assertEquals("hold", budget.action()); assertEquals("flap_budget", budget.reason());
        Decision expired = traceLatency(trace, "budget_expired", c, "faas", NOW + 65000, 350);
        assertEquals("switch", expired.action()); assertEquals("latency_breach", expired.reason()); assertEquals("monolith", expired.target());
    }
    private Decision traceLatency(java.nio.file.Path trace, String stage, Configuration c, String active, long now, double p99) throws Exception {
        Signals s = signal(c, now);
        Signals input = new Signals(now, s.samples(), p99, s.arrivalRps(), s.pending(), s.valid(), s.healthy(), s.gatewayPressure(), s.unexpectedErrorRatio());
        Decision d = policy.evaluate(c, input, routing(active, Set.of()), now);
        var row = Map.of("stage", stage, "atEpochMs", now, "syntheticP99Ms", p99, "decision", d);
        writeString(trace.resolve("decisions.jsonl"), new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(row) + "\n",
                CREATE, APPEND);
        return d;
    }
    @Test void absentHeadroomCanTriggerAnEligibleHigherCapacityAlternative() {
        Configuration c = scenario("stable"); var profiles = new HashMap<>(c.profiles());
        profiles.put("monolith", Map.of("mix", new Profile("synthetic", "fixture", 0, 10, 10, 100)));
        Decision d = evaluate(copy(c, "mock", Set.of(), profiles, c.tuning()), NOW);
        assertEquals("headroom", d.reason()); assertEquals("faas", d.target());
    }
    private Configuration capacityFixture(double rate, double activeMax, double targetMax) {
        Map<String, Boolean> healthy = new HashMap<>();
        ARCHITECTURES.forEach(name -> healthy.put(name, Set.of("faas", "monolith").contains(name)));
        var profiles = Map.of(
                "faas", Map.of("mix", new Profile("synthetic", "capacity-fixture", 0, activeMax, 100, 10)),
                "monolith", Map.of("mix", new Profile("synthetic", "capacity-fixture", 0, targetMax, 200, 500)));
        var mock = new MockSignals(NOW, 1200, 10d, rate, 0, true, healthy, false);
        var config = new Configuration(true, false, false, "mock", "mix", Set.of(), profiles, defaults(), mock);
        validate(config);
        return config;
    }
    @Test void capacityOverloadCanLeaveAnUnsupportedActiveLatencyProfileWithoutExtrapolatingIt() {
        Configuration c = capacityFixture(120, 100, 200);
        Decision d = policy.evaluate(c, signal(c, NOW), routing("faas", Set.of()), NOW);
        assertEquals("switch", d.action()); assertEquals("headroom", d.reason()); assertEquals("monolith", d.target());
        assertEquals("outside_profile_support", d.candidates().stream()
                .filter(candidate -> candidate.architecture().equals("faas")).findFirst().orElseThrow().reason());
        assertEquals(0, d.breachCount()); assertEquals(0, d.opportunityCount());
    }
    @Test void unsupportedActiveLatencyWithoutCapacityPressureStillHolds() {
        Configuration c = capacityFixture(80, 70, 200);
        Decision d = policy.evaluate(c, signal(c, NOW), routing("faas", Set.of()), NOW);
        assertEquals("hold", d.action()); assertEquals("active_profile_unknown", d.reason());
    }
    @Test void capacityPressureDoesNotAdmitUnsupportedTargetsOrSyntheticRealCapacity() {
        Configuration unsupported = capacityFixture(120, 100, 110);
        assertEquals("no_eligible_candidate", policy.evaluate(unsupported, signal(unsupported, NOW), routing("faas", Set.of()), NOW).reason());
        Configuration c = capacityFixture(120, 100, 200);
        var profiles = new HashMap<>(c.profiles());
        profiles.put("monolith", Map.of("mix", new Profile("measured", "fixture-only", 0, 200, 200, 500)));
        Configuration real = copy(c, "real", Set.of(), profiles, c.tuning());
        assertEquals("active_profile_unknown", new SelectionPolicy().evaluate(real, signal(real, NOW), routing("faas", Set.of()), NOW).reason());
    }
    @Test void capacitySwitchStillRequiresFreshSufficientWorkloadEvidence() {
        Configuration c = capacityFixture(120, 100, 200); Signals s = signal(c, NOW);
        Decision stale = policy.evaluate(c, new Signals(NOW - 15001, s.samples(), s.p99Ms(), s.arrivalRps(), 0, true, s.healthy(), false), routing("faas", Set.of()), NOW);
        assertEquals("stale_or_invalid_signal", stale.reason());
        Decision undersampled = policy.evaluate(c, new Signals(NOW, 1, s.p99Ms(), s.arrivalRps(), 0, true, s.healthy(), false), routing("faas", Set.of()), NOW);
        assertEquals("insufficient_samples", undersampled.reason());
    }
    @Test void capacitySwitchKeepsDwellAndFlapGuards() {
        Configuration c = capacityFixture(120, 100, 200); policy.applied(NOW - 1000);
        assertEquals("cooldown", policy.evaluate(c, signal(c, NOW), routing("faas", Set.of()), NOW).reason());
        var t = c.tuning();
        var noDwell = new Tuning(t.tickMs(), t.windowSeconds(), t.minSamples(), t.maxAgeMs(), t.highP99Ms(), t.recoveryP99Ms(),
                t.breachTicks(), t.opportunityTicks(), 0, t.maxUtilization(), t.improvementFraction(), t.improvementMs(), 1, t.flapWindowMs());
        Configuration guarded = copy(c, "mock", Set.of(), c.profiles(), noDwell);
        assertEquals("flap_budget", policy.evaluate(guarded, signal(guarded, NOW + 5000), routing("faas", Set.of()), NOW + 5000).reason());
    }
    @Test void profileSupportIsNotExtrapolated() {
        Configuration c = scenario("stable"); Signals s = signal(c, NOW);
        Decision d = policy.evaluate(c, new Signals(NOW, 1200, 350d, 101, 0, true, s.healthy(), false), routing("monolith", Set.of()), NOW);
        assertEquals("no_eligible_candidate", d.reason());
    }
    @Test void unhealthyActiveCanChooseAnEligibleAlternativeWithoutLatencyDebounce() {
        assertEquals("availability_or_requirement", evaluate(scenario("outage"), NOW).reason());
    }
    @Test void sharedSlowdownDoesNotInventABetterCandidate() {
        Configuration c = scenario("stable"); Signals s = signal(c, NOW);
        Decision d = policy.evaluate(c, new Signals(NOW, 1200, 900d, 10, 0, true, s.healthy(), false), routing("monolith", Set.of()), NOW);
        assertEquals("keep_active", d.reason());
    }
    @Test void unexpectedErrorFixtureCanSelectAlternativeWithoutInventingHighLatency() {
        Decision d = evaluate(scenario("errors"), NOW);
        assertEquals("switch", d.action()); assertEquals("faas", d.target());
        assertEquals("active_errors", d.candidates().stream().filter(c -> c.architecture().equals("monolith")).findFirst().orElseThrow().reason());
    }
}
