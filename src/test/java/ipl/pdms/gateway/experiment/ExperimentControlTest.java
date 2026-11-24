package ipl.pdms.gateway.experiment;

import static ipl.pdms.gateway.config.GatewayProperties.forTest;
import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.ReflectionTestUtils.invokeMethod;
import static org.springframework.test.util.ReflectionTestUtils.setField;
import static reactor.core.publisher.Mono.defer;
import static reactor.core.scheduler.Schedulers.isInNonBlockingThread;
import static reactor.core.scheduler.Schedulers.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionJournal;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionRouting;
import ipl.pdms.gateway.selection.SelectionService;
import ipl.pdms.gateway.selection.SelectionSignals;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import reactor.test.StepVerifier;

class ExperimentControlTest {
    @TempDir Path temporary;
    static class Clock extends GatewayClock {
        long ms = 1793300000000L, ns;
        public long epochMillis() { return ms; }
        public long nanoTime() { return ns; }
        void advance(long delta) { ms += delta; ns += delta * 1_000_000; }
    }
    final Clock clock = new Clock();
    final RoutingTable routing = mock(RoutingTable.class);
    final AdminAuth auth = mock(AdminAuth.class);
    final RuntimeCoordinator runtime = mock(RuntimeCoordinator.class);
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final ObjectMapper mapper = new ObjectMapper();
    final TrafficStatistics traffic = new TrafficStatistics(clock, forTest("unused", 1000));
    final Stimulus stimulus = new Stimulus(clock, traffic);
    SelectionService selection;
    SelectionJournal journal;
    ExperimentControl experiment;

    @BeforeEach void setup() throws Exception {
        owner("monolith");
        when(runtime.managed()).thenReturn(true); when(runtime.phase()).thenReturn("ready");
        final RequestLog log = mock(RequestLog.class);
        when(log.file()).thenReturn(temporary.resolve("requests.jsonl")); when(log.instanceId()).thenReturn("test");
        journal = new SelectionJournal(mapper, log);
        final var health = mock(ipl.pdms.gateway.health.BackendHealth.class);
        ARCHITECTURES.forEach(a -> when(health.realisation(a)).thenReturn(ipl.pdms.gateway.health.HealthState.Status.UP));
        selection = new SelectionService(routing, new SelectionSignals(routing, health, traffic),
                mock(SelectionRouting.class), clock, mapper, journal, meters, true, forTest("unused", 1000));
        experiment = create(filePlan(), true);
    }
    private void owner(String name) {
        final var backends = new LinkedHashMap<String, Map<String, String>>();
        ARCHITECTURES.forEach(a -> backends.put(a, Map.of("default", "http://unused")));
        when(routing.current()).thenReturn(new RoutingSnapshot("routing/1", 0, name, "request", backends, Set.of()));
    }
    @AfterEach void close() throws Exception {
        invokeMethod(selection, "stop"); journal.close(); meters.close();
    }
    private ExperimentControl.Start request(String indicator, String sequence) {
        return new ExperimentControl.Start("t", indicator, "mix", sequence);
    }
    private Configuration config() { return (Configuration)selection.status().get("configuration"); }
    private ObjectNode filePlan() throws Exception {
        try (final var input = new ClassPathResource("selection-experiment.json").getInputStream()) {
            return (ObjectNode)mapper.readTree(input);
        }
    }
    private ExperimentControl create(ObjectNode plan, boolean enabled) throws Exception {
        final Path file = temporary.resolve("experiment-" + randomUUID() + ".json");
        mapper.writeValue(file.toFile(), plan);
        return new ExperimentControl(selection, journal, routing, clock, auth, mapper, enabled,
                new FileSystemResource(file), runtime, traffic, stimulus);
    }
    private void advance(long ms) { clock.advance(ms); experiment.tick(); }
    private Map<String, Object> evaluate() {
        return selection.evaluate(randomUUID().toString(), ((Number)selection.status().get("version")).longValue(), 0, "test");
    }

    @Test void experimentStatusReadsStateOffTheRequestEventLoop() {
        when(auth.bearer("test")).thenReturn(true);
        final var snapshot = routing.current();
        when(routing.current()).thenAnswer(call -> {
            assertFalse(isInNonBlockingThread());
            return snapshot;
        });
        StepVerifier.create(defer(() -> experiment.get("test")).subscribeOn(parallel()))
                .assertNext(response -> assertEquals("idle", response.getBody().get("phase")))
                .expectComplete().verify(ofSeconds(5));
    }

    @ParameterizedTest @ValueSource(strings={"reference", "adaptive"})
    void sequenceUsesRealSignalsAndRunsUntilStopped(String sequence) {
        experiment.start(request("T1", sequence));
        assertEquals("real", config().source()); assertEquals(null, config().mock());
        assertFalse((boolean)selection.status().get("applicationEnabled"));
        assertEquals("warmup", experiment.status().get("phase"));
        final String hash = (String)selection.status().get("policySha256");
        advance(29999); assertEquals("warmup", experiment.status().get("phase"));
        assertEquals("", stimulus.target());
        advance(1); assertEquals("running", experiment.status().get("phase"));
        assertEquals(sequence.equals("adaptive"), selection.status().get("applicationEnabled"));
        final Object effect = stimulus.status().get("activatedAtEpochMs");
        advance(604800000); assertEquals("running", experiment.status().get("phase"));
        assertEquals(effect, stimulus.status().get("activatedAtEpochMs"));
        assertEquals(hash, selection.status().get("policySha256"));
        assertEquals(sequence.equals("adaptive"), selection.status().get("applicationEnabled"));
        experiment.stop(); assertEquals("stopped", experiment.status().get("phase"));
        assertFalse((boolean)selection.status().get("applicationEnabled"));
        assertEquals("", stimulus.target()); assertEquals("monolith", routing.current().active());
    }
    @ParameterizedTest @ValueSource(strings={"T1","T2","T3","T4","T5","T6","T7"})
    void everyCaseStartsFromItsDeclaredOwnerWithoutMockValues(String indicator) throws Exception {
        final var plan = filePlan();
        owner(plan.path("cases").path(indicator).path("startingArchitecture").asText(plan.path("defaultArchitecture").asText()));
        experiment.start(request(indicator, "reference")); advance(30000);
        assertEquals("running", experiment.status().get("phase"));
        assertEquals("real", config().source()); assertEquals(null, config().mock());
        assertTrue((boolean)selection.status().get("policyFrozen"));
        final Set<String> required = switch (indicator) {
            case "T4" -> Set.of("consent_integrity_anchor");
            case "T5" -> Set.of("read_your_writes_snapshot");
            default -> Set.of();
        };
        assertEquals(required, config().requiredCapabilities());
        experiment.stop();
        assertEquals(Set.of(), config().requiredCapabilities());
        assertEquals("", stimulus.target());
    }
    @Test void stalePublicationHoldsDespiteNewRealSlowCompletions() {
        experiment.start(request("T6", "adaptive"));
        clock.advance(30000);
        traffic.arrival("mix"); traffic.completion(10, false); clock.advance(1); experiment.tick();
        assertEquals(10d, traffic.publishedSnapshot(30).get("p99_ms"));
        final Object original = traffic.publishedSnapshot(30).get("source_timestamp_epoch_ms");
        advance(31000);
        traffic.arrival("mix"); traffic.completion(350, false); clock.advance(1);
        assertEquals(350d, traffic.snapshot(30).get("p99_ms"));
        assertEquals(original, traffic.publishedSnapshot(30).get("source_timestamp_epoch_ms"));
        assertEquals("stale_or_invalid_signal", evaluate().get("reason"));
        experiment.stop();
        assertEquals(traffic.snapshot(30), traffic.publishedSnapshot(30));
    }
    @ParameterizedTest @ValueSource(strings={"T4","T5"})
    void requirementsAreNormalContextAndCanProduceARealSourceRecommendation(String indicator) {
        owner(indicator.equals("T5") ? "microservices-async" : "monolith");
        experiment.start(request(indicator, "adaptive")); advance(30000);
        final var result = evaluate();
        assertEquals(indicator.equals("T5") ? "monolith" : "decentralized", result.get("target")); assertEquals("real", result.get("source"));
        assertEquals("availability_or_requirement", result.get("reason"));
        assertFalse((boolean)result.get("applyRequested"));
    }
    @Test void automaticRunWaitsForRuntimeAndFirstBusinessArrival() throws Exception {
        final var plan = filePlan(); plan.set("run", mapper.valueToTree(request("T1", "reference")));
        experiment = create(plan, true); when(runtime.phase()).thenReturn("unreconciled");
        traffic.arrival("mix"); advance(1); assertEquals("idle", experiment.status().get("phase"));
        when(runtime.phase()).thenReturn("ready"); experiment.tick();
        assertEquals("warmup", experiment.status().get("phase"));
    }
    @Test void noTrafficDoesNotStartTheConfiguredRun() throws Exception {
        final var plan = filePlan(); plan.set("run", mapper.valueToTree(request("T1", "adaptive")));
        experiment = create(plan, true); experiment.tick(); assertEquals("idle", experiment.status().get("phase"));
    }
    @Test void wrongOwnerAndDisabledInjectionAreRejected() throws Exception {
        assertThrows(SelectionService.ControlException.class, () -> experiment.start(request("T5", "adaptive")));
        experiment = create(filePlan(), false);
        assertThrows(SelectionService.ControlException.class, () -> experiment.start(request("T1", "reference")));
    }
    @ParameterizedTest @ValueSource(strings={"T0","T8","T9","T11"})
    void unknownCasesAreRejected(String indicator) {
        assertThrows(SelectionService.ControlException.class, () -> experiment.start(request(indicator, "reference")));
        assertEquals(false, selection.status().get("policyFrozen"));
    }
    @Test void externalContextAndUncertainHandoverAreNotOverwritten() {
        experiment.start(request("T1", "adaptive"));
        selection.updateContext(randomUUID().toString(), ((Number)selection.status().get("version")).longValue(),
                "write", Set.of(), "external");
        setField(selection, "automaticPause", "routing_reconciliation_required"); experiment.tick();
        assertEquals("failed", experiment.status().get("phase"));
        assertEquals("write", config().workload());
        assertEquals("routing_reconciliation_required", selection.status().get("automaticPauseReason"));
    }
    @Test void stabilityPulsesLeadToContinuousDelay() {
        experiment.start(request("T7", "adaptive")); advance(30000);
        assertTrue(hasFault()); advance(500); assertFalse(hasFault());
        advance(3500); assertTrue(hasFault()); advance(500); assertFalse(hasFault());
        advance(15500); assertTrue(hasFault());
        advance(604800000); assertEquals("running", experiment.status().get("phase")); assertTrue(hasFault());
        assertEquals(1, config().tuning().windowSeconds()); assertEquals(4, config().tuning().breachTicks());
    }
    private boolean hasFault() {
        return !stimulus.status().get("indicator").equals("");
    }
    @Test void closingTheApplicationGateRemainsSafeWhenTheJournalFails() throws Exception {
        journal.close();
        assertThrows(SelectionService.ControlException.class, () -> selection.setApplicationEnabled(false));
        assertFalse((boolean)selection.status().get("applicationEnabled"));
    }
    @Test void statusShowsDefaultArchitectureAndCompleteDelayBeforeStimulus() {
        final var status = experiment.status();
        assertEquals("monolith", status.get("defaultArchitecture"));
        assertEquals("monolith", status.get("activeArchitecture"));
        assertEquals(30L, status.get("warmupSeconds"));
        assertFalse(status.containsKey("phaseSeconds"));
        assertEquals(30L, status.get("stimulusStartsAfterSeconds"));
        experiment.start(request("T1", "reference"));
        assertEquals(clock.epochMillis() + 30000, experiment.status().get("plannedStimulusAtEpochMs"));
        assertFalse(experiment.status().containsKey("plannedComparisonAtEpochMs"));
    }
    @ParameterizedTest @ValueSource(strings={"T1","T5"})
    void casesInheritTheDefaultArchitectureOrUseTheirExplicitOverride(String indicator) throws Exception {
        final var plan = filePlan(); plan.put("defaultArchitecture", "faas");
        final String initial = indicator.equals("T5") ? "microservices-async" : "faas";
        owner(initial); experiment = create(plan, true);
        experiment.start(request(indicator, "reference"));
        assertEquals("faas", experiment.status().get("defaultArchitecture"));
        assertEquals(initial, experiment.status().get("startingArchitecture"));
        advance(30000); assertEquals(initial, stimulus.target());
    }
    @Test void invalidStimulusIsRejectedAtStartup() throws Exception {
        final var plan = filePlan();
        ((ObjectNode)plan.path("cases").path("T1")).put("delayMs", -1);
        assertThrows(IllegalArgumentException.class, () -> create(plan, true));
    }
}
