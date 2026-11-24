package ipl.pdms.gateway.experiment;

import static ipl.pdms.gateway.config.GatewayProperties.forTest;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static reactor.core.publisher.Mono.empty;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.core.publisher.Mono.never;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class StimulusTest {
    final Stimulus stimulus = new Stimulus(new GatewayClock(), mock(TrafficStatistics.class));
    JsonNode cases;
    String defaultArchitecture;

    @BeforeEach void loadCases() throws IOException {
        try (final var input = new ClassPathResource("selection-experiment.json").getInputStream()) {
            final var plan = new ObjectMapper().readTree(input);
            cases = plan.path("cases"); defaultArchitecture = plan.path("defaultArchitecture").asText();
        }
    }
    private ObjectNode settings(String indicator) {
        final ObjectNode settings = cases.required(indicator).deepCopy();
        if (!settings.has("startingArchitecture")) {
            settings.put("startingArchitecture", defaultArchitecture);
        }
        return settings;
    }
    private boolean applyCase(String indicator, ObjectNode settings) {
        return stimulus.applyCase(indicator, settings, 0, 30);
    }
    private Mono<Void> forward(String architecture, Map<String, Object> attributes, Runnable next) {
        return stimulus.forward(architecture, false, "request", attributes, () -> fromRunnable(next));
    }
    private boolean unavailable(Throwable error) {
        return error instanceof ResponseStatusException response && response.getStatusCode().value() == 503;
    }

    @ParameterizedTest @CsvSource({"T1,75", "T1,350", "T6,350"})
    void latencyCasesDelayOnlyBusinessTrafficToTheFixedTarget(String indicator, long delayMs) {
        final var forwarded = new AtomicInteger();
        final var config = settings(indicator).put("delayMs", delayMs);
        applyCase(indicator, config);
        forward("faas", new HashMap<>(), forwarded::incrementAndGet).block();
        stimulus.forward("monolith", true, "probe", new HashMap<>(), () -> fromRunnable(forwarded::incrementAndGet)).block();
        assertDelayed(indicator, config, 0);
        assertEquals(2, forwarded.get()); assertEquals(1L, stimulus.status().get("affectedRequests"));
        assertEquals("monolith", stimulus.target()); assertFalse(applyCase(indicator, config));
    }
    private void assertDelayed(String indicator, ObjectNode config, long elapsedMs) {
        final long delayMs = config.path("delayMs").asLong();
        final var forwarded = new AtomicInteger();
        final Map<String, Object> attributes = new HashMap<>();
        StepVerifier.withVirtualTime(() -> {
            stimulus.applyCase(indicator, config, elapsedMs, 30);
            return forward("monolith", attributes, forwarded::incrementAndGet);
        }).expectSubscription().expectNoEvent(ofMillis(delayMs - 1))
                .then(() -> assertEquals(0, forwarded.get())).thenAwait(ofMillis(1)).expectComplete().verify(ofSeconds(5));
        assertEquals(1, forwarded.get());
        final var evidence = (Map<?, ?>)attributes.get(Stimulus.EVIDENCE);
        assertEquals(indicator, evidence.get("indicator")); assertEquals("monolith", evidence.get("target"));
        assertEquals(delayMs, evidence.get("configured_delay_ms"));
    }

    @Test void t2CapacityRefusesExcessRequestsAndReleasesCancelledSlots() {
        applyCase("T2", settings("T2").put("queueCapacity", 0));
        final var first = stimulus.forward("monolith", false, "first", new HashMap<>(), () -> never()).subscribe();
        try {
            assertEquals(1, stimulus.status().get("inFlight"));
            forward("faas", new HashMap<>(), () -> {}).block(ofSeconds(1));
            StepVerifier.create(forward("monolith", new HashMap<>(), () -> {})).expectErrorMatches(this::unavailable).verify(ofSeconds(5));
        } finally {
            first.dispose();
        }
        assertEquals(0, stimulus.status().get("inFlight")); assertEquals(0, stimulus.status().get("queued"));
    }
    @Test void t2QueuedRequestWaitsUntilTheFirstRequestCompletes() {
        final List<String> completed = new CopyOnWriteArrayList<>();
        StepVerifier.withVirtualTime(() -> {
            applyCase("T2", settings("T2").put("delayMs", 1000).put("queueCapacity", 1));
            return Mono.when(forward("monolith", new HashMap<>(), () -> completed.add("first")),
                    forward("monolith", new HashMap<>(), () -> completed.add("second")));
        }).expectSubscription().then(() -> assertEquals(1, stimulus.status().get("queued")))
                .thenAwait(ofMillis(1000)).then(() -> assertEquals(List.of("first"), completed))
                .thenAwait(ofMillis(1000)).expectComplete().verify(ofSeconds(5));
        assertEquals(List.of("first", "second"), completed);
        assertEquals(0, stimulus.status().get("inFlight")); assertEquals(0, stimulus.status().get("queued"));
    }
    @Test void t2QueueTimeoutReturns503AndDoesNotLeakPermits() {
        StepVerifier.withVirtualTime(() -> {
            applyCase("T2", settings("T2").put("delayMs", 1000).put("queueCapacity", 1).put("maxWaitMs", 50));
            final var first = stimulus.forward("monolith", false, "first", new HashMap<>(), () -> never()).subscribe();
            return forward("monolith", new HashMap<>(), () -> {}).doFinally(signal -> first.dispose());
        }).expectSubscription().expectNoEvent(ofMillis(49)).thenAwait(ofMillis(1))
                .expectErrorMatches(this::unavailable).verify(ofSeconds(5));
        assertEquals(0, stimulus.status().get("inFlight")); assertEquals(0, stimulus.status().get("queued"));
    }

    @Test void t3FailsTargetRequestsAndProbesAndClearRestoresBoth() {
        applyCase("T3", settings("T3"));
        final var called = new AtomicInteger();
        final var failed = stimulus.probe("http://mono", () -> {
            called.incrementAndGet(); return CompletableFuture.completedFuture("UP");
        }, Set.of("http://mono"));
        assertThrows(java.util.concurrent.CompletionException.class, failed::join);
        assertEquals(0, called.get());
        assertEquals("UP", stimulus.probe("http://faas", () -> CompletableFuture.completedFuture("UP"), Set.of("http://mono")).join());
        StepVerifier.create(forward("monolith", new HashMap<>(), called::incrementAndGet))
                .expectError(ConnectException.class).verify(ofSeconds(5));
        assertEquals(0, called.get()); assertEquals(1L, stimulus.status().get("failedProbes"));
        forward("faas", new HashMap<>(), called::incrementAndGet).block();
        stimulus.clear();
        forward("monolith", new HashMap<>(), called::incrementAndGet).block();
        assertEquals(2, called.get());
        assertEquals("UP", stimulus.probe("http://mono", () -> CompletableFuture.completedFuture("UP"), Set.of("http://mono")).join());
    }

    @ParameterizedTest @CsvSource({"T4,consent_integrity_anchor", "T5,read_your_writes_snapshot"})
    void requirementCasesExposeTheirCapabilityWithoutAlteringRequests(String indicator, String capability) {
        final var config = settings(indicator);
        applyCase(indicator, config);
        assertEquals(Set.of(capability), stimulus.requiredCapabilities());
        final var called = new AtomicInteger(); final Map<String, Object> attributes = new HashMap<>();
        forward(config.path("startingArchitecture").asText(), attributes, called::incrementAndGet).block(ofSeconds(1));
        assertEquals(1, called.get()); assertFalse(attributes.containsKey(Stimulus.EVIDENCE));
        assertEquals(0L, stimulus.status().get("affectedRequests"));
        stimulus.clear(); assertEquals(Set.of(), stimulus.requiredCapabilities());
    }
    @Test void t6FreezesRealPublicationWhileRawMeasurementsContinueAndClearResumesIt() {
        final var clock = mock(GatewayClock.class);
        when(clock.epochMillis()).thenReturn(1000000L);
        final var traffic = new TrafficStatistics(clock, forTest("unused", 1000));
        final var observed = new Stimulus(clock, traffic);
        traffic.arrival("mix"); traffic.completion(10, false);
        when(clock.epochMillis()).thenReturn(1000001L);
        observed.applyCase("T6", settings("T6"), 0, 30);
        final var frozen = traffic.publishedSnapshot(30);
        assertEquals(10d, frozen.get("p99_ms")); assertEquals(true, frozen.get("valid"));
        when(clock.epochMillis()).thenReturn(1031001L);
        traffic.arrival("mix"); traffic.completion(350, false);
        when(clock.epochMillis()).thenReturn(1031002L);
        observed.applyCase("T6", settings("T6"), 31001, 30);
        assertEquals(frozen, traffic.publishedSnapshot(30)); assertEquals(350d, traffic.snapshot(30).get("p99_ms"));
        assertTrue(clock.epochMillis() - ((Number)frozen.get("source_timestamp_epoch_ms")).longValue() > 15000);
        observed.clear(); assertEquals(traffic.snapshot(30), traffic.publishedSnapshot(30));
    }

    @ParameterizedTest @CsvSource({"0,true", "500,false", "4000,true", "4500,false", "20000,true",
            "19999,false", "260000,true", "604800000,true"})
    void t7AppliesDelayOnlyDuringPulsesAndSustainedWindows(long elapsedMs, boolean delayed) {
        if (delayed) {
            assertDelayed("T7", settings("T7"), elapsedMs);
        } else {
            final var called = new AtomicInteger(); final Map<String, Object> attributes = new HashMap<>();
            stimulus.applyCase("T7", settings("T7"), elapsedMs, 30);
            StepVerifier.create(forward("monolith", attributes, called::incrementAndGet))
                    .expectSubscription().then(() -> assertEquals(1, called.get())).expectComplete().verify(ofSeconds(5));
            assertFalse(attributes.containsKey(Stimulus.EVIDENCE));
        }
    }
    @ParameterizedTest @ValueSource(strings={"T1","T2","T3","T4","T5","T6","T7"})
    void warmupAndCleanupLeaveEveryCaseInactive(String indicator) {
        final var config = settings(indicator);
        stimulus.applyCase(indicator, config, -1, 30);
        assertEquals("", stimulus.target()); assertEquals(Set.of(), stimulus.requiredCapabilities());
        applyCase(indicator, config); stimulus.clear();
        assertEquals("", stimulus.target()); assertEquals(Set.of(), stimulus.requiredCapabilities());
        final var called = new AtomicInteger();
        forward(config.path("startingArchitecture").asText(), new HashMap<>(), called::incrementAndGet).block();
        assertEquals(1, called.get()); assertEquals(0L, stimulus.status().get("affectedRequests"));
    }
}
