package ipl.pdms.gateway;

import static ipl.pdms.gateway.config.GatewayProperties.forTest;
import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static ipl.pdms.gateway.runtime.RuntimeCoordinator.ARCHITECTURES;
import static ipl.pdms.gateway.runtime.RuntimeCoordinator.ORDINARY;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.ReflectionTestUtils.getField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeControlClient;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionJournal;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RuntimeCoordinatorTest {
    @TempDir Path directory;
    final ObjectMapper mapper = new ObjectMapper();
    final List<String> events = new CopyOnWriteArrayList<>();
    final Map<String, ObjectNode> processes = new LinkedHashMap<>();
    RuntimeCoordinator runtime;
    RoutingTable routing;
    SimpleMeterRegistry meters;
    String unavailable;
    BackendHealth health;
    java.util.function.Consumer<RuntimeControlClient.Process> beforeDisable = process -> {};
    java.util.function.Consumer<RuntimeControlClient.Process> beforeStatus = process -> {};

    @BeforeEach void start() throws Exception {
        var backends = new LinkedHashMap<String, Map<String, String>>();
        int port = 19000;
        for (String architecture : List.of("monolith", "microservices", "microservices-async", "faas", "decentralized")) {
            var targets = new LinkedHashMap<String, String>();
            for (String service : architecture.startsWith("microservices") ? List.of("person", "consent", "snapshot") : List.of("default")) {
                String url = "http://localhost:" + port++;
                targets.put(service, url);
                ObjectNode state = mapper.createObjectNode().put("architecture", architecture).put("instanceId", randomUUID().toString())
                        .put("enabled", false).put("drained", true).put("blockchainAvailable", architecture.equals("decentralized"));
                ObjectNode snapshot = state.putObject("snapshot").put("version", 0);
                snapshot.putObject("policy").put("enabled", false).set("allowedEventTypes", mapper.valueToTree(ORDINARY));
                processes.put(url, state);
            }
            backends.put(architecture, targets);
        }
        Path file = directory.resolve("routing.json");
        mapper.writeValue(file.toFile(), Map.of("version", "routing/1", "epoch", 0, "active", "monolith", "mode", "request", "backends", backends));
        routing = new RoutingTable(forTest(file.toString(), 1000), mapper);
        RuntimeControlClient client = mock(RuntimeControlClient.class);
        when(client.status(any())).thenAnswer(call -> {
            RuntimeControlClient.Process process = call.getArgument(0);
            beforeStatus.accept(process);
            if (process.url().equals(unavailable)) {
                throw new IllegalStateException("Unreachable isolated process");
            }
            return processes.get(process.url()).deepCopy();
        });
        when(client.set(any(), anyBoolean(), anySet(), anyString())).thenAnswer(call -> {
            RuntimeControlClient.Process process = call.getArgument(0);
            boolean enabled = call.getArgument(1);
            if (process.url().equals(unavailable)) {
                throw new IllegalStateException("Unreachable isolated process");
            }
            if (!enabled) {
                beforeDisable.accept(process);
            }
            ObjectNode state = processes.get(process.url());
            state.put("enabled", enabled);
            ObjectNode snapshot = (ObjectNode) state.get("snapshot");
            snapshot.put("version", snapshot.path("version").longValue() + 1);
            ((ObjectNode) snapshot.get("policy")).put("enabled", enabled)
                    .set("allowedEventTypes", mapper.valueToTree(call.<Set<String>>getArgument(2)));
            events.add((enabled ? "enable:" : "disable:") + process.architecture() + ":" + process.url());
            return state.deepCopy();
        });
        health = mock(BackendHealth.class);
        when(health.realisation(anyString())).thenReturn(UP);
        RequestLog log = mock(RequestLog.class);
        when(log.file()).thenReturn(directory.resolve("requests.jsonl"));
        when(log.instanceId()).thenReturn(randomUUID().toString());
        meters = new SimpleMeterRegistry();
        runtime = new RuntimeCoordinator(routing, client, health, mapper, log, meters, true, ofSeconds(2));
    }

    @AfterEach void close() throws Exception {
        if (runtime != null) {
            runtime.close();
        }
        if (meters != null) {
            meters.close();
        }
    }

    org.springframework.http.ResponseEntity<Map<String, Object>> select(String name) {
        return runtime.switchTo(name, routing.current().epoch(), randomUUID().toString());
    }

    @Test void startupClosesBusinessAdmissionWithoutEnablingWorkers() {
        assertEquals("unreconciled", runtime.phase());
        for (String name : ARCHITECTURES) {
            assertFalse(runtime.enter(name));
        }
        assertTrue(events.isEmpty());
    }

    @Test void startupAutomaticallyEstablishesTheConfiguredOwnerOnceAllBackendsAreHealthy() {
        when(health.realisation("faas")).thenReturn(ipl.pdms.gateway.health.HealthState.Status.DOWN);
        runtime.initialize();
        assertEquals("unreconciled", runtime.phase());
        assertTrue(events.isEmpty());
        when(health.realisation("faas")).thenReturn(UP);
        runtime.initialize();
        assertEquals("ready", runtime.phase());
        assertTrue(runtime.allows("monolith"));
        int mutations = events.size();
        runtime.initialize();
        assertEquals(mutations, events.size());
    }

    @Test void staleAutomaticDecisionsCannotMutateBackends() {
        long epoch = routing.current().epoch();
        assertEquals(200, select("faas").getStatusCode().value());
        int mutations = events.size();
        assertEquals(409, runtime.switchTo("monolith", epoch, "stale-decision").getStatusCode().value());
        assertEquals(mutations, events.size());
    }

    @Test void everyProcessMustAcknowledgeDisableBeforeAnyTargetEnables() {
        assertEquals(200, select("microservices").getStatusCode().value());
        assertEquals(9, events.subList(0, 9).stream().filter(e -> e.startsWith("disable:")).count());
        assertEquals(3, events.stream().filter(e -> e.startsWith("enable:microservices:")).count());
        assertTrue(runtime.allows("microservices"));
        assertFalse(runtime.allows("monolith"));
        assertEquals(3, processes.values().stream().filter(p -> p.path("enabled").asBoolean()).count());
    }

    @Test void outstandingConsentSnapshotCanReadPersonBeforeItsDependencyStops() {
        assertEquals(200, select("microservices-async").getStatusCode().value());
        events.clear();
        var targets = routing.current().backends().get("microservices-async");
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        beforeDisable = process -> {
            if (process.url().equals(targets.get("consent"))) {
                // Complete the admitted snapshot callback before draining its person dependency.
                assertTrue(processes.get(targets.get("person")).path("enabled").asBoolean());
                assertTrue(processes.get(targets.get("snapshot")).path("enabled").asBoolean());
                writes.incrementAndGet();
            }
        };
        assertEquals(200, select("faas").getStatusCode().value());
        assertEquals(1, writes.get());
        assertTrue(events.indexOf("disable:microservices-async:" + targets.get("consent"))
                < events.indexOf("disable:microservices-async:" + targets.get("person")));
        assertTrue(events.indexOf("disable:microservices-async:" + targets.get("person"))
                < events.indexOf("disable:microservices-async:" + targets.get("snapshot")));
    }

    @Test void enablingStartsDependenciesBeforeConsentCanIssueCallbacks() {
        assertEquals(200, select("microservices-async").getStatusCode().value());
        var targets = routing.current().backends().get("microservices-async");
        assertTrue(events.indexOf("enable:microservices-async:" + targets.get("snapshot"))
                < events.indexOf("enable:microservices-async:" + targets.get("person")));
        assertTrue(events.indexOf("enable:microservices-async:" + targets.get("person"))
                < events.indexOf("enable:microservices-async:" + targets.get("consent")));
    }

    @Test void defaultAndServiceOverridesAreAllEnabledAndDrained() {
        var targets = routing.current().backends().get("microservices");
        replaceTargets("microservices", Map.of("default", targets.get("person"),
                "consent", targets.get("consent"), "snapshot", targets.get("snapshot")));
        assertEquals(200, select("microservices").getStatusCode().value());
        for (String url : targets.values()) {
            assertTrue(processes.get(url).path("enabled").asBoolean());
        }
        assertEquals(3, events.stream().filter(e -> e.startsWith("enable:microservices:")).count());
        events.clear();
        assertEquals(200, select("monolith").getStatusCode().value());
        for (String url : targets.values()) {
            assertFalse(processes.get(url).path("enabled").asBoolean());
        }
        assertEquals(3, events.stream().filter(e -> e.startsWith("disable:microservices:")).count());
    }

    @Test void unusedDefaultAndPlatformOnlyUrlsAreNotBusinessProcesses() {
        var targets = new LinkedHashMap<>(routing.current().backends().get("microservices"));
        targets.put("default", "http://unused-default.invalid");
        targets.put("platform", "http://platform-only.invalid");
        replaceTargets("microservices", targets);
        assertEquals(200, select("microservices").getStatusCode().value());
        assertEquals(3, events.stream().filter(e -> e.startsWith("enable:microservices:")).count());
        assertTrue(events.stream().noneMatch(e -> e.contains(".invalid")));
    }

    @Test void sharedEffectiveUrlsAreManagedOnceAndUnreachableOverrideFailsClosed() {
        String url = routing.current().backends().get("microservices").get("person");
        replaceTargets("microservices", Map.of("default", url, "person", url, "consent", url, "snapshot", url));
        assertEquals(200, select("microservices").getStatusCode().value());
        assertEquals(1, events.stream().filter(e -> e.startsWith("enable:microservices:")).count());
        events.clear(); unavailable = url;
        assertEquals(503, select("monolith").getStatusCode().value());
        assertFalse(runtime.allows("monolith"));
        assertTrue(events.stream().noneMatch(e -> e.startsWith("enable:")));
    }

    @SuppressWarnings("unchecked")
    private void replaceTargets(String architecture, Map<String, String> targets) {
        var previous = routing.current();
        var backends = new LinkedHashMap<>(previous.backends()); backends.put(architecture, targets);
        var replacement = new RoutingSnapshot(previous.version(), previous.epoch(), previous.active(), previous.mode(),
                backends, previous.disabled());
        ((java.util.concurrent.atomic.AtomicReference<RoutingSnapshot>) getField(routing, "current"))
                .set(replacement);
    }

    @Test void inFlightGatewayBusinessRequestDrainsBeforeBackendMutation() throws Exception {
        select("monolith"); events.clear();
        assertTrue(runtime.enter("monolith"));
        try (var executor = newSingleThreadExecutor()) {
            var result = executor.submit(() -> select("faas"));
            long deadline = nanoTime() + SECONDS.toNanos(1);
            while (runtime.allows("monolith") && nanoTime() < deadline) {
                sleep(5);
            }
            assertFalse(runtime.allows("monolith"));
            assertTrue(events.isEmpty());
            runtime.exit();
            assertEquals(200, result.get(3, SECONDS).getStatusCode().value());
        }
        assertTrue(runtime.allows("faas"));
    }

    @Test void unreachableProcessRefusesTheReplacementAndClosesAdmission() {
        select("monolith"); events.clear();
        unavailable = processes.entrySet().stream().filter(e -> e.getValue().path("architecture").asText().equals("microservices"))
                .findFirst().orElseThrow().getKey();
        assertEquals(503, select("faas").getStatusCode().value());
        assertTrue(events.stream().noneMatch(e -> e.startsWith("enable:")));
        assertEquals("unknown", runtime.phase());
        assertFalse(runtime.allows("monolith")); assertFalse(runtime.allows("faas"));
        unavailable = null;
        runtime.initialize();
        assertEquals("unknown", runtime.phase());
        assertEquals(409, select("monolith").getStatusCode().value());
    }

    @Test void slowObservationCannotBlockHandoverOrCertifyItsOldSnapshot() throws Exception {
        assertEquals(200, select("monolith").getStatusCode().value());
        assertEquals(true, runtime.observedStatus().get("observedCoherent"));
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        beforeStatus = process -> {
            if (currentThread().getName().equals("isolated-observer")) {
                entered.countDown();
                try {
                    if (!release.await(3, SECONDS)) {
                        throw new IllegalStateException("fixture timeout");
                    }
                }
                catch (InterruptedException interrupted) {
                    currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        };
        try (var executor = newSingleThreadExecutor(r -> new Thread(r, "isolated-observer"))) {
            var observation = executor.submit(runtime::observedStatus);
            try {
                assertTrue(entered.await(1, SECONDS));
                assertTimeoutPreemptively(ofSeconds(1), () ->
                        assertEquals(200, select("faas").getStatusCode().value()));
            } finally {
                release.countDown();
            }
            assertEquals(false, observation.get(2, SECONDS).get("observedCoherent"));
        }
        assertEquals(true, runtime.observedStatus().get("observedCoherent"));
    }

    @Test void slowStatusReadCannotBlockHandoverAndMarksConcurrentChangesIncoherent() throws Exception {
        assertEquals(200, select("monolith").getStatusCode().value());
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        beforeStatus = process -> {
            if (currentThread().getName().equals("isolated-runtime-observer")) {
                entered.countDown();
                try {
                    if (!release.await(3, SECONDS)) {
                        throw new IllegalStateException("fixture timeout");
                    }
                }
                catch (InterruptedException interrupted) {
                    currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        };
        try (var executor = newSingleThreadExecutor(r -> new Thread(r, "isolated-runtime-observer"))) {
            var observation = executor.submit(runtime::observedStatus);
            try {
                assertTrue(entered.await(1, SECONDS));
                assertTimeoutPreemptively(ofSeconds(1), () ->
                        assertEquals(200, select("faas").getStatusCode().value()));
            } finally {
                release.countDown();
            }
            assertEquals(false, observation.get(2, SECONDS).get("observedCoherent"));
        }
    }

    @Test void journalFailureNeverEnablesAnOwner() throws Exception {
        SelectionJournal journal = (SelectionJournal) getField(runtime, "journal");
        assertNotNull(journal); journal.close();
        assertEquals(503, select("monolith").getStatusCode().value());
        assertTrue(events.stream().noneMatch(e -> e.startsWith("enable:")));
        assertFalse(runtime.allows("monolith"));
    }
}
