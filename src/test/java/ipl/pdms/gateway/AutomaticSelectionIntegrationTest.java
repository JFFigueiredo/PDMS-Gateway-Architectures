package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.readString;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeControlClient;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionJournal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = {
        "spring.flyway.enabled=false", "pdms.gateway.runtime.managed=true", "pdms.gateway.run-id=automatic-test",
        "pdms.gateway.selection.allow-apply=true", "pdms.gateway.experiment.enabled=true", "pdms.gateway.health-interval=100ms",
        "pdms.gateway.health-success-threshold=1", "pdms.gateway.runtime.drain-timeout=3s"})
@AutoConfigureWebTestClient
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
class AutomaticSelectionIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "automatic-isolated-test";
    private static final Path DIR;
    private static final Map<String, String> ARCH_BY_URL = new LinkedHashMap<>();
    private static final List<HttpServer> SERVERS = new java.util.ArrayList<>();
    private static final java.util.concurrent.ExecutorService BACKENDS = newVirtualThreadPerTaskExecutor();
    private static volatile CountDownLatch entered;
    private static volatile CountDownLatch release;
    private static volatile String unhealthyBackend;
    private static volatile boolean ledgerAvailable;
    private static volatile int ledgerDelayPhase;
    static {
        try {
            DIR = createTempDirectory("pdms-automatic-selection");
            ObjectNode routing = JSON.createObjectNode().put("version", "routing/1").put("epoch", 0)
                    .put("active", "monolith").put("mode", "request");
            ObjectNode targets = routing.putObject("backends");
            for (String arch : ARCHITECTURES) {
                ObjectNode target = targets.putObject(arch);
                for (String service : arch.startsWith("microservices") ? List.of("person", "snapshot", "consent") : List.of("default")) {
                    HttpServer backend = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
                    backend.setExecutor(BACKENDS);
                    backend.createContext("/", exchange -> {
                        try {
                            if (exchange.getRequestURI().getPath().endsWith("/blocked")) {
                                entered.countDown(); release.await(5, SECONDS);
                            }
                            if (exchange.getRequestURI().getPath().endsWith("/slow")) {
                                sleep(350);
                            }
                            byte[] body = ("{\"status\":\"UP\",\"architecture\":\"" + arch + "\"}").getBytes(UTF_8);
                            exchange.getResponseHeaders().set("Content-Type", "application/json");
                            boolean healthFailure = arch.equals(unhealthyBackend)
                                    && exchange.getRequestURI().getPath().equals("/actuator/health");
                            exchange.sendResponseHeaders(healthFailure ? 503 : 200, body.length);
                            try (var out = exchange.getResponseBody()) {
                                out.write(body);
                            }
                        } catch (InterruptedException interrupted) {
                            currentThread().interrupt();
                        }
                        finally {
                            exchange.close();
                        }
                    });
                    backend.start(); SERVERS.add(backend);
                    String url = "http://localhost:" + backend.getAddress().getPort();
                    ARCH_BY_URL.put(url, arch); target.put(service, url);
                }
            }
            writeString(DIR.resolve("routing.json"), routing.toString());
            try (var input = AutomaticSelectionIntegrationTest.class.getResourceAsStream("/selection-experiment.json")) {
                ObjectNode experiment = (ObjectNode)JSON.readTree(input);
                experiment.put("warmupSeconds", 1);
                writeString(DIR.resolve("experiment.json"), experiment.toString());
            }
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
    @AfterAll static void stopBackends() {
        SERVERS.forEach(s -> s.stop(0));
        BACKENDS.close();
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("pdms.gateway.routing-file", () -> DIR.resolve("routing.json").toString());
        r.add("pdms.gateway.log-dir", () -> DIR.resolve("logs").toString());
        r.add("pdms.gateway.admin-token", () -> TOKEN);
        r.add("pdms.gateway.experiment.file", () -> "file:" + DIR.resolve("experiment.json"));
        r.add("pdms.gateway.outbox.admin-token", () -> "isolated-ledger-test");
    }
    @MockitoBean RuntimeControlClient management;
    @Autowired RuntimeCoordinator runtime;
    @Autowired RoutingTable routing;
    @Autowired SelectionJournal journal;
    @Autowired ipl.pdms.gateway.experiment.ExperimentControl experiment;
    @MockitoSpyBean RequestLog requestLog;
    @org.springframework.test.context.bean.override.mockito.MockitoSpyBean
    ipl.pdms.gateway.monitoring.TrafficStatistics stats;
    @Autowired WebTestClient client;
    @LocalServerPort int port;
    final Map<String, ObjectNode> states = new ConcurrentHashMap<>();
    volatile String failEnable;
    volatile boolean closeJournalOnEnable;

    @BeforeEach void prepare() throws Exception {
        unhealthyBackend = null;
        ledgerAvailable = false;
        ledgerDelayPhase = 0;
        entered = new CountDownLatch(1); release = new CountDownLatch(1);
        ARCH_BY_URL.forEach((url, arch) -> {
            ObjectNode state = JSON.createObjectNode().put("architecture", arch).put("instanceId", randomUUID().toString())
                    .put("enabled", false).put("drained", true);
            state.putObject("snapshot").put("version", 0).putObject("policy").put("enabled", false);
            states.put(url, state);
        });
        when(management.status(any())).thenAnswer(call -> observedProcess(call.getArgument(0)));
        when(management.set(any(), anyBoolean(), anySet(), anyString())).thenAnswer(call -> {
            RuntimeControlClient.Process process = call.getArgument(0); boolean enabled = call.getArgument(1);
            if (enabled && process.architecture().equals(failEnable)) {
                throw new IllegalStateException("Isolated failed enable");
            }
            if (enabled && closeJournalOnEnable && process.architecture().equals("faas")) {
                journal.close();
            }
            ObjectNode state = states.get(process.url());
            state.put("enabled", enabled).put("drained", true);
            ObjectNode snapshot = (ObjectNode)state.path("snapshot"); snapshot.put("version", snapshot.path("version").asLong() + 1);
            ((ObjectNode)snapshot.path("policy")).put("enabled", enabled).set("allowedEventTypes", JSON.valueToTree(call.getArgument(2)));
            return state.deepCopy();
        });
        long deadline = nanoTime() + ofSeconds(6).toNanos();
        while (get("/admin/health").path("realisations").properties().stream().anyMatch(e -> !e.getValue().path("status").asText().equals("UP"))) {
            if (nanoTime() > deadline) {
                fail("Backend health did not become UP");
            } sleep(50);
        }
        await(() -> runtime.phase().equals("ready"));
        assertTrue(runtime.allows("monolith"));
    }
    private JsonNode get(String path) throws Exception {
        return JSON.readTree(client.get().uri(path).header("Authorization", "Bearer " + TOKEN).exchange()
                .expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
    }
    private ObjectNode observedProcess(RuntimeControlClient.Process process) {
        var state = states.get(process.url()).deepCopy();
        if (process.architecture().equals("decentralized")) {
            state.put("blockchainAvailable", ledgerAvailable);
            state.putArray("effectiveEventTypes").add("CONSENT_LEDGER_ANCHOR");
            var anchors = state.putArray("effectiveOnchainAnchorTypes");
            if (ledgerAvailable) {
                anchors.add("CONSENT_CREATED").add("CONSENT_UPDATED").add("CONSENT_WITHDRAWN").add("CONSENT_DELETED");
            }
            state.putObject("fixtureLedgerDelay").put("synthetic", true).put("phase", ledgerDelayPhase)
                .put("pending", new int[]{0, 10, 20, 0}[ledgerDelayPhase])
                .put("oldestPendingAgeMs", new int[]{0, 2000, 5000, 0}[ledgerDelayPhase])
                .put("confirmed", ledgerDelayPhase == 3 ? 20 : 0);
        }
        return state;
    }

    @Test void removedOutboxAdminEndpointsAreUnavailableAndRuntimeRequiresAuthentication() {
        for (var path : List.of("/admin/outbox", "/admin/outbox/ledger")) {
            client.get().uri(path).header("Authorization", "Bearer " + TOKEN).exchange().expectStatus().isNotFound();
        }
        client.post().uri("/admin/outbox").header("Authorization", "Bearer " + TOKEN)
            .contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isNotFound();
        client.patch().uri("/admin/outbox").header("Authorization", "Bearer " + TOKEN)
            .contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isNotFound();
        client.get().uri("/admin/runtime").exchange().expectStatus().isUnauthorized();
        client.post().uri("/admin/runtime").contentType(APPLICATION_JSON).bodyValue("{}")
            .exchange().expectStatus().isEqualTo(405);
    }

    private ObjectNode envelope() throws Exception {
        return JSON.createObjectNode().put("requestId", randomUUID().toString())
                .put("expectedVersion", get("/admin/selection").path("version").asLong());
    }
    private void post(String path, ObjectNode body) {
        client.post().uri(path).header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON)
                .bodyValue(body.toString()).exchange().expectStatus().isOk();
    }
    private void put(String path, ObjectNode body) {
        client.put().uri(path).header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON)
                .bodyValue(body.toString()).exchange().expectStatus().isOk();
    }
    private void configure(boolean real) throws Exception {
        post("/admin/selection/scenarios/read-crossover", envelope());
        ObjectNode config = get("/admin/selection").path("configuration").deepCopy();
        config.put("autoEvaluate", true).put("applyChanges", true);
        ObjectNode tuning = (ObjectNode)config.path("tuning");
        tuning.put("tickMs", 100).put("breachTicks", 1).put("minSamples", 1).put("highP99Ms", 100).put("recoveryP99Ms", 50);
        if (real) {
            config.put("source", "real").put("workload", "mix");
            tuning.put("highP99Ms", 300).put("recoveryP99Ms", 250);
            // Profiles combine archived local measurements; they make no cross-environment performance claim.
            Map<String, double[]> measured = Map.of(
                    "monolith", new double[]{404.1, 99.368},
                    "faas", new double[]{379.216, 75.325},
                    "microservices", new double[]{249.172, 127.714},
                    "microservices-async", new double[]{248.164, 345.472},
                    "decentralized", new double[]{395.692, 109.866});
            ObjectNode profiles = (ObjectNode)config.path("profiles");
            profiles.removeAll();
            measured.forEach((architecture, values) -> profiles.putObject(architecture).putObject("mix")
                    .put("evidenceKind", "measured")
                    .put("provenance", "local gateway logs p99; results mix t15 throughput support")
                    .put("minRateRps", 0).put("maxRateRps", values[0]).put("capacityRps", values[0])
                    .put("predictedP99Ms", values[1]));
        }
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update);
    }
    private void freeze() throws Exception {
        post("/admin/selection/freeze", envelope().put("runId", "automatic-test"));
    }
    private void onlyHealthy(String target) throws Exception {
        ObjectNode mock = get("/admin/selection").path("configuration").path("mock").deepCopy();
        mock.put("sampleTimestampEpochMs", currentTimeMillis());
        for (String name : ARCHITECTURES) {
            ((ObjectNode)mock.path("healthy")).put(name, name.equals(target));
        }
        ObjectNode update = envelope(); update.set("mock", mock); put("/admin/selection/mock", update);
    }
    private HttpRequest business(String path) {
        return business(path, "read");
    }
    private HttpRequest business(String path, String plan) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/person/" + path)).timeout(ofSeconds(8))
                .header("X-Run-Id", "automatic-test").header("X-Plan-Id", plan)
                .header("X-Session-Id", randomUUID().toString()).header("X-Request-Id", randomUUID().toString()).GET().build();
    }
    private void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = nanoTime() + ofSeconds(8).toNanos();
        while (!condition.getAsBoolean()) {
            if (nanoTime() > deadline) {
                fail("Timed out waiting for automatic selection");
            }
            sleep(25);
        }
    }
    private void awaitTarget(String target) throws Exception {
        await(() -> routing.current().active().equals(target) && runtime.phase().equals("ready") && runtime.allows(target));
        var response = newHttpClient().send(business("probe"), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode()); assertEquals(target, JSON.readTree(response.body()).path("architecture").asText());
    }
    @Test void explicitEvaluationCannotSwitchEvenWithAFrozenApplyPolicy() throws Exception {
        configure(false);
        ObjectNode configuration = get("/admin/selection").path("configuration").deepCopy();
        configuration.put("autoEvaluate", false);
        ObjectNode update = envelope(); update.set("configuration", configuration);
        put("/admin/selection", update);
        onlyHealthy("faas"); freeze();
        long epoch = routing.current().epoch();
        ObjectNode request = envelope().put("expectedEpoch", epoch);
        JsonNode decision = JSON.readTree(client.post().uri("/admin/selection/evaluate")
                .header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON).bodyValue(request.toString())
                .exchange().expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("switch", decision.path("action").asText());
        assertEquals("faas", decision.path("target").asText());
        assertFalse(decision.path("applied").asBoolean());
        assertFalse(decision.path("applyRequested").asBoolean());
        assertEquals(epoch, routing.current().epoch());
        assertTrue(runtime.allows("monolith"));
        assertFalse(runtime.allows("faas"));
        states.values().forEach(state -> assertEquals(state.path("architecture").asText().equals("monolith"),
                state.path("enabled").asBoolean()));
    }

    @Test void automaticSelectionReachesAllFiveAndMovesHttpAndWorkersTogether() throws Exception {
        configure(false); onlyHealthy("faas"); long epoch = routing.current().epoch();
        sleep(1100); assertEquals(epoch, routing.current().epoch(), "Unfrozen policy must not apply automatically");
        freeze();
        for (String target : List.of("faas", "microservices", "microservices-async", "decentralized", "monolith")) {
            onlyHealthy(target); awaitTarget(target);
            states.values().forEach(s -> assertEquals(s.path("architecture").asText().equals(target), s.path("enabled").asBoolean()));
        }
        assertTrue(routing.current().epoch() > epoch);
    }
    @Test void liveGatewayLatencyTriggersAnAutomaticCutover() throws Exception {
        configure(true);
        assertEquals(200, newHttpClient().send(business("slow", "mix"), discarding()).statusCode());
        freeze(); awaitTarget("faas");
        JsonNode decision = get("/admin/selection").path("lastDecision");
        assertEquals("latency_breach", decision.path("reason").asText());
        assertEquals("faas", decision.path("target").asText());
        assertTrue(decision.path("signals").path("p99Ms").asDouble() > 300, decision.toString());
        String records = readString(DIR.resolve("logs").resolve(journal.fileName()));
        assertTrue(records.contains("live_observation")); assertTrue(records.contains("latency_breach"));
        assertTrue(records.contains("selection_application_outcome"));
    }
    @Test void liveGatewayArrivalRateTriggersAHeadroomCutover() throws Exception {
        var initial = runtime.switchTo("microservices", routing.current().epoch(), randomUUID().toString());
        assertTrue(initial.getStatusCode().is2xxSuccessful(), initial.toString());
        assertEquals("microservices", routing.current().active());

        configure(true);
        ObjectNode config = get("/admin/selection").path("configuration").deepCopy();
        ObjectNode tuning = (ObjectNode)config.path("tuning");
        tuning.put("tickMs", 100).put("windowSeconds", 5).put("minSamples", 1).put("maxAgeMs", 10000)
                .put("maxUtilization", 0.20).put("highP99Ms", 10000).put("recoveryP99Ms", 9000);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update);
        freeze();

        HttpClient http = newHttpClient();
        List<java.util.concurrent.CompletableFuture<HttpResponse<Void>>> requests = new java.util.ArrayList<>();
        long next = nanoTime();
        for (int i = 0; i < 400; i++) {
            requests.add(http.sendAsync(business("headroom-" + i, "mix"), discarding()));
            next += ofMillis(15).toNanos();
            long wait = next - nanoTime();
            if (wait > 0) {
                NANOSECONDS.sleep(wait);
            }
        }
        for (var request : requests) {
            assertEquals(200, request.get(10, SECONDS).statusCode());
        }

        Path journalPath = DIR.resolve("logs").resolve(journal.fileName());
        await(() -> {
            try {
                String saved = readString(journalPath);
                return saved.contains("\"event\":\"selection_application_outcome\"")
                        && saved.contains("\"reason\":\"headroom\"") && saved.contains("\"activeAfter\":\"faas\"");
            } catch (Exception unavailable) {
                return false;
            }
        });
        String records = readString(journalPath);
        JsonNode decision = null, outcome = null;
        for (String line : records.lines().toList()) {
            JsonNode event = JSON.readTree(line);
            if (event.path("event").asText().equals("selection_decision")
                    && event.path("reason").asText().equals("headroom")) {
                decision = event;
            }
            if (event.path("event").asText().equals("selection_application_outcome")
                    && event.path("reason").asText().equals("headroom")) {
                outcome = event;
            }
        }
        assertNotNull(decision, records);
        assertNotNull(outcome, records);
        assertTrue(outcome.path("applied").asBoolean());
        assertEquals("faas", outcome.path("activeAfter").asText());
        assertEquals("faas", routing.current().active());
        assertEquals("live_observation", decision.path("evidenceKind").asText());
        assertEquals("microservices", decision.path("active").asText());
        assertEquals("faas", decision.path("target").asText());
        assertTrue(decision.path("signals").path("arrivalRps").asDouble() > 49.8, decision.toString());
        JsonNode active = stream(decision.path("candidates").spliterator(), false)
                .filter(candidate -> candidate.path("architecture").asText().equals("microservices"))
                .findFirst().orElseThrow();
        assertEquals("insufficient_headroom", active.path("reason").asText());
        var destination = http.send(business("post-cutover", "mix"), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, destination.statusCode());
        assertEquals("faas", JSON.readTree(destination.body()).path("architecture").asText());
    }
    @Test void liveBackendHealthFailureTriggersAnAutomaticCutover() throws Exception {
        configure(true); freeze();
        try {
            unhealthyBackend = "monolith";
            awaitTarget("faas");
            assertEquals("DOWN", get("/admin/health").path("realisations").path("monolith").path("status").asText());
            await(() -> {
                try {
                    for (String line : readAllLines(DIR.resolve("logs").resolve(journal.fileName()))) {
                        JsonNode event = JSON.readTree(line);
                        if (event.path("event").asText().equals("selection_application_outcome")
                                && event.path("reason").asText().equals("availability_or_requirement")
                                && event.path("applied").asBoolean()) {
                            return true;
                        }
                    }
                } catch (Exception unavailable) {
                    return false;
                }
                return false;
            });
            boolean decisionFound = false, appliedFound = false;
            for (String line : readAllLines(DIR.resolve("logs").resolve(journal.fileName()))) {
                JsonNode event = JSON.readTree(line);
                if (!event.path("reason").asText().equals("availability_or_requirement")) {
                    continue;
                }
                if (event.path("event").asText().equals("selection_decision")) {
                    assertEquals("real", event.path("source").asText());
                    assertEquals("live_observation", event.path("evidenceKind").asText());
                    assertFalse(event.path("signals").path("healthy").path("monolith").asBoolean(true));
                    assertEquals("monolith", event.path("active").asText());
                    assertEquals("faas", event.path("target").asText());
                    decisionFound = true;
                }
                if (event.path("event").asText().equals("selection_application_outcome")) {
                    assertTrue(event.path("applied").asBoolean());
                    assertEquals("faas", event.path("activeAfter").asText());
                    appliedFound = true;
                }
            }
            assertTrue(decisionFound); assertTrue(appliedFound);
        } finally {
            unhealthyBackend = null;
        }
    }
    @Test void anchoringRequirementCutsOverWithoutRequestLogHealthOrLedgerReadiness() throws Exception {
        doReturn(false).when(requestLog).isComplete();
        doReturn(false).when(requestLog).writerHealthy();
        configure(true); freeze();
        ObjectNode context = envelope().put("workload", "mix");
        context.putArray("requiredCapabilities").add("consent_integrity_anchor");
        put("/admin/selection/context", context);
        awaitTarget("decentralized");
        // Status waits for the evaluator to persist its outcome.
        get("/admin/selection");
        boolean contextFound = false, appliedFound = false;
        for (String line : readAllLines(DIR.resolve("logs").resolve(journal.fileName()))) {
            JsonNode event = JSON.readTree(line);
            if (event.path("event").asText().equals("selection_context")) {
                contextFound |= event.path("requiredCapabilities").toString().contains("consent_integrity_anchor");
            }
            if (event.path("event").asText().equals("selection_application_outcome")) {
                assertEquals("real", event.path("source").asText());
                assertEquals("availability_or_requirement", event.path("reason").asText());
                assertFalse(event.path("signals").has("ledgerReady"));
                assertFalse(event.path("signals").has("evidenceHealthy"));
                assertTrue(event.path("applied").asBoolean());
                assertEquals("decentralized", event.path("activeAfter").asText());
                appliedFound = true;
            }
        }
        assertTrue(contextFound); assertTrue(appliedFound);
    }
    @Test void ledgerDelayObservationsKeepTheRequiredAnchoringRoute() throws Exception {
        configure(true); freeze();
        ledgerAvailable = true;
        try {
            ObjectNode context = envelope().put("workload", "mix");
            context.putArray("requiredCapabilities").add("consent_integrity_anchor");
            put("/admin/selection/context", context);
            await(() -> routing.current().active().equals("decentralized")
                    && runtime.phase().equals("ready") && runtime.allows("decentralized"));
            get("/admin/selection");
            long epoch = routing.current().epoch();
            Path observations = DIR.resolve("logs").resolve("t5-observations-" + runtime.instanceId() + ".jsonl");
            HttpClient http = newHttpClient();
            for (int phase = 0; phase < 4; phase++) {
                ledgerDelayPhase = phase;
                JsonNode ledger = get("/admin/runtime").path("processes").properties().stream()
                    .map(Map.Entry::getValue).filter(process -> process.path("architecture").asText().equals("decentralized"))
                    .findFirst().orElseThrow();
                JsonNode delay = ledger.path("fixtureLedgerDelay");
                assertTrue(delay.path("synthetic").asBoolean());
                assertEquals(new int[]{0, 10, 20, 0}[phase], delay.path("pending").asInt(-1));
                assertEquals(new int[]{0, 2000, 5000, 0}[phase], delay.path("oldestPendingAgeMs").asInt(-1));
                assertEquals(phase == 3 ? 20 : 0, delay.path("confirmed").asInt(-1));
                var response = http.send(business("ledger-delay-" + phase, "mix"), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertEquals("decentralized", JSON.readTree(response.body()).path("architecture").asText());
                long completed = currentTimeMillis();
                await(() -> {
                    try {
                        return get("/admin/selection").path("lastDecision").path("evaluatedAtEpochMs").asLong() > completed;
                    }
                    catch (Exception unavailable) {
                        return false;
                    }
                });
                JsonNode decision = get("/admin/selection").path("lastDecision");
                assertEquals("keep_active", decision.path("reason").asText());
                assertEquals(epoch, routing.current().epoch());
                ObjectNode captured = JSON.createObjectNode().put("event", "t5_fixture_observation")
                        .put("atEpochMs", currentTimeMillis()).put("routingEpoch", epoch);
                captured.set("ledger", ledger); captured.set("decision", decision);
                writeString(observations, captured + "\n", CREATE,
                        APPEND);
            }
        } finally {
            ledgerAvailable = false;
            ledgerDelayPhase = 0;
        }
    }
    @Test void embeddedConsentRequirementExcludesBothSplitVariantsAndCutsOver() throws Exception {
        requirementCutsOver("microservices", "person_update_persists_embedded_consents",
                java.util.Set.of("microservices", "microservices-async"));
    }
    @Test void snapshotRequirementExcludesAsyncAndCutsOver() throws Exception {
        requirementCutsOver("microservices-async", "read_your_writes_snapshot",
                java.util.Set.of("microservices-async"));
    }
    private void requirementCutsOver(String initialOwner, String requirement, java.util.Set<String> excluded) throws Exception {
        assertTrue(runtime.switchTo(initialOwner, routing.current().epoch(), randomUUID().toString()).getStatusCode().is2xxSuccessful());
        configure(true); freeze();
        JsonNode before = get("/admin/selection");
        assertEquals(initialOwner, routing.current().active());
        assertTrue(before.path("configuration").path("requiredCapabilities").isEmpty());
        String frozenHash = before.path("policySha256").asText();
        ObjectNode context = envelope().put("workload", "mix");
        context.putArray("requiredCapabilities").add(requirement);
        put("/admin/selection/context", context);
        awaitTarget("faas");
        JsonNode after = get("/admin/selection");
        assertEquals(frozenHash, after.path("policySha256").asText());
        assertTrue(after.path("version").asLong() > before.path("version").asLong());
        boolean contextFound = false, appliedFound = false;
        for (String line : readAllLines(DIR.resolve("logs").resolve(journal.fileName()))) {
            JsonNode event = JSON.readTree(line);
            if (event.path("event").asText().equals("selection_context")) {
                contextFound |= event.path("requiredCapabilities").toString().contains(requirement);
            }
            if (!event.path("event").asText().equals("selection_application_outcome")) {
                continue;
            }
            assertEquals("real", event.path("source").asText());
            assertEquals("availability_or_requirement", event.path("reason").asText());
            assertEquals(initialOwner, event.path("active").asText());
            assertTrue(event.path("applied").asBoolean());
            assertEquals("faas", event.path("activeAfter").asText());
            for (JsonNode candidate : event.path("candidates")) {
                boolean incompatible = excluded.contains(candidate.path("architecture").asText());
                assertEquals(!incompatible, candidate.path("eligible").asBoolean());
                assertEquals(incompatible ? "missing_capability" : "eligible", candidate.path("reason").asText());
            }
            appliedFound = true;
        }
        assertTrue(contextFound); assertTrue(appliedFound);
    }
    @Test void frozenObservationTimestampHoldsPerformanceSwitchingUntilRecovery() throws Exception {
        configure(true);
        ObjectNode config = get("/admin/selection").path("configuration").deepCopy();
        ((ObjectNode)config.path("tuning")).put("maxAgeMs", 2000);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update);
        HttpClient http = newHttpClient();
        assertEquals(200, http.send(business("slow", "mix"), discarding()).statusCode());
        await(() -> stats.snapshot(30).get("source_timestamp_epoch_ms") != null);
        long frozenAt = ((Number)stats.snapshot(30).get("source_timestamp_epoch_ms")).longValue();
        java.util.concurrent.atomic.AtomicLong actualAt = new java.util.concurrent.atomic.AtomicLong();
        // Freeze only the exported timestamp; retain real samples and rates.
        doAnswer(call -> {
            @SuppressWarnings("unchecked") Map<String, Object> window = (Map<String, Object>)call.callRealMethod();
            actualAt.set(((Number)window.get("source_timestamp_epoch_ms")).longValue());
            window.put("source_timestamp_epoch_ms", frozenAt);
            return window;
        }).when(stats).snapshot(anyInt());
        Path captured = DIR.resolve("logs").resolve("t6-observations-" + runtime.instanceId() + ".jsonl");
        try {
            await(() -> currentTimeMillis() - frozenAt > 2000);
            freeze(); long epoch = routing.current().epoch();
            for (int phase = 0; phase < 3; phase++) {
                var response = http.send(business("fresh-traffic-" + phase, "mix"), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertEquals("monolith", JSON.readTree(response.body()).path("architecture").asText());
                long completed = currentTimeMillis();
                await(() -> {
                    try {
                        return get("/admin/selection").path("lastDecision").path("evaluatedAtEpochMs").asLong() > completed;
                    }
                    catch (Exception unavailable) {
                        return false;
                    }
                });
                JsonNode decision = get("/admin/selection").path("lastDecision");
                assertEquals("stale_or_invalid_signal", decision.path("reason").asText());
                assertTrue(decision.path("signals").path("valid").asBoolean());
                assertTrue(decision.path("signals").path("samples").asInt() > 0);
                assertEquals(frozenAt, decision.path("signals").path("sampledAtEpochMs").asLong());
                assertTrue(decision.path("evaluatedAtEpochMs").asLong() - frozenAt > 2000);
                assertTrue(actualAt.get() > frozenAt);
                assertEquals(epoch, routing.current().epoch());
                ObjectNode row = JSON.createObjectNode().put("event", "t6_timestamp_hook")
                        .put("phase", phase).put("actualSourceTimestamp", actualAt.get()).put("frozenTimestamp", frozenAt);
                row.set("decision", decision);
                writeString(captured, row + "\n", CREATE, APPEND);
            }
            doCallRealMethod().when(stats).snapshot(anyInt());
            assertEquals(200, http.send(business("slow", "mix"), discarding()).statusCode());
            awaitTarget("faas");
            get("/admin/selection");
            JsonNode recovered = null;
            for (String line : readAllLines(DIR.resolve("logs").resolve(journal.fileName()))) {
                JsonNode event = JSON.readTree(line);
                if (event.path("event").asText().equals("selection_application_outcome")
                        && event.path("reason").asText().equals("latency_breach")) {
                    recovered = event;
                }
            }
            assertNotNull(recovered);
            assertTrue(recovered.path("applied").asBoolean());
            assertTrue(recovered.path("signals").path("sampledAtEpochMs").asLong() > frozenAt);
            assertTrue(recovered.path("evaluatedAtEpochMs").asLong()
                    - recovered.path("signals").path("sampledAtEpochMs").asLong() <= 2000);
        } finally {
            doCallRealMethod().when(stats).snapshot(anyInt());
        }
    }
    @Test void managedAutomaticSwitchDrainsAnAlreadyAdmittedRequest() throws Exception {
        configure(false); onlyHealthy("faas");
        var pending = newHttpClient().sendAsync(business("blocked"), HttpResponse.BodyHandlers.ofString());
        assertTrue(entered.await(2, SECONDS));
        java.util.concurrent.CompletableFuture<HttpResponse<String>> waiting = null;
        try {
            freeze(); await(() -> runtime.phase().equals("transitioning"));
            assertFalse(pending.isDone());
            waiting = newHttpClient().sendAsync(business("during-drain"), HttpResponse.BodyHandlers.ofString());
            sleep(100);
            assertFalse(waiting.isDone(), "New requests must wait without blocking the old owner's drain");
        } finally {
            release.countDown();
        }
        assertEquals("monolith", JSON.readTree(pending.get(5, SECONDS).body()).path("architecture").asText());
        var resumed = waiting.get(5, SECONDS);
        assertEquals(200, resumed.statusCode());
        assertEquals("faas", JSON.readTree(resumed.body()).path("architecture").asText());
        awaitTarget("faas");
    }
    @Test void uncertainHandoverPausesAutomaticApplicationAndLeavesAdmissionClosed() throws Exception {
        configure(false); onlyHealthy("faas"); failEnable = "faas"; freeze();
        await(() -> runtime.phase().equals("unknown"));
        JsonNode status = get("/admin/selection");
        assertEquals("routing_reconciliation_required", status.path("automaticPauseReason").asText());
        assertFalse(status.path("lastDecision").path("applied").asBoolean());
        long epoch = routing.current().epoch(); onlyHealthy("decentralized"); sleep(1100);
        assertEquals(epoch, routing.current().epoch());
        assertEquals(503, newHttpClient().send(business("closed"), discarding()).statusCode());
    }
    @Test void missingOutcomeRecordPausesAfterAnOtherwiseSuccessfulCutover() throws Exception {
        configure(false); onlyHealthy("faas"); closeJournalOnEnable = true; freeze(); awaitTarget("faas");
        JsonNode status = get("/admin/selection");
        assertEquals("outcome_journal_unavailable_reconcile_routing", status.path("automaticPauseReason").asText());
        assertTrue(status.path("lastDecision").path("applied").asBoolean());
        assertTrue(status.path("lastDecision").path("outcomeUnknown").asBoolean());
        long epoch = routing.current().epoch(); sleep(1100); assertEquals(epoch, routing.current().epoch());
        assertFalse(status.path("journalHealthy").asBoolean());
        String saved = readString(DIR.resolve("logs").resolve(journal.fileName()));
        assertTrue(saved.contains("selection_decision"));
        assertFalse(saved.contains("selection_application_outcome"));
        ObjectNode captured = JSON.createObjectNode().put("event", "t8_outcome_write_failure")
                .put("routingEpochAfterPause", epoch);
        captured.set("status", status);
        writeString(DIR.resolve("logs").resolve("t8-outcome-failure-" + journal.fileName() + ".json"), captured.toString());
    }
    @Test void delayIsMeasuredAndAdaptiveSequenceChangesTheActualDestination() throws Exception {
        runDelayExperiment("adaptive", "faas");
    }
    @Test void referenceSequenceKeepsTheSameOwnerUnderTheSameMeasuredDelay() throws Exception {
        runDelayExperiment("reference", "monolith");
    }
    private void runDelayExperiment(String sequence, String expected) throws Exception {
        experiment.start(new ipl.pdms.gateway.experiment.ExperimentControl.Start("automatic-test", "T1", "read", sequence));
        var running = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (var clients = newSingleThreadExecutor()) {
            var traffic = clients.submit(() -> {
                var http = newHttpClient();
                while (running.get()) {
                    assertEquals(200, http.send(business("injected-latency"), discarding()).statusCode());
                    sleep(25);
                }
                return null;
            });
            try {
                if (sequence.equals("adaptive")) {
                    awaitTarget(expected);
                } else {
                    await(() -> ((Number)((Map<?, ?>)experiment.status().get("stimulus")).get("affectedRequests")).longValue() >= 8);
                }
                assertEquals("running", experiment.status().get("phase"));
            } finally {
                running.set(false);
                experiment.stop();
            }
            traffic.get(5, SECONDS);
        }
        assertEquals(expected, routing.current().active());
        assertTrue(((Number)stats.snapshot(30).get("p99_ms")).doubleValue() >= 350,
                "Normal statistics must include actual injected waiting");
        assertEquals("real", get("/admin/selection").path("configuration").path("source").asText());
        assertEquals("live_observation", get("/admin/selection").path("lastDecision").path("evidenceKind").asText());
        String saved = readString(DIR.resolve("logs").resolve(journal.fileName()));
        assertTrue(saved.contains("experiment_stimulus")); assertFalse(saved.contains("selection_mock_inputs"));
    }
    @Test void gatewayExperimentInjectsConditionWhileClientsSendOnlyBusinessTraffic() throws Exception {
        client.get().uri("/admin/experiment").exchange().expectStatus().isUnauthorized();
        experiment.start(new ipl.pdms.gateway.experiment.ExperimentControl.Start("automatic-test", "T3", "read", "adaptive"));
        var running = new java.util.concurrent.atomic.AtomicBoolean(true);
        var responses = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
        try (var clients = newSingleThreadExecutor()) {
            var traffic = clients.submit(() -> {
                var http = newHttpClient();
                while (running.get()) {
                    responses.add(http.send(business("experiment-traffic"), discarding()).statusCode());
                    sleep(50);
                }
                return null;
            });
            try {
                awaitTarget("microservices");
                assertEquals("running", get("/admin/experiment").path("phase").asText());
                assertEquals("availability_or_requirement", get("/admin/selection").path("lastDecision").path("reason").asText());
                experiment.stop();
            } finally {
                running.set(false);
            }
            traffic.get(5, SECONDS);
        }
        assertTrue(responses.size() > 10);
        assertTrue(responses.stream().anyMatch(code -> code == 503), "The HTTP fault must reach clients");
        assertTrue(responses.stream().anyMatch(code -> code == 200), "Unaffected traffic must complete");
        assertEquals("real", get("/admin/selection").path("configuration").path("source").asText());
        assertEquals("stopped", get("/admin/experiment").path("phase").asText());
        assertEquals("experiment_stopped", get("/admin/selection").path("automaticPauseReason").asText());
        assertEquals("microservices", routing.current().active(), "Stopping inputs must not force a reverse cutover");
    }
}
