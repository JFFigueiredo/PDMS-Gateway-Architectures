package ipl.pdms.gateway;

import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.net.http.HttpClient.newHttpClient;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.writeString;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.http.MediaType.APPLICATION_JSON;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = RANDOM_PORT,
        properties = {"spring.flyway.enabled=false", "pdms.gateway.runtime.managed=false"})
@AutoConfigureWebTestClient
@AutoConfigureObservability
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GatewayIntegrationTest {

    private static final String TOKEN = "test-admin-token";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Path DIR;
    private static final HttpServer MONOLITH;
    private static final HttpServer MS_PERSON;
    private static final HttpServer MS_SNAPSHOT;
    private static final HttpServer MS_CONSENT;
    private static final int DEAD_PORT;
    private static final ServerSocket RESETTER;
    private static final Set<String> DOWN = newKeySet();

    static {
        try {
            DIR = createTempDirectory("pdms-gateway-test");
            MONOLITH = mock("monolith");
            MS_PERSON = mock("ms-person");
            MS_SNAPSHOT = mock("ms-snapshot");
            MS_CONSENT = mock("ms-consent");
            try (ServerSocket probe = new ServerSocket(0)) {
                DEAD_PORT = probe.getLocalPort();
            }
            RESETTER = startResetter();
            String routing = """
                    {"version":"routing/1","epoch":0,"active":"monolith","mode":"request",
                     "backends":{
                       "monolith":{"default":"http://localhost:%d"},
                       "microservices":{"person":"http://localhost:%d","snapshot":"http://localhost:%d","consent":"http://localhost:%d"},
                       "dead":{"default":"http://localhost:%d"},
                       "resetter":{"default":"http://localhost:%d"}
                     }}""".formatted(port(MONOLITH), port(MS_PERSON), port(MS_SNAPSHOT), port(MS_CONSENT),
                    DEAD_PORT, RESETTER.getLocalPort());
            writeString(DIR.resolve("routing.json"), routing);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("pdms.gateway.routing-file", () -> DIR.resolve("routing.json").toString());
        registry.add("pdms.gateway.log-dir", () -> DIR.resolve("logs").toString());
        registry.add("pdms.gateway.admin-token", () -> TOKEN);
        registry.add("pdms.gateway.run-id", () -> "r1");
        registry.add("spring.cloud.gateway.server.webflux.httpclient.response-timeout", () -> "1s");
        registry.add("pdms.gateway.health-interval", () -> "100ms");
        registry.add("pdms.gateway.health-timeout", () -> "500ms");
        registry.add("pdms.gateway.admission-max-in-flight", () -> "2");
        registry.add("pdms.gateway.admission-queue", () -> "1");
        registry.add("pdms.gateway.admission-max-wait", () -> "300ms");
    }

    @Autowired
    WebTestClient client;

    @Autowired
    MeterRegistry registry;

    @LocalServerPort
    int serverPort;

    private WebTestClient.RequestHeadersSpec<?> get(String path, String session) {
        return client.get().uri(java.net.URI.create(path))
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix")
                .header("X-Session-Id", session).header("X-Request-Id", session + "-" + nanoTime());
    }

    private JsonNode admin(String path) throws IOException {
        return body(client.get().uri(path).header(AUTHORIZATION, "Bearer " + TOKEN)
                .exchange().expectStatus().isOk());
    }

    private void awaitHealth(String realisation, String wanted) throws Exception {
        String last = null;
        for (int i = 0; i < 100; i++) {
            last = admin("/admin/health").get("realisations").get(realisation).get("status").asText();
            if (wanted.equals(last)) {
                return;
            }
            sleep(50);
        }
        throw new AssertionError(realisation + " health stayed " + last + ", wanted " + wanted);
    }

    private JsonNode body(WebTestClient.ResponseSpec spec) throws IOException {
        byte[] bytes = spec.expectBody().returnResult().getResponseBodyContent();
        return MAPPER.readTree(bytes);
    }

    @Test
    @Order(1)
    void routesToActiveRealisationAndForwardsAuthorizationUnmodified() throws IOException {
        WebTestClient.ResponseSpec r = client.get().uri("/api/person/abc")
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix")
                .header("X-Session-Id", "s-a").header("X-Request-Id", "q1")
                .header(AUTHORIZATION, "Bearer abc.def.ghi")
                .exchange().expectStatus().isOk();
        JsonNode json = body(r);
        assertEquals("monolith", json.get("backend").asText());
        assertEquals("/api/person/abc", json.get("target").asText());
        assertEquals("Bearer abc.def.ghi", json.get("authorization").asText());
    }

    @Test
    @Order(2)
    void rawPathAndQueryAreForwardedByteForByte() throws IOException {
        JsonNode json = body(get("/api/snapshot/by-email/a%40b.example?x=%2B1&y=a+b", "s-enc")
                .exchange().expectStatus().isOk());
        assertEquals("/api/snapshot/by-email/a%40b.example?x=%2B1&y=a+b", json.get("target").asText());
    }

    @Test
    @Order(3)
    void rejectsUnknownPathsWrongMethodsAndMissingIdentity() {
        client.post().uri("/api/unknown")
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix")
                .header("X-Session-Id", "s-x").header("X-Request-Id", "q")
                .exchange().expectStatus().isNotFound();
        client.delete().uri("/api/snapshot/person/abc/latest")
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix")
                .header("X-Session-Id", "s-x").header("X-Request-Id", "q")
                .exchange().expectStatus().isEqualTo(405).expectHeader().valueEquals("Allow", "GET");
        client.get().uri("/api/person/abc/").exchange().expectStatus().isNotFound();
        client.get().uri("/api/person/abc").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/person/abc").header("X-Run-Id", "r1").header("X-Plan-Id", "bogus")
                .header("X-Session-Id", "s").header("X-Request-Id", "q").exchange().expectStatus().isBadRequest();
    }

    @Test
    @Order(4)
    void adminRequiresTheBearerToken() {
        client.get().uri("/admin/status").exchange().expectStatus().isUnauthorized();
        client.get().uri("/admin/status").header(AUTHORIZATION, "Bearer wrong")
                .exchange().expectStatus().isUnauthorized();
        client.get().uri("/internal/stats").exchange().expectStatus().isUnauthorized();
    }

    @Test
    @Order(5)
    void architectureStateHasNoMutationEndpoints() throws Exception {
        JsonNode before = admin("/admin/status");
        for (String path : List.of("/admin/cutover", "/admin/mode", "/admin/architectures/monolith")) {
            client.post().uri(path).header(AUTHORIZATION, "Bearer " + TOKEN)
                    .contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isNotFound();
        }
        client.post().uri("/admin/runtime").header(AUTHORIZATION, "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isEqualTo(405);
        JsonNode after = admin("/admin/status");
        assertEquals(before.path("active"), after.path("active"));
        assertEquals(before.path("epoch"), after.path("epoch"));
        assertEquals(4, admin("/admin/architectures").path("architectures").size());
    }

    @Test
    @Order(6)
    void statsAndLogReflectTheTraffic() throws Exception {
        JsonNode stats = body(client.get().uri("/internal/stats?window_s=60")
                .header(AUTHORIZATION, "Bearer " + TOKEN)
                .exchange().expectStatus().isOk());
        assertTrue(stats.get("n").asInt() >= 2, stats.toString());
        assertTrue(stats.get("p99_ms").asDouble() >= 0);
        assertTrue(stats.get("valid").asBoolean());

        JsonNode status = body(client.get().uri("/admin/status")
                .header(AUTHORIZATION, "Bearer " + TOKEN)
                .exchange().expectStatus().isOk());
        assertEquals(0, status.get("log_rows_dropped").asInt());
        Path file = DIR.resolve("logs").resolve(status.get("log_file").asText());

        List<String> lines = List.of();
        for (int i = 0; i < 40; i++) {
            lines = readAllLines(file, UTF_8);
            if (lines.size() >= 3) {
                break;
            }
            sleep(100);
        }
        assertTrue(lines.size() >= 3, "log rows: " + lines.size());
        long previousMax = 0;
        for (String line : lines) {
            JsonNode row = MAPPER.readTree(line);
            assertEquals(status.get("instance_id").asText(), row.get("instance_id").asText());
            previousMax = max(previousMax, row.get("seq").asLong());
        }
        assertTrue(previousMax >= lines.size());
    }

    @Test
    @Order(7)
    void platformEndpointsAreProxiedWithoutSessionIdentityAndOutsideTheStatistics() throws IOException {
        JsonNode before = body(client.get().uri("/internal/stats?window_s=60")
                .header(AUTHORIZATION, "Bearer " + TOKEN).exchange().expectStatus().isOk());
        for (String path : List.of("/actuator/health", "/actuator/health/db", "/actuator/prometheus",
                "/actuator/metrics/http.server.requests?tag=uri:%2Fapi", "/v3/api-docs", "/swagger-ui/index.html")) {
            JsonNode json = body(client.get().uri(java.net.URI.create(path)).exchange().expectStatus().isOk());
            assertEquals(path, json.get("target").asText());
        }
        JsonNode after = body(client.get().uri("/internal/stats?window_s=60")
                .header(AUTHORIZATION, "Bearer " + TOKEN).exchange().expectStatus().isOk());
        assertEquals(before.get("n").asInt(), after.get("n").asInt());
        client.get().uri("/gateway/actuator/health").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    @Order(8)
    void splitSnapshotCreateIsRoutedToTheSnapshotService() throws IOException {
        JsonNode json = body(client.post().uri("/api/snapshot/person/abc")
                .header("X-Probe", "1").header("X-Probe-Token", TOKEN).header("X-Probe-Target", "microservices")
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix")
                .header("X-Session-Id", "s-snap").header("X-Request-Id", "q-snap")
                .exchange().expectStatus().isOk());
        assertEquals("/api/snapshot/person/abc", json.get("target").asText());
        assertEquals("ms-snapshot", json.get("backend").asText());
    }

    @Test
    @Order(9)
    void catalogueIsListedWithTargetsPerRealisation() throws IOException {
        client.get().uri("/admin/endpoints").exchange().expectStatus().isUnauthorized();
        JsonNode json = body(client.get().uri("/admin/endpoints")
                .header(AUTHORIZATION, "Bearer " + TOKEN).exchange().expectStatus().isOk());
        assertEquals(json.get("endpoints").size(), json.get("count").asInt());
        JsonNode create = null;
        for (JsonNode e : json.get("endpoints")) {
            if (e.get("operation").asText().equals("snapshot_create")) {
                create = e;
            }
        }
        assertTrue(create != null && create.get("targets").has("microservices"));
        assertTrue(create.get("targets").get("microservices").asText().contains(String.valueOf(port(MS_SNAPSHOT))));
    }

    @Test
    @Order(10)
    void probeHeadersNeedTheGatewaySecret() throws IOException {
        client.get().uri("/api/person/p1").header("X-Probe", "1")
                .exchange().expectStatus().isUnauthorized();
        client.get().uri("/api/person/p1").header("X-Probe", "1").header("X-Probe-Token", "wrong")
                .exchange().expectStatus().isUnauthorized();
        client.get().uri("/actuator/health").header("X-Probe-Target", "monolith")
                .exchange().expectStatus().isUnauthorized();
        JsonNode json = body(client.get().uri("/api/person/p1").header("X-Probe", "1")
                .header("X-Probe-Token", TOKEN).header("X-Probe-Target", "monolith")
                .exchange().expectStatus().isOk());
        assertEquals("monolith", json.get("backend").asText());
        client.get().uri("/api/person/p1").header("X-Probe", "1").header("X-Probe-Token", TOKEN)
                .header("X-Probe-Target", "nowhere").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/person/p1").header("X-Probe-Token", TOKEN).header("X-Probe-Target", "monolith")
                .exchange().expectStatus().isBadRequest();
    }

    @Test
    @Order(12)
    void admissionLimitQueuesThenRefusesWithGatewayErrors() throws Exception {
        ExecutorService pool = newFixedThreadPool(5);
        List<Future<String>> results = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int n = i;
            results.add(pool.submit(() -> {
                var r = get("/api/person/slow", "s-adm-" + n).exchange().expectBody().returnResult();
                String error = r.getResponseHeaders().getFirst("X-Gateway-Error");
                return r.getStatus().value() + ":" + (error == null ? "-" : error);
            }));
            sleep(20);
        }
        List<String> seen = new ArrayList<>();
        for (Future<String> f : results) {
            seen.add(f.get());
        }
        pool.shutdown();
        assertEquals(2, seen.stream().filter("200:-"::equals).count(), seen.toString());
        assertEquals(1, seen.stream().filter("503:admission_timeout"::equals).count(), seen.toString());
        assertEquals(2, seen.stream().filter("503:admission_full"::equals).count(), seen.toString());

        String scrape = new String(client.get().uri("/gateway/actuator/prometheus").exchange()
                .expectStatus().isOk().expectBody().returnResult().getResponseBodyContent(), UTF_8);
        assertTrue(scrape.contains("pdms_dyn_admission_rejected_total"), "rejection counter missing");
        assertTrue(scrape.contains("reason=\"admission_full\""));
        assertTrue(scrape.contains("reason=\"admission_timeout\""));
        boolean idle = false;
        for (int i = 0; i < 50 && !idle; i++) {
            String now = new String(client.get().uri("/gateway/actuator/prometheus").exchange()
                    .expectBody().returnResult().getResponseBodyContent(), UTF_8);
            idle = now.lines().filter(l -> l.startsWith("pdms_dyn_admission_inflight") || l.startsWith("pdms_dyn_admission_queued"))
                    .allMatch(l -> l.endsWith(" 0.0"));
            if (!idle) {
                sleep(100);
            }
        }
        assertTrue(idle, "admission counters did not return to zero");
    }

    @Test
    @Order(13)
    void gatewayFailuresAreMappedAndLogged() throws Exception {
        var refused = client.get().uri("/api/person/x").header("X-Probe", "1").header("X-Probe-Token", TOKEN)
                .header("X-Probe-Target", "dead").header("X-Request-Id", "f-refused").exchange();
        refused.expectStatus().isEqualTo(503).expectHeader().valueEquals("X-Gateway-Error", "upstream_unreachable");
        client.get().uri("/api/person/x").header("X-Probe", "1").header("X-Probe-Token", TOKEN)
                .header("X-Probe-Target", "resetter").header("X-Request-Id", "f-reset").exchange()
                .expectStatus().isEqualTo(502).expectHeader().valueEquals("X-Gateway-Error", "upstream_reset");
        client.get().uri("/api/person/hang").header("X-Probe", "1").header("X-Probe-Token", TOKEN)
                .header("X-Probe-Target", "monolith").header("X-Request-Id", "f-timeout").exchange()
                .expectStatus().isEqualTo(504).expectHeader().valueEquals("X-Gateway-Error", "upstream_timeout");

        JsonNode status = admin("/admin/status");
        Path file = DIR.resolve("logs").resolve(status.get("log_file").asText());
        JsonNode reset = null;
        JsonNode timeout = null;
        JsonNode unreachable = null;
        for (int i = 0; i < 40 && (reset == null || timeout == null || unreachable == null); i++) {
            for (String line : readAllLines(file, UTF_8)) {
                JsonNode row = MAPPER.readTree(line);
                String id = row.path("request_id").asText();
                if (id.equals("f-reset")) {
                    reset = row;
                } else if (id.equals("f-timeout")) {
                    timeout = row;
                } else if (id.equals("f-refused")) {
                    unreachable = row;
                }
            }
            if (reset == null || timeout == null || unreachable == null) {
                sleep(100);
            }
        }
        assertEquals("upstream_unreachable", unreachable.get("error_class").asText());
        assertEquals(503, unreachable.get("status").asInt());
        assertEquals("gateway", unreachable.get("error_origin").asText());
        assertEquals("upstream_reset", reset.get("error_class").asText());
        assertEquals(502, reset.get("status").asInt());
        assertEquals("upstream_timeout", timeout.get("error_class").asText());
        assertEquals(504, timeout.get("status").asInt());
        assertTrue(timeout.get("upstream_headers_ms").isNumber(), "upstream_headers_ms is set on failures too");
        assertTrue(timeout.get("gateway_duration_ms").asDouble() >= 900, timeout.toString());
    }

    @Test
    @Order(14)
    void forwardingHeaderPolicyAndLocalPaths() throws Exception {
        // Use a real socket: in-memory WebTestClient has no client address.
        java.net.http.HttpRequest request = java.net.http.HttpRequest
                .newBuilder(java.net.URI.create("http://localhost:" + serverPort + "/api/person/hp"))
                .header("X-Run-Id", "r1").header("X-Plan-Id", "mix").header("X-Session-Id", "s-headers")
                .header("X-Request-Id", "hp-1").header("X-Real-IP", "spoofed.invalid").header("Forwarded", "for=spoofed.invalid")
                .header("X-Forwarded-For", "spoofed.invalid").header("X-Forwarded-Proto", "gopher")
                .header("True-Client-IP", "spoofed.invalid").GET().build();
        java.net.http.HttpResponse<String> reply = newHttpClient()
                .send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals(200, reply.statusCode());
        JsonNode json = MAPPER.readTree(reply.body());
        assertTrue(json.get("realip").isNull(), json.toString());
        assertTrue(json.get("forwarded").isNull(), json.toString());
        assertTrue(json.get("xff").isTextual(), "the gateway sets the client address: " + json);
        assertFalse(json.get("xff").asText().contains("spoofed.invalid"), json.toString());
        assertTrue(json.get("xff").asText().matches("127\\.0\\.0\\.1|0:0:0:0:0:0:0:1"), json.toString());
        assertEquals("http", json.get("xproto").asText(), json.toString());
        client.get().uri("/admin/nothing").header(AUTHORIZATION, "Bearer " + TOKEN)
                .exchange().expectStatus().isNotFound();
        client.get().uri("/internal/nothing").exchange().expectStatus().isNotFound();
        client.get().uri("/gateway/nothing").exchange().expectStatus().isNotFound();
        client.get().uri(java.net.URI.create("/swagger-ui/../api/person/abc")).exchange().expectStatus().isNotFound();
    }

    @Test
    @Order(15)
    void runIdIsCheckedRejectionsKeepTheirIdsAndTheLogStartsWithProvenance() throws Exception {
        client.get().uri("/api/person/rid").header("X-Run-Id", "other-run").header("X-Plan-Id", "mix")
                .header("X-Session-Id", "s-rid").header("X-Request-Id", "rid-1").exchange()
                .expectStatus().isEqualTo(409).expectHeader().valueEquals("X-Gateway-Reject", "run_id_mismatch");
        client.get().uri("/api/person/rid").header("X-Run-Id", "r1").header("X-Plan-Id", "bogus")
                .header("X-Session-Id", "s-rid2").header("X-Request-Id", "rid-2").exchange().expectStatus().isBadRequest();

        JsonNode status = admin("/admin/status");
        assertTrue(status.has("provenance") && status.get("provenance").has("revision"), status.toString());
        Path file = DIR.resolve("logs").resolve(status.get("log_file").asText());
        JsonNode mismatch = null;
        JsonNode invalid = null;
        JsonNode start = null;
        for (int i = 0; i < 40 && (mismatch == null || invalid == null); i++) {
            for (String line : readAllLines(file, UTF_8)) {
                JsonNode row = MAPPER.readTree(line);
                if (row.path("event").asText().equals("run_start")) {
                    start = row;
                }
                String id = row.path("request_id").asText();
                if (id.equals("rid-1")) {
                    mismatch = row;
                } else if (id.equals("rid-2")) {
                    invalid = row;
                }
            }
            if (mismatch == null || invalid == null) {
                sleep(100);
            }
        }
        assertEquals("run_id_mismatch", mismatch.get("error_class").asText());
        assertEquals("other-run", mismatch.get("client_run_id").asText());
        assertEquals("s-rid", mismatch.get("session_id").asText());
        assertEquals("missing_or_invalid_identity", invalid.get("error_class").asText());
        assertEquals("s-rid2", invalid.get("session_id").asText());
        assertEquals("bogus", invalid.get("plan_id").asText());
        assertTrue(start != null && start.has("jvm_input_arguments") && start.has("config_hash")
                && start.has("admission_max_in_flight") && start.has("java_version"), String.valueOf(start));
    }

    @Test
    @Order(17)
    void diagnosticCountersAttributeErrorsAndHeaderTimerExcludesFailedExchanges() throws Exception {
        double rejected = diagnosticErrorCount("none", "gateway_reject", "gateway", "unknown_path", false);
        client.get().uri("/api/diagnostic-unknown").exchange().expectStatus().isNotFound();
        awaitDiagnostic(() -> diagnosticErrorCount("none", "gateway_reject", "gateway", "unknown_path", false) == rejected + 1);

        long before = diagnosticHeaderCount("monolith");
        client.get().uri("/api/person/diagnostic-ok").header("X-Probe", "1")
                .header("X-Probe-Token", TOKEN).header("X-Probe-Target", "monolith")
                .exchange().expectStatus().isOk();
        awaitDiagnostic(() -> diagnosticHeaderCount("monolith") == before + 1);

        long headers = diagnosticHeaderCount("monolith");
        double timeouts = diagnosticErrorCount("monolith", "person_read", "gateway", "upstream_timeout", true);
        client.get().uri("/api/person/hang").header("X-Probe", "1")
                .header("X-Probe-Token", TOKEN).header("X-Probe-Target", "monolith")
                .exchange().expectStatus().isEqualTo(504);
        awaitDiagnostic(() -> diagnosticErrorCount("monolith", "person_read", "gateway", "upstream_timeout", true) == timeouts + 1);
        assertEquals(headers, diagnosticHeaderCount("monolith"), "timeout must not be recorded as response headers");

        double refused = diagnosticErrorCount("dead", "person_read", "gateway", "upstream_unreachable", true);
        client.get().uri("/api/person/diagnostic-refused").header("X-Probe", "1")
                .header("X-Probe-Token", TOKEN).header("X-Probe-Target", "dead")
                .exchange().expectStatus().isEqualTo(503);
        awaitDiagnostic(() -> diagnosticErrorCount("dead", "person_read", "gateway", "upstream_unreachable", true) == refused + 1);
        assertEquals(0, diagnosticHeaderCount("dead"));
        assertEquals(1, registry.get("pdms_dyn_log_writer_healthy").gauge().value());
        assertEquals(65536, registry.get("pdms_dyn_log_queue_capacity").gauge().value());
        assertTrue(registry.find("pdms_dyn_event_loop_pending_tasks").gauges().stream().allMatch(g -> g.value() >= 0));
        assertFalse(registry.find("pdms_dyn_event_loop_pending_tasks").gauges().isEmpty());
    }

    @Test
    @Order(18)
    void selectionCannotEnableApplicationWithoutStartupPermission() throws Exception {
        JsonNode selection = admin("/admin/selection");
        assertFalse(selection.path("allowApplyAtStartup").asBoolean());
        var configuration = (com.fasterxml.jackson.databind.node.ObjectNode)selection.path("configuration").deepCopy();
        configuration.put("enabled", true).put("applyChanges", true);
        var body = MAPPER.createObjectNode().put("requestId", randomUUID().toString())
                .put("expectedVersion", selection.path("version").asLong());
        body.set("configuration", configuration);
        client.put().uri("/admin/selection").header(AUTHORIZATION, "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(body.toString()).exchange().expectStatus().isEqualTo(409);
        assertFalse(admin("/admin/selection").path("configuration").path("enabled").asBoolean());
    }

    private double diagnosticErrorCount(String target, String operation, String origin, String reason, boolean probe) {
        var counter = registry.find("pdms_dyn_gateway_errors").tags("realisation", target,
                "operation", operation, "origin", origin, "reason", reason, "probe", String.valueOf(probe)).counter();
        return counter == null ? 0 : counter.count();
    }

    private long diagnosticHeaderCount(String target) {
        var timer = registry.find("pdms_dyn_upstream_headers_duration").tags("realisation", target,
                "operation", "person_read", "probe", "true").timer();
        return timer == null ? 0 : timer.count();
    }

    private static void awaitDiagnostic(java.util.function.BooleanSupplier condition) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "diagnostic metric did not settle");
    }

    private static HttpServer mock(String name) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            String target = exchange.getRequestURI().getRawPath()
                    + (exchange.getRequestURI().getRawQuery() != null ? "?" + exchange.getRequestURI().getRawQuery() : "");
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (target.startsWith("/api/person/slow")) {
                try {
                    sleep(400);
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }
            if (target.startsWith("/api/person/hang")) {
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }
            var h = exchange.getRequestHeaders();
            String json = "{\"status\":\"UP\",\"backend\":\"" + name + "\",\"target\":\"" + target
                    + "\",\"authorization\":" + (auth == null ? "null" : "\"" + auth + "\"")
                    + ",\"xff\":" + quote(h.getFirst("X-Forwarded-For"))
                    + ",\"realip\":" + quote(h.getFirst("X-Real-IP"))
                    + ",\"forwarded\":" + quote(h.getFirst("Forwarded"))
                    + ",\"xproto\":" + quote(h.getFirst("X-Forwarded-Proto")) + "}";
            byte[] bytes = json.getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            boolean unhealthy = target.equals("/actuator/health") && DOWN.contains(name);
            exchange.sendResponseHeaders(unhealthy ? 503 : 200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.setExecutor(newCachedThreadPool());
        server.start();
        return server;
    }

    private static String quote(String value) {
        return value == null ? "null" : "\"" + value + "\"";
    }

    private static ServerSocket startResetter() throws IOException {
        ServerSocket server = new ServerSocket(0);
        Thread t = new Thread(() -> {
            while (!server.isClosed()) {
                try (Socket c = server.accept()) {
                    c.setSoTimeout(2_000);
                    try {
                        // Wait for the HTTP request so the client has completed the TCP connect.
                        if (c.getInputStream().read() != -1) {
                            c.setSoLinger(true, 0); // Close with RST.
                        }
                    } catch (SocketTimeoutException ignored) {
                        // Let an idle connection close without stopping the test server.
                    }
                } catch (IOException e) {
                    return;
                }
            }
        }, "test-resetter");
        t.setDaemon(true);
        t.start();
        return server;
    }

    private static int port(HttpServer server) {
        return server.getAddress().getPort();
    }
}
