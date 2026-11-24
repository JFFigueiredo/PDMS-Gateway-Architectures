package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readString;
import static java.nio.file.Files.writeString;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.MediaType.APPLICATION_JSON;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = RANDOM_PORT,
        properties = {"spring.flyway.enabled=false", "pdms.gateway.runtime.managed=false"})
@AutoConfigureWebTestClient
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SelectionAdminIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "selection-test-only";
    private static final Path DIR;
    private static final HttpServer BACKEND;
    static {
        try {
            DIR = createTempDirectory("pdms-selection-test");
            BACKEND = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            BACKEND.createContext("/", e -> {
                byte[] value = "{\"status\":\"UP\"}".getBytes();
                e.sendResponseHeaders(200, value.length);
                try(var out=e.getResponseBody()){
                out.write(value);
            }
            });
            BACKEND.start();
            ObjectNode routing = JSON.createObjectNode().put("version", "routing/1").put("epoch", 0).put("active", "monolith").put("mode", "request");
            ObjectNode targets = routing.putObject("backends");
            for (String name : ARCHITECTURES) {
                targets.putObject(name).put("default", "http://localhost:" + BACKEND.getAddress().getPort());
            }
            writeString(DIR.resolve("routing.json"), routing.toString());
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("pdms.gateway.routing-file", () -> DIR.resolve("routing.json").toString());
        r.add("pdms.gateway.log-dir", () -> DIR.resolve("logs").toString());
        r.add("pdms.gateway.admin-token", () -> TOKEN);
        r.add("pdms.gateway.health-interval", () -> "100ms");
        r.add("pdms.gateway.selection.allow-apply", () -> "true");
    }
    @Autowired WebTestClient client;
    @Autowired ipl.pdms.gateway.selection.SelectionJournal journal;
    private JsonNode get(String path) throws Exception {
        return JSON.readTree(client.get().uri(path).header("Authorization", "Bearer " + TOKEN).exchange()
                .expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
    }
    private ObjectNode envelope() throws Exception {
        return JSON.createObjectNode().put("requestId", randomUUID().toString()).put("expectedVersion", get("/admin/selection").path("version").asLong());
    }
    private WebTestClient.ResponseSpec put(String path, ObjectNode body) {
        return client.put().uri(path).header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON).bodyValue(body.toString()).exchange();
    }
    private JsonNode scenario(String name) throws Exception {
        return JSON.readTree(client.post().uri("/admin/selection/scenarios/" + name).header("Authorization", "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(envelope().toString()).exchange().expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
    }
    private ObjectNode evaluation() throws Exception {
        return envelope().put("expectedEpoch", get("/admin/status").path("epoch").asLong());
    }
    private WebTestClient.ResponseSpec evaluate(ObjectNode body) {
        return client.post().uri("/admin/selection/evaluate").header("Authorization", "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(body.toString()).exchange();
    }
    @Test @Order(1) void authenticationAndDisabledDefaults() throws Exception {
        client.get().uri("/admin/selection").exchange().expectStatus().isUnauthorized();
        client.post().uri("/admin/selection/evaluate").contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isUnauthorized();
        JsonNode status = get("/admin/selection");
        assertFalse(status.path("configuration").path("enabled").asBoolean());
        assertFalse(status.path("configuration").path("applyChanges").asBoolean());
    }
    @Test @Order(2) void scenarioReplayAndDryRunNeverMutateRouting() throws Exception {
        ObjectNode change = envelope();
        String path = "/admin/selection/scenarios/anchoring-required";
        byte[] first = client.post().uri(path).header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON)
                .bodyValue(change.toString()).exchange().expectStatus().isOk().expectBody().returnResult().getResponseBodyContent();
        byte[] replay = client.post().uri(path).header("Authorization", "Bearer " + TOKEN).contentType(APPLICATION_JSON)
                .bodyValue(change.toString()).exchange().expectStatus().isOk().expectBody().returnResult().getResponseBodyContent();
        assertEquals(JSON.readTree(first), JSON.readTree(replay));
        ObjectNode request = evaluation();
        JsonNode result = JSON.readTree(evaluate(request).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("decentralized", result.path("target").asText()); assertFalse(result.path("applied").asBoolean());
        assertEquals("aggregate_fixture", result.path("evidenceKind").asText());
        assertEquals(result, JSON.readTree(evaluate(request).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent()));
        assertEquals("monolith", get("/admin/status").path("active").asText());
    }
    @Test @Order(3) void invalidAndStaleRequestsFailWithoutChangingConfiguration() throws Exception {
        ObjectNode request = envelope().put("expectedEpoch", 999);
        evaluate(request).expectStatus().isEqualTo(409);
        ObjectNode update = envelope().put("expectedVersion", 999);
        update.set("configuration", get("/admin/selection").path("configuration"));
        put("/admin/selection", update).expectStatus().isEqualTo(409);
        update = envelope(); ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("madeUpField", true); update.set("configuration", config);
        put("/admin/selection", update).expectStatus().isBadRequest();
        ObjectNode malformed = envelope().put("expectedVersion", 1.5); malformed.set("configuration", config);
        put("/admin/selection", malformed).expectStatus().isBadRequest();
    }
    @Test @Order(4) void refreshingMockSignalsPreservesDebounce() throws Exception {
        scenario("read-crossover");
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        ((ObjectNode)config.path("tuning")).put("tickMs", 100);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update).expectStatus().isOk();
        assertEquals("debounce", JSON.readTree(evaluate(evaluation()).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent()).path("reason").asText());
        sleep(110);
        ObjectNode mock = (ObjectNode)get("/admin/selection").path("configuration").path("mock").deepCopy();
        mock.put("sampleTimestampEpochMs", currentTimeMillis());
        update = envelope(); update.set("mock", mock); put("/admin/selection/mock", update).expectStatus().isOk();
        JsonNode result = JSON.readTree(evaluate(evaluation()).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("faas", result.path("target").asText()); assertEquals("switch", result.path("action").asText());
    }
    @Test @Order(5) void explicitEvaluationCannotApplyAChangeEvenWhenApplicationIsEnabled() throws Exception {
        scenario("read-crossover");
        for (int i = 0; i < 50 && !get("/admin/health").path("realisations").path("faas").path("status").asText().equals("UP"); i++) {
            sleep(50);
        }
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("applyChanges", true); ((ObjectNode)config.path("tuning")).put("breachTicks", 1);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update).expectStatus().isOk();
        ObjectNode request = evaluation();
        JsonNode result = JSON.readTree(evaluate(request).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertFalse(result.path("applied").asBoolean(), result.toString());
        assertFalse(result.path("applyRequested").asBoolean());
        assertEquals("monolith", get("/admin/status").path("active").asText());
        assertEquals(result, JSON.readTree(evaluate(request).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent()));
        assertEquals(0, get("/admin/status").path("epoch").asLong());
        Path journal = DIR.resolve("logs").resolve(get("/admin/selection").path("journalFile").asText());
        String records = readString(journal);
        assertTrue(records.contains("selection_decision")); assertFalse(records.contains("selection_application_outcome"));
    }
    @Test @Order(6) void scheduledApplicationCanBeArmedButWaitsForFreeze() throws Exception {
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("autoEvaluate", true); config.put("applyChanges", true);
        ObjectNode update = envelope(); update.set("configuration", config);
        put("/admin/selection", update).expectStatus().isOk();
        long version = get("/admin/selection").path("version").asLong();
        assertEquals(update.path("expectedVersion").asLong() + 1, version);
        assertTrue(get("/admin/selection").path("automaticWaitingForFreeze").asBoolean());
        assertEquals("monolith", get("/admin/status").path("active").asText());
    }
    @Test @Order(6) void realAdapterRequiresTheDeclaredWorkloadContext() throws Exception {
        scenario("read-crossover");
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("source", "real");
        config.path("profiles").forEach(arch -> arch.forEach(p -> ((ObjectNode)p).put("evidenceKind", "measured")));
        ((ObjectNode)config.path("tuning")).put("minSamples", 1);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update).expectStatus().isOk();
        client.get().uri("/api/person/read-context").header("X-Run-Id", "selection-fixture")
                .header("X-Plan-Id", "read").header("X-Session-Id", "selection-read-context").header("X-Request-Id", randomUUID().toString())
                .exchange().expectStatus().isOk();
        JsonNode decision = JSON.readTree(evaluate(evaluation()).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("live_observation", decision.path("evidenceKind").asText());
        assertTrue(decision.path("signals").path("valid").asBoolean(), decision.toString());
        config.put("workload", "write"); update = envelope(); update.set("configuration", config); put("/admin/selection", update).expectStatus().isOk();
        decision = JSON.readTree(evaluate(evaluation()).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("stale_or_invalid_signal", decision.path("reason").asText());
        assertFalse(decision.path("signals").path("valid").asBoolean());
    }
    @Test @Order(7) void scheduledDryRunProducesDecisionsWithoutAnotherHttpEvaluation() throws Exception {
        scenario("tie");
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("autoEvaluate", true);
        ObjectNode update = envelope(); update.set("configuration", config);
        put("/admin/selection", update).expectStatus().isOk();
        JsonNode last = get("/admin/selection").path("lastDecision");
        for (int i = 0; i < 50 && last.isEmpty(); i++) {
            sleep(50);
            last = get("/admin/selection").path("lastDecision");
        }
        assertTrue(last.path("requestId").asText().startsWith("scheduled-"), last.toString());
        assertFalse(last.path("applied").asBoolean());
        assertEquals("monolith", get("/admin/status").path("active").asText());
        client.post().uri("/admin/selection/reset").header("Authorization", "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(envelope().toString()).exchange().expectStatus().isOk();
        assertFalse(get("/admin/selection").path("configuration").path("enabled").asBoolean());
    }
    @Test @Order(8) void unavailableJournalPreventsAnOtherwiseEligibleLiveCutover() throws Exception {
        scenario("outage");
        JsonNode before = get("/admin/status");
        JsonNode preview = JSON.readTree(evaluate(evaluation()).expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
        assertEquals("switch", preview.path("action").asText());
        assertNotEquals(before.path("active").asText(), preview.path("target").asText());
        ObjectNode config = (ObjectNode)get("/admin/selection").path("configuration").deepCopy();
        config.put("applyChanges", true);
        ObjectNode update = envelope(); update.set("configuration", config); put("/admin/selection", update).expectStatus().isOk();
        journal.close();
        JsonNode failure = JSON.readTree(evaluate(evaluation()).expectStatus().isEqualTo(503)
                .expectBody().returnResult().getResponseBodyContent());
        assertEquals("decision_journal_unavailable", failure.path("error").asText());
        JsonNode after = get("/admin/status");
        assertEquals(before.path("active"), after.path("active"));
        assertEquals(before.path("epoch"), after.path("epoch"));
        assertFalse(get("/admin/selection").path("journalHealthy").asBoolean());
        ObjectNode captured = JSON.createObjectNode().put("event", "t8_decision_write_failure");
        captured.set("before", before); captured.set("preview", preview); captured.set("failure", failure); captured.set("after", after);
        writeString(DIR.resolve("logs/t8-decision-failure.json"), captured.toString());
    }
}
