package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.writeString;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = RANDOM_PORT,
        properties = {"spring.flyway.enabled=false", "pdms.gateway.runtime.managed=false", "pdms.gateway.run-id=freeze-test"})
@AutoConfigureWebTestClient
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
class SelectionFreezeIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "freeze-test-only";
    private static final Path DIR;
    static {
        try {
            DIR = createTempDirectory("pdms-selection-freeze-test");
            ObjectNode routes = JSON.createObjectNode().put("version", "routing/1").put("epoch", 0)
                    .put("active", "monolith").put("mode", "request");
            ObjectNode backends = routes.putObject("backends");
            for (String name : ARCHITECTURES) {
                backends.putObject(name).put("default", "http://localhost:9");
            }
            writeString(DIR.resolve("routing.json"), routes.toString());
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("pdms.gateway.routing-file", () -> DIR.resolve("routing.json").toString());
        r.add("pdms.gateway.log-dir", () -> DIR.resolve("logs").toString());
        r.add("pdms.gateway.admin-token", () -> TOKEN);
    }
    @Autowired WebTestClient client;
    @Autowired ipl.pdms.gateway.selection.SelectionJournal journal;

    private JsonNode json(WebTestClient.ResponseSpec response) throws Exception {
        return JSON.readTree(response.expectStatus().isOk().expectBody().returnResult().getResponseBodyContent());
    }
    private JsonNode get(String path) throws Exception {
        return json(client.get().uri(path).header("Authorization", "Bearer " + TOKEN).exchange());
    }
    private ObjectNode envelope() throws Exception {
        return JSON.createObjectNode().put("requestId", randomUUID().toString())
                .put("expectedVersion", get("/admin/selection").path("version").asLong());
    }
    private WebTestClient.ResponseSpec post(String path, ObjectNode body) {
        return client.post().uri(path).header("Authorization", "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(body.toString()).exchange();
    }
    private WebTestClient.ResponseSpec put(String path, ObjectNode body) {
        return client.put().uri(path).header("Authorization", "Bearer " + TOKEN)
                .contentType(APPLICATION_JSON).bodyValue(body.toString()).exchange();
    }
    private JsonNode load() throws Exception {
        JsonNode loaded = json(post("/admin/selection/scenarios/read-crossover", envelope()));
        ObjectNode config = loaded.path("configuration").deepCopy();
        ((ObjectNode)config.path("tuning")).put("tickMs", 100);
        ObjectNode body = envelope(); body.set("configuration", config);
        return json(put("/admin/selection", body));
    }
    private JsonNode freeze() throws Exception {
        return json(post("/admin/selection/freeze", envelope().put("runId", "freeze-test")));
    }
    private JsonNode evaluate() throws Exception {
        return json(post("/admin/selection/evaluate", envelope().put("expectedEpoch", get("/admin/status").path("epoch").asLong())));
    }

    @Test void freezeIsVersionedIdempotentAndBlocksEveryConfigurationPath() throws Exception {
        assertFalse(get("/admin/selection").path("policyFrozen").asBoolean());
        JsonNode before = load();
        ObjectNode request = envelope().put("runId", "freeze-test");
        JsonNode frozen = json(post("/admin/selection/freeze", request));
        assertTrue(frozen.path("policyFrozen").asBoolean());
        assertEquals(before.path("version").asLong() + 1, frozen.path("version").asLong());
        assertEquals(before.path("policySha256"), frozen.path("policySha256"));
        assertEquals(frozen, json(post("/admin/selection/freeze", request)));
        post("/admin/selection/freeze", envelope().put("runId", "freeze-test")).expectStatus().isEqualTo(409);
        ObjectNode update = envelope(); update.set("configuration", frozen.path("configuration"));
        put("/admin/selection", update).expectStatus().isEqualTo(409).expectBody()
                .jsonPath("$.error").isEqualTo("selection_policy_frozen");
        post("/admin/selection/scenarios/stable", envelope()).expectStatus().isEqualTo(409).expectBody()
                .jsonPath("$.error").isEqualTo("selection_policy_frozen");
        post("/admin/selection/reset", envelope()).expectStatus().isEqualTo(409).expectBody()
                .jsonPath("$.error").isEqualTo("selection_policy_frozen");
        assertEquals(frozen, get("/admin/selection"));
        assertEquals(0, get("/admin/status").path("epoch").asLong());
    }

    @Test void frozenInputsCanRefreshAndChangeContextWithoutChangingPolicyHash() throws Exception {
        load(); JsonNode frozen = freeze(); JsonNode hash = frozen.path("policySha256");
        assertEquals("debounce", evaluate().path("reason").asText());
        sleep(110);
        ObjectNode mock = get("/admin/selection").path("configuration").path("mock").deepCopy();
        mock.put("sampleTimestampEpochMs", currentTimeMillis());
        ObjectNode refresh = envelope(); refresh.set("mock", mock);
        JsonNode refreshed = json(put("/admin/selection/mock", refresh));
        assertEquals(hash, refreshed.path("policySha256"));
        JsonNode decision = evaluate();
        assertEquals("switch", decision.path("action").asText()); assertEquals("faas", decision.path("target").asText());
        assertFalse(decision.path("applied").asBoolean()); assertTrue(decision.path("policyFrozen").asBoolean());
        assertEquals("freeze-test", decision.path("runId").asText()); assertEquals(hash, decision.path("policySha256"));
        ObjectNode context = envelope().put("workload", "write");
        context.putArray("requiredCapabilities").add("consent_integrity_anchor");
        JsonNode changed = json(put("/admin/selection/context", context));
        assertEquals(hash, changed.path("policySha256"));
        assertEquals(frozen.path("configuration").path("tuning"), changed.path("configuration").path("tuning"));
        assertEquals(frozen.path("configuration").path("profiles"), changed.path("configuration").path("profiles"));
        assertEquals("write", changed.path("configuration").path("workload").asText());
        assertEquals(changed, json(put("/admin/selection/context", context)));
        assertEquals("monolith", get("/admin/status").path("active").asText());
        assertEquals(0, get("/admin/status").path("epoch").asLong());
        var records = new ArrayList<JsonNode>();
        for (String line : readAllLines(DIR.resolve("logs").resolve(changed.path("journalFile").asText()))) {
            records.add(JSON.readTree(line));
        }
        JsonNode freezeRecord = records.stream().filter(n -> n.path("event").asText().equals("selection_policy_frozen")).findFirst().orElseThrow();
        assertEquals(hash, freezeRecord.path("policySha256"));
        assertEquals("pdms-selection-policy/1", freezeRecord.path("policyDocument").path("contract").asText());
        for (String event : java.util.List.of("selection_mock_inputs", "selection_context", "selection_decision")) {
            var rows = records.stream().filter(n -> n.path("event").asText().equals(event)).toList(); assertFalse(rows.isEmpty());
            for (JsonNode row : rows) {
                assertEquals(hash, row.path("policySha256")); assertTrue(row.path("policyFrozen").asBoolean());
                assertEquals("freeze-test", row.path("runId").asText());
            }
        }
    }

    @Test void authStaleVersionsRunMismatchAndMalformedContextAreRejected() throws Exception {
        client.post().uri("/admin/selection/freeze").contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isUnauthorized();
        client.put().uri("/admin/selection/context").contentType(APPLICATION_JSON).bodyValue("{}").exchange().expectStatus().isUnauthorized();
        load();
        post("/admin/selection/freeze", envelope().put("expectedVersion", 999).put("runId", "freeze-test"))
                .expectStatus().isEqualTo(409).expectBody().jsonPath("$.error").isEqualTo("stale_selection_version");
        post("/admin/selection/freeze", envelope().put("runId", "another-run"))
                .expectStatus().isEqualTo(409).expectBody().jsonPath("$.error").isEqualTo("selection_run_id_mismatch");
        post("/admin/selection/freeze", envelope().put("runId", 17)).expectStatus().isBadRequest();
        JsonNode frozen = freeze();
        ObjectNode context = envelope().put("workload", "read"); context.putArray("requiredCapabilities").add("unknown");
        put("/admin/selection/context", context).expectStatus().isBadRequest();
        context.putArray("requiredCapabilities").add("consent_integrity_anchor").add("consent_integrity_anchor");
        put("/admin/selection/context", context).expectStatus().isBadRequest();
        context.putArray("requiredCapabilities").add(1);
        put("/admin/selection/context", context).expectStatus().isBadRequest();
        context.putArray("requiredCapabilities"); context.put("workload", "unknown");
        put("/admin/selection/context", context).expectStatus().isBadRequest();
        context.put("workload", "read"); context.put("expectedVersion", 999);
        put("/admin/selection/context", context).expectStatus().isEqualTo(409);
        context.put("expectedVersion", frozen.path("version").asLong()).put("tuning", "forbidden");
        put("/admin/selection/context", context).expectStatus().isBadRequest();
        assertEquals(frozen, get("/admin/selection"));
    }

    @Test void journalFailurePreventsFreezeAndLeavesConfigurationUntouched() throws Exception {
        JsonNode before = load(); journal.close();
        post("/admin/selection/freeze", envelope().put("runId", "freeze-test"))
                .expectStatus().isEqualTo(503).expectBody().jsonPath("$.error").isEqualTo("decision_journal_unavailable");
        JsonNode after = get("/admin/selection");
        assertFalse(after.path("policyFrozen").asBoolean()); assertEquals(before.path("version"), after.path("version"));
        assertEquals(before.path("configuration"), after.path("configuration"));
    }

    @Test void scheduledDryRunCarriesTheFrozenPolicyIdentity() throws Exception {
        JsonNode loaded = load(); ObjectNode config = loaded.path("configuration").deepCopy();
        config.put("autoEvaluate", true); ObjectNode update = envelope(); update.set("configuration", config);
        json(put("/admin/selection", update)); JsonNode frozen = freeze();
        JsonNode last = get("/admin/selection").path("lastDecision");
        for (int i = 0; i < 60 && last.isEmpty(); i++) {
            sleep(50);
            last = get("/admin/selection").path("lastDecision");
        }
        assertTrue(last.path("requestId").asText().startsWith("scheduled-"));
        assertTrue(last.path("policyFrozen").asBoolean()); assertEquals(frozen.path("policySha256"), last.path("policySha256"));
        assertFalse(last.path("applied").asBoolean()); assertEquals(0, get("/admin/status").path("epoch").asLong());
    }
}
