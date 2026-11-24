package ipl.pdms.gateway;

import static ipl.pdms.gateway.routing.Endpoint.Kind.API;
import static ipl.pdms.gateway.routing.PdmsEndpoints.all;
import static ipl.pdms.gateway.routing.RouteMatcher.decide;
import static ipl.pdms.gateway.routing.Service.CONSENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ipl.pdms.gateway.routing.Endpoint;
import ipl.pdms.gateway.routing.RouteMatcher;
import ipl.pdms.gateway.routing.Service;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class RouteMatcherTest {

    @ParameterizedTest
    @CsvSource({
            "POST,/api/person,PERSON,person_create",
            "GET,/api/person/11111111-1111-1111-1111-111111111111,PERSON,person_read",
            "PUT,/api/person/abc,PERSON,person_update",
            "DELETE,/api/person/abc,PERSON,person_delete",
            "GET,/api/person/health,PERSON,person_health",
            "POST,/api/mapper/person,PERSON,person_create",
            "GET,/api/mapper/person/abc,PERSON,person_read",
            "PUT,/api/mapper/person/abc,PERSON,person_update",
            "DELETE,/api/mapper/person/abc,PERSON,person_delete",
            "GET,/api/mapper/person/health,PERSON,person_health",
            "POST,/api/person/abc/consents,CONSENT,consent_create",
            "GET,/api/person/abc/consents,CONSENT,consent_list",
            "GET,/api/person/abc/consents/c1,CONSENT,consent_read",
            "PUT,/api/person/abc/consents/c1,CONSENT,consent_update",
            "DELETE,/api/person/abc/consents/c1,CONSENT,consent_delete",
            "POST,/api/person/abc/consents/c1/withdraw,CONSENT,consent_withdraw",
            "GET,/api/snapshot/person/abc/latest,SNAPSHOT,snapshot_latest",
            "GET,/api/snapshot/by-email/a%40b.example,SNAPSHOT,snapshot_by_email",
            "GET,/api/snapshot/by-phone/%2B351912345678,SNAPSHOT,snapshot_by_phone",
            "POST,/api/snapshot/person/abc,SNAPSHOT,snapshot_create"})
    void routesBusinessOperations(String method, String path, Service service, String operation) {
        RouteMatcher.Decision d = decide(method, path);
        assertTrue(d.routed(), path);
        assertEquals(service, d.match().service());
        assertEquals(operation, d.match().operation());
        assertEquals(API, d.match().kind());
    }

    @ParameterizedTest
    @CsvSource({
            "/actuator,actuator_index",
            "/actuator/health,actuator_health",
            "/actuator/health/liveness,actuator_health_component",
            "/actuator/health/db/details,actuator_health_component",
            "/actuator/info,actuator_info",
            "/actuator/metrics,actuator_metrics",
            "/actuator/metrics/http.server.requests,actuator_metric",
            "/actuator/prometheus,actuator_prometheus",
            "/v3/api-docs,openapi_docs",
            "/v3/api-docs.yaml,openapi_docs_yaml",
            "/v3/api-docs/swagger-config,openapi_docs_group",
            "/swagger-ui.html,swagger_ui_page",
            "/swagger-ui/index.html,swagger_ui_asset",
            "/swagger-ui/swagger-ui-bundle.js,swagger_ui_asset"})
    void routesPlatformEndpoints(String path, String operation) {
        RouteMatcher.Decision d = decide("GET", path);
        assertTrue(d.routed(), path);
        assertEquals(Service.PLATFORM, d.match().service());
        assertEquals(operation, d.match().operation());
        assertEquals(Endpoint.Kind.PLATFORM, d.match().kind());
    }

    @Test
    void literalHealthWinsOverThePersonIdTemplateOnGetOnly() {
        assertEquals("person_health", decide("GET", "/api/person/health").match().operation());
        assertEquals("person_update", decide("PUT", "/api/person/health").match().operation());
    }

    @Test
    void malformedIdStillReachesConsentOwner() {
        RouteMatcher.Decision d = decide("POST", "/api/person/not-a-uuid/consents");
        assertTrue(d.routed());
        assertEquals(CONSENT, d.match().service());
    }

    @Test
    void wrongMethodOnKnownPathIs405WithAllowList() {
        RouteMatcher.Decision d = decide("DELETE", "/api/snapshot/person/abc/latest");
        assertEquals(405, d.status());
        assertEquals(List.of("GET"), d.allow());
        RouteMatcher.Decision person = decide("POST", "/api/person/abc");
        assertEquals(405, person.status());
        assertEquals(List.of("GET", "PUT", "DELETE"), person.allow());
        RouteMatcher.Decision snapshot = decide("GET", "/api/snapshot/person/abc");
        assertEquals(405, snapshot.status());
        assertEquals(List.of("POST"), snapshot.allow());
        assertEquals(405, decide("POST", "/actuator/health").status());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/api/person/", "/api/person//abc", "/api/person/abc/consentsX", "/api/person/abc/consents/c1/other",
            "/api/mapper/person/abc/consents", "/api/snapshot/", "/api/unknown", "/", "/actuator/",
            "/actuator/health/", "/actuator/env", "/actuator/shutdown", "/actuator/metrics/a/b",
            "/v3/api-docs/", "/swagger-ui", "/swagger-ui/", "/api/person/abc/consents/c1/withdraw/x",
            "/gateway/actuator/health", "/admin/status",
            "/api/person/../person/abc", "/swagger-ui/../api/person/abc", "/api/person/%2e%2e/abc",
            "/api/person/%2E/abc", "/api/person/.%2e/abc", "/api/./person/abc",
            "/actuator/health/x/..;/..;/env", "/swagger-ui/..;/admin/x", "/api/person/..;", "/api/person/abc;jsessionid=1"})
    void everythingElseIsUnknown(String path) {
        RouteMatcher.Decision d = decide("GET", path);
        assertFalse(d.routed(), path);
        assertEquals(404, d.status(), path);
    }

    @Test
    void headAndOptionsAreNotRouted() {
        assertFalse(decide("HEAD", "/api/person/abc").routed());
        assertFalse(decide("OPTIONS", "/api/person/abc").routed());
    }

    @Test
    void catalogueHasNoDuplicateOperationsAndExpectedSize() {
        Set<String> seen = new HashSet<>();
        long api = 0;
        for (Endpoint e : all()) {
            assertTrue(seen.add(e.method() + " " + e.template()), "duplicate " + e);
            api += e.kind() == API ? 1 : 0;
        }
        assertEquals(20, api);
        assertEquals(32, all().size());
    }
}
