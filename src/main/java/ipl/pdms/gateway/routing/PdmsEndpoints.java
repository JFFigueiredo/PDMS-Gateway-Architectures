package ipl.pdms.gateway.routing;

import static ipl.pdms.gateway.routing.Endpoint.Kind.API;
import static ipl.pdms.gateway.routing.Service.CONSENT;
import static ipl.pdms.gateway.routing.Service.PERSON;
import static ipl.pdms.gateway.routing.Service.SNAPSHOT;

import ipl.pdms.gateway.routing.Endpoint.Kind;
import java.util.ArrayList;
import java.util.List;

/** First match wins; list literal paths before parameter templates. */
public final class PdmsEndpoints {

    private static final List<Endpoint> ALL = build();

    private PdmsEndpoints() {
    }

    public static List<Endpoint> all() {
        return ALL;
    }

    private static List<Endpoint> build() {
        final List<Endpoint> e = new ArrayList<>();
        for (final String prefix : List.of("/api/person", "/api/mapper/person")) {
            final String origin = "PersonController " + prefix;
            e.add(api("GET", prefix + "/health", PERSON, "person_health", origin));
            e.add(api("POST", prefix, PERSON, "person_create", origin));
            e.add(api("GET", prefix + "/{personId}", PERSON, "person_read", origin));
            e.add(api("PUT", prefix + "/{personId}", PERSON, "person_update", origin));
            e.add(api("DELETE", prefix + "/{personId}", PERSON, "person_delete", origin));
        }

        final String consents = "/api/person/{personId}/consents";
        final String consentOrigin = "ConsentController " + consents;
        e.add(api("POST", consents, CONSENT, "consent_create", consentOrigin));
        e.add(api("GET", consents, CONSENT, "consent_list", consentOrigin));
        e.add(api("GET", consents + "/{consentId}", CONSENT, "consent_read", consentOrigin));
        e.add(api("PUT", consents + "/{consentId}", CONSENT, "consent_update", consentOrigin));
        e.add(api("DELETE", consents + "/{consentId}", CONSENT, "consent_delete", consentOrigin));
        e.add(api("POST", consents + "/{consentId}/withdraw", CONSENT, "consent_withdraw", consentOrigin));

        final String snapshotOrigin = "SnapshotController /api/snapshot";
        e.add(api("GET", "/api/snapshot/person/{personId}/latest", SNAPSHOT, "snapshot_latest", snapshotOrigin));
        e.add(api("GET", "/api/snapshot/by-email/{email}", SNAPSHOT, "snapshot_by_email", snapshotOrigin));
        e.add(api("GET", "/api/snapshot/by-phone/{phone}", SNAPSHOT, "snapshot_by_phone", snapshotOrigin));
        // Only split snapshot services implement this operation; other variants answer 404/405.
        e.add(api("POST", "/api/snapshot/person/{personId}", SNAPSHOT, "snapshot_create",
                snapshotOrigin + " (split snapshot services only)"));

        final String actuator = "Spring Boot Actuator";
        e.add(platform("GET", "/actuator", "actuator_index", actuator));
        e.add(platform("GET", "/actuator/health", "actuator_health", actuator));
        e.add(platform("GET", "/actuator/health/**", "actuator_health_component", actuator));
        e.add(platform("GET", "/actuator/info", "actuator_info", actuator));
        e.add(platform("GET", "/actuator/metrics", "actuator_metrics", actuator));
        e.add(platform("GET", "/actuator/metrics/{name}", "actuator_metric", actuator));
        e.add(platform("GET", "/actuator/prometheus", "actuator_prometheus", actuator));

        final String openapi = "springdoc-openapi";
        e.add(platform("GET", "/v3/api-docs", "openapi_docs", openapi));
        e.add(platform("GET", "/v3/api-docs.yaml", "openapi_docs_yaml", openapi));
        e.add(platform("GET", "/v3/api-docs/{group}", "openapi_docs_group", openapi));
        e.add(platform("GET", "/swagger-ui.html", "swagger_ui_page", openapi));
        e.add(platform("GET", "/swagger-ui/**", "swagger_ui_asset", openapi));
        return List.copyOf(e);
    }

    private static Endpoint api(String method, String template, Service service, String operation, String origin) {
        return new Endpoint(method, template, service, operation, API, origin);
    }

    private static Endpoint platform(String method, String template, String operation, String origin) {
        return new Endpoint(method, template, Service.PLATFORM, operation, Kind.PLATFORM, origin);
    }
}
