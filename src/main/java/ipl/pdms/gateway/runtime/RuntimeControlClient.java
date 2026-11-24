package ipl.pdms.gateway.runtime;

import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static java.lang.Math.addExact;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.PATCH;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.util.Assert.isTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

/** Blocking management I/O runs on boundedElastic, outside the request event loop. */
@Component
public class RuntimeControlClient {
    public record Process(String architecture, String url) {}
    private final String adminToken;
    private final Duration timeout;
    private final ObjectMapper mapper;
    private final WebClient http;

    public RuntimeControlClient(ObjectMapper mapper,
                                @Value("${pdms.gateway.outbox.admin-token:}") String adminToken,
                                @Value("${pdms.gateway.outbox.timeout:10s}") Duration timeout) {
        isTrue(timeout != null && !timeout.isNegative() && !timeout.isZero(), "Invalid backend control timeout");
        this.adminToken = adminToken; this.timeout = timeout; this.mapper = mapper;
        final HttpClient client = HttpClient.create().disableRetry(true).followRedirect(false)
                .option(CONNECT_TIMEOUT_MILLIS, 5000).responseTimeout(timeout);
        http = WebClient.builder().clientConnector(new ReactorClientHttpConnector(client))
                .codecs(c -> c.defaultCodecs().maxInMemorySize(65536)).build();
    }

    public ObjectNode status(Process process) {
        final ObjectNode status = exchange(process, null);
        validate(process, status);
        return status;
    }

    public ObjectNode set(Process process, boolean enabled, Set<String> types, String reason) {
        final ObjectNode before = status(process);
        final long version = before.path("snapshot").path("version").longValue();
        final ObjectNode request = mapper.createObjectNode().put("requestId", randomUUID().toString())
                .put("expectedInstanceId", before.path("instanceId").textValue())
                .put("expectedVersion", version).put("enabled", enabled).put("reason", reason);
        request.set("allowedEventTypes", mapper.valueToTree(types));
        final ObjectNode after = exchange(process, request);
        validate(process, after);
        if (!after.path("instanceId").equals(before.path("instanceId"))
                || after.path("snapshot").path("version").longValue() != addExact(version, 1)
                || after.path("enabled").booleanValue() != enabled
                || (!enabled && !after.path("drained").asBoolean(false))) {
            throw new IllegalStateException("Backend did not acknowledge the requested permission/drain: " + process.architecture());
        }
        final Set<String> actual = new java.util.HashSet<>();
        after.path("snapshot").path("policy").path("allowedEventTypes")
                .forEach(type -> actual.add(type.asText()));
        if (!actual.equals(types)) {
            throw new IllegalStateException("Backend allowlist acknowledgement differs");
        }
        return after;
    }

    private ObjectNode exchange(Process process, ObjectNode request) {
        if (adminToken.isBlank()) {
            throw new IllegalStateException("Backend management token is missing");
        }
        final var builder = http.method(request == null ? GET : PATCH)
                .uri(process.url() + "/actuator/outbox-control")
                .header("X-Outbox-Admin-Token", adminToken).accept(APPLICATION_JSON);
        final WebClient.RequestHeadersSpec<?> call = request == null ? builder
                : builder.contentType(APPLICATION_JSON).bodyValue(request);
        try {
            final ObjectNode result = call.exchangeToMono(response -> response.bodyToMono(String.class).defaultIfEmpty("")
                    .map(text -> {
                        if (response.statusCode().value() != 200) {
                            throw new IllegalStateException("Backend management returned HTTP "
                                + response.statusCode().value() + " for " + process.architecture());
                        }
                        try {
                            final JsonNode body = mapper.readTree(text);
                            if (body instanceof ObjectNode node) {
                                return node;
                            }
                        } catch (java.io.IOException invalid) {
                            throw new IllegalStateException("Invalid backend management JSON");
                        }
                        throw new IllegalStateException("Invalid backend management reply");
                    })).timeout(timeout).block(timeout.plusSeconds(1));
            if (result == null) {
                throw new IllegalStateException("Empty backend management reply");
            }
            return result;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Backend management failed: " + process.architecture(), failure);
        }
    }

    private static void validate(Process process, ObjectNode status) {
        final JsonNode version = status.path("snapshot").path("version");
        if (!process.architecture().equals(status.path("architecture").asText())
                || !status.path("instanceId").isTextual()
                || !version.isIntegralNumber() || !version.canConvertToLong() || version.longValue() < 0
                || !status.path("enabled").isBoolean() || !status.path("drained").isBoolean()
                || !status.path("enabled").equals(status.path("snapshot").path("policy").path("enabled"))) {
            throw new IllegalStateException("Invalid backend identity/permission reply: " + process.architecture());
        }
        try {
            fromString(status.path("instanceId").textValue());
        }
        catch (IllegalArgumentException invalid) {
            throw new IllegalStateException("Invalid backend instanceId");
        }
        if (process.architecture().equals("decentralized")) {
            boolean ledger = false;
            for (final JsonNode type : status.path("effectiveEventTypes")) {
                if (type.asText().equals("CONSENT_LEDGER_ANCHOR")) {
                    ledger = true;
                }
            }
            if (!ledger || !status.path("blockchainAvailable").isBoolean()
                    || !status.path("effectiveOnchainAnchorTypes").isArray()) {
                throw new IllegalStateException("Decentralized ledger permission is not independent/enabled");
            }
        }
    }
}
