package ipl.pdms.gateway.selection;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_FLOAT_AS_INT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.ALLOW_COERCION_OF_SCALARS;
import static ipl.pdms.gateway.selection.SelectionModel.Configuration.defaults;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.http.ResponseEntity.badRequest;
import static org.springframework.http.ResponseEntity.ok;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.publisher.Mono.just;
import static reactor.core.scheduler.Schedulers.boundedElastic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.routing.RoutingTable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class SelectionController {
    private final AdminAuth auth;
    private final SelectionService service;
    private final RoutingTable routing;
    private final GatewayClock clock;
    private final ObjectMapper mapper;
    public SelectionController(AdminAuth auth, SelectionService service, RoutingTable routing, GatewayClock clock, ObjectMapper mapper) {
        this.auth = auth; this.service = service; this.routing = routing; this.clock = clock;
        this.mapper = mapper.copy().enable(FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(FAIL_ON_NULL_FOR_PRIMITIVES)
                .disable(ACCEPT_FLOAT_AS_INT).disable(ALLOW_COERCION_OF_SCALARS);
    }
    @GetMapping("/admin/selection")
    public Mono<ResponseEntity<Map<String, Object>>> status(@RequestHeader(value="Authorization", required=false) String authorization) {
        return call(authorization, service::status);
    }
    @PutMapping("/admin/selection")
    public Mono<ResponseEntity<Map<String, Object>>> configure(@RequestHeader(value="Authorization", required=false) String authorization,
                                                              @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion", "configuration"));
            return service.configure(id(json), number(json, "expectedVersion"),
                    mapper.treeToValue(json.required("configuration"), SelectionModel.Configuration.class), "config:" + body);
        });
    }
    @PutMapping("/admin/selection/mock")
    public Mono<ResponseEntity<Map<String, Object>>> mock(@RequestHeader(value="Authorization", required=false) String authorization,
                                                         @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion", "mock"));
            return service.updateMock(id(json), number(json, "expectedVersion"),
                    mapper.treeToValue(json.required("mock"), SelectionModel.MockSignals.class), "mock:" + body);
        });
    }
    @PostMapping("/admin/selection/freeze")
    public Mono<ResponseEntity<Map<String, Object>>> freeze(@RequestHeader(value="Authorization", required=false) String authorization,
                                                           @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion", "runId"));
            return service.freeze(id(json), number(json, "expectedVersion"), text(json, "runId"), "freeze:" + body);
        });
    }
    @PutMapping("/admin/selection/context")
    public Mono<ResponseEntity<Map<String, Object>>> context(@RequestHeader(value="Authorization", required=false) String authorization,
                                                            @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion", "workload", "requiredCapabilities"));
            final JsonNode capabilities = json.required("requiredCapabilities");
            if (!capabilities.isArray()) {
                throw new IllegalArgumentException();
            }
            final Set<String> required = new java.util.HashSet<>();
            for (final JsonNode value : capabilities) {
                if (!value.isTextual() || !required.add(value.textValue())) {
                    throw new IllegalArgumentException();
                }
            }
            return service.updateContext(id(json), number(json, "expectedVersion"), text(json, "workload"), required, "context:" + body);
        });
    }
    @PostMapping("/admin/selection/scenarios/{name}")
    public Mono<ResponseEntity<Map<String, Object>>> scenario(@RequestHeader(value="Authorization", required=false) String authorization,
                                                             @PathVariable String name, @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion"));
            return service.configure(id(json), number(json, "expectedVersion"),
                    SelectionScenarios.create(name, routing.current().active(), clock.epochMillis()), "scenario:" + name + ":" + body);
        });
    }
    @PostMapping("/admin/selection/evaluate")
    public Mono<ResponseEntity<Map<String, Object>>> evaluate(@RequestHeader(value="Authorization", required=false) String authorization,
                                                             @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion", "expectedEpoch"));
            return service.evaluate(id(json), number(json, "expectedVersion"), number(json, "expectedEpoch"), body);
        });
    }
    @PostMapping("/admin/selection/reset")
    public Mono<ResponseEntity<Map<String, Object>>> reset(@RequestHeader(value="Authorization", required=false) String authorization,
                                                          @RequestBody String body) {
        return call(authorization, () -> {
            final JsonNode json = parse(body, Set.of("requestId", "expectedVersion"));
            return service.configure(id(json), number(json, "expectedVersion"), defaults(), "reset:" + body);
        });
    }
    private Mono<ResponseEntity<Map<String, Object>>> call(String authorization, Callable<Map<String, Object>> work) {
        if (!auth.bearer(authorization)) {
            return just(ResponseEntity.status(401).body(Map.of("error", "unauthorised")));
        }
        return fromCallable(() -> ok(work.call())).subscribeOn(boundedElastic())
                .onErrorResume(SelectionService.ControlException.class, e -> just(ResponseEntity.status(e.status).body(Map.of("error", e.getMessage()))))
                .onErrorResume(e -> e instanceof IllegalArgumentException || e instanceof com.fasterxml.jackson.core.JsonProcessingException
                                || e instanceof NullPointerException,
                        e -> just(badRequest().body(Map.of("error", "invalid_selection_request"))));
    }
    private JsonNode parse(String body, Set<String> fields) throws Exception {
        if (body.getBytes(UTF_8).length > 65536) {
            throw new SelectionService.ControlException(413, "selection_request_too_large");
        }
        final JsonNode json = mapper.readTree(body);
        if (json == null || !json.isObject() || json.size() != fields.size()) {
            throw new IllegalArgumentException();
        }
        final var names = json.fieldNames(); while (names.hasNext()) {
            if (!fields.contains(names.next())) {
                throw new IllegalArgumentException();
            }
        }
        return json;
    }
    private String id(JsonNode json) {
        return text(json, "requestId");
    }
    private String text(JsonNode json, String field) {
        final JsonNode value = json.required(field);
        if (!value.isTextual()) {
            throw new IllegalArgumentException();
        } return value.textValue();
    }
    private long number(JsonNode json, String field) {
        final JsonNode value = json.required(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw new IllegalArgumentException();
        }
        return value.longValue();
    }
}
