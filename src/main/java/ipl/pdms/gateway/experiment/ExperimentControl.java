package ipl.pdms.gateway.experiment;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_FLOAT_AS_INT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.ALLOW_COERCION_OF_SCALARS;
import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.WORKLOADS;
import static java.util.UUID.randomUUID;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.springframework.http.ResponseEntity.ok;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.publisher.Mono.just;
import static reactor.core.scheduler.Schedulers.boundedElastic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionJournal;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;
import ipl.pdms.gateway.selection.SelectionService;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class ExperimentControl {
    public record Start(String runId, String indicator, String workload, String sequence) {}
    private static final Set<String> PHASES = Set.of("warmup", "running");
    private final SelectionService selection;
    private final SelectionJournal journal;
    private final RoutingTable routing;
    private final GatewayClock clock;
    private final AdminAuth auth;
    private final ObjectMapper mapper;
    private final boolean enabled;
    private final RuntimeCoordinator runtime;
    private final TrafficStatistics traffic;
    private final Stimulus stimulus;
    private final JsonNode plan;
    private final String planHash;
    private final String scheduleHash;
    private Start request;
    private long startedNs, startedMs, ownedVersion;
    private String phase = "idle", error = "";

    public ExperimentControl(SelectionService selection, SelectionJournal journal, RoutingTable routing,
                             GatewayClock clock, AdminAuth auth, ObjectMapper mapper,
                             @Value("${pdms.gateway.experiment.enabled:false}") boolean enabled,
                             @Value("${pdms.gateway.experiment.file:classpath:selection-experiment.json}") Resource file,
                             RuntimeCoordinator runtime, TrafficStatistics traffic, Stimulus stimulus) throws IOException {
        this.selection = selection; this.journal = journal; this.routing = routing; this.clock = clock;
        this.auth = auth; this.enabled = enabled; this.runtime = runtime; this.traffic = traffic; this.stimulus = stimulus;
        this.mapper = mapper.copy().enable(FAIL_ON_UNKNOWN_PROPERTIES).enable(FAIL_ON_NULL_FOR_PRIMITIVES)
                .disable(ACCEPT_FLOAT_AS_INT).disable(ALLOW_COERCION_OF_SCALARS);
        try (final var input = file.getInputStream()) {
            final byte[] bytes = input.readAllBytes();
            plan = this.mapper.readTree(bytes);
            planHash = sha256Hex(bytes);
        }
        validatePlan();
        final ObjectNode schedule = plan.deepCopy();
        schedule.remove("run");
        scheduleHash = sha256Hex(this.mapper.writeValueAsBytes(schedule));
    }

    @GetMapping("/admin/experiment")
    public Mono<ResponseEntity<Map<String, Object>>> get(@RequestHeader(value="Authorization", required=false) String token) {
        if (!auth.bearer(token)) {
            return just(ResponseEntity.status(401).body(Map.of("error", "unauthorised")));
        }
        return fromCallable(() -> ok(status())).subscribeOn(boundedElastic());
    }

    public synchronized Map<String, Object> start(Start next) {
        synchronized (selection) {
            validateStart(next);
            begin(next);
            return status();
        }
    }
    private void validateStart(Start next) {
        if (!enabled || !validRun(next)) {
            throw new SelectionService.ControlException(400, "invalid_or_disabled_experiment");
        }
        final var state = selection.status();
        final var config = (Configuration)state.get("configuration");
        if (request != null || (boolean)state.get("policyFrozen") || config.enabled()
                || !state.get("automaticPauseReason").equals("") || !state.get("runId").equals(next.runId())) {
            throw new SelectionService.ControlException(409, "experiment_requires_fresh_matching_gateway");
        }
        if (!(boolean)state.get("allowApplyAtStartup") || !runtime.managed() || !runtime.phase().equals("ready")
                || !routing.current().active().equals(definition(next).path("startingArchitecture").asText())) {
            throw new SelectionService.ControlException(409, "experiment_starting_state_not_ready");
        }
    }
    private void begin(Start next) {
        request = next; startedNs = clock.nanoTime(); startedMs = clock.epochMillis();
        try {
            selection.setApplicationEnabled(false);
            selection.configure(id(), version(), configuration(next), "experiment-configuration");
            selection.freeze(id(), version(), next.runId(), "experiment-freeze");
            ownedVersion = version(); phase = "warmup";
            event("experiment_phase");
        } catch (RuntimeException failure) {
            fail(failure);
        }
    }

    @Scheduled(initialDelay = 100, fixedDelay = 100)
    synchronized void tick() {
        try {
            startConfiguredRun();
            if (PHASES.contains(phase)) {
                synchronized (selection) {
                    advance();
                }
            }
        } catch (RuntimeException failure) {
            fail(failure);
        }
    }
    private void startConfiguredRun() {
        if (enabled && phase.equals("idle") && !plan.path("run").isNull() && runtime.phase().equals("ready")
                && ((Number)traffic.snapshot(120).get("n_arrivals")).intValue() > 0) {
            start(mapper.convertValue(plan.path("run"), Start.class));
        }
    }
    private void advance() {
        if (version() != ownedVersion || !selection.status().get("automaticPauseReason").equals("")) {
            throw new IllegalStateException("experiment_selection_state_changed");
        }
        final long elapsed = (clock.nanoTime() - startedNs) / 1_000_000 - warmupMs();
        final String next = elapsed < 0 ? "warmup" : "running";
        final boolean entering = !phase.equals(next);
        phase = next;
        applyStimuli(elapsed);
        selection.setApplicationEnabled(elapsed >= 0 && request.sequence().equals("adaptive"));
        if (entering) {
            event("experiment_phase");
        }
    }
    private void applyStimuli(long elapsed) {
        final boolean changed = stimulus.applyCase(request.indicator(), definition(request), elapsed, configurationWindow());
        updateRequirements(stimulus.requiredCapabilities());
        if (changed) {
            event("experiment_stimulus");
        }
    }
    private int configurationWindow() {
        return ((Configuration)selection.status().get("configuration")).tuning().windowSeconds();
    }
    private void updateRequirements(Set<String> required) {
        final var current = (Configuration)selection.status().get("configuration");
        if (!current.requiredCapabilities().equals(required)) {
            selection.updateContext(id(), version(), request.workload(), required, "experiment-requirements");
            ownedVersion = version();
        }
    }

    @PreDestroy public synchronized void stop() {
        if (PHASES.contains(phase)) {
            try {
                finish("stopped");
            } finally {
                removeStimuli();
            }
        }
    }
    private void finish(String outcome) {
        selection.setApplicationEnabled(false);
        selection.pauseExperiment(outcome);
        removeStimuli();
        phase = outcome;
        event("experiment_finished");
    }
    private void fail(RuntimeException failure) {
        error = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        phase = "failed";
        try {
            selection.setApplicationEnabled(false); selection.pauseExperiment("failed");
        } finally {
            removeStimuli();
        }
        event("experiment_failed");
    }
    private void removeStimuli() {
        stimulus.clear();
        if (request != null && version() == ownedVersion) {
            updateRequirements(Set.of());
        }
    }
    private boolean validRun(Start value) {
        return value != null && value.runId() != null && !value.runId().isBlank()
                && value.indicator() != null && plan.path("cases").has(value.indicator())
                && WORKLOADS.contains(value.workload() == null ? "" : value.workload())
                && Set.of("reference", "adaptive").contains(value.sequence() == null ? "" : value.sequence());
    }
    private JsonNode definition(Start value) {
        return plan.path("cases").path(value.indicator());
    }
    private Configuration configuration(Start value) {
        final var profiles = new LinkedHashMap<String, Map<String, Profile>>();
        merge(plan.path("profiles"), definition(value).path("profiles")).properties().forEach(entry -> profiles.put(entry.getKey(),
                Map.of(value.workload(), mapper.convertValue(entry.getValue(), Profile.class))));
        return new Configuration(true, true, true, "real", value.workload(), Set.of(), profiles,
                mapper.convertValue(merge(plan.path("tuning"), definition(value).path("tuning")), Tuning.class), null);
    }
    private ObjectNode merge(JsonNode base, JsonNode overrides) {
        final ObjectNode result = ((ObjectNode)base).deepCopy();
        if (overrides.isObject()) {
            overrides.properties().forEach(entry -> result.set(entry.getKey(), result.path(entry.getKey()).isObject()
                    && entry.getValue().isObject() ? merge(result.get(entry.getKey()), entry.getValue()) : entry.getValue().deepCopy()));
        }
        return result;
    }
    private void validatePlan() {
        if (plan == null || !plan.has("run") || !plan.path("cases").isObject() || plan.path("cases").isEmpty()
                || !ARCHITECTURES.contains(plan.path("defaultArchitecture").asText())
                || !plan.path("profiles").isObject() || !plan.path("tuning").isObject()
                || !plan.path("warmupSeconds").isInt() || warmupMs() < 0 || warmupMs() > 3_600_000) {
            throw new IllegalArgumentException("Invalid stimulus experiment file");
        }
        plan.path("cases").properties().forEach(entry -> {
            if (entry.getValue() instanceof ObjectNode definition && !definition.has("startingArchitecture")) {
                definition.put("startingArchitecture", plan.path("defaultArchitecture").asText());
            }
            if (!ARCHITECTURES.contains(entry.getValue().path("startingArchitecture").asText())
                    || !ARCHITECTURES.contains(entry.getValue().path("expectedArchitecture").asText())) {
                throw new IllegalArgumentException("Invalid experiment case: " + entry.getKey());
            }
            final var run = new Start("validation", entry.getKey(), "mix", "reference");
            SelectionService.validate(configuration(run));
            Stimulus.validateCase(entry.getKey(), entry.getValue());
        });
        if (!plan.path("run").isNull() && !validRun(mapper.convertValue(plan.path("run"), Start.class))) {
            throw new IllegalArgumentException("Invalid automatic experiment run");
        }
    }
    private long warmupMs() {
        return plan.path("warmupSeconds").asLong() * 1000;
    }
    public synchronized Map<String, Object> status() {
        final var result = new LinkedHashMap<String, Object>();
        result.put("enabledAtStartup", enabled); result.put("phase", phase); result.put("error", error);
        result.put("evidenceKind", "live_observations_with_controlled_stimuli"); result.put("experimentSha256", planHash);
        result.put("stimulusScheduleSha256", scheduleHash);
        result.put("defaultArchitecture", plan.path("defaultArchitecture").asText());
        result.put("activeArchitecture", routing.current().active());
        result.put("stimulusStartsAfterSeconds", warmupMs() / 1000);
        result.put("stimulus", stimulus.status()); result.put("warmupSeconds", warmupMs() / 1000);
        if (request != null) {
            result.put("run", request); result.put("startedAtEpochMs", startedMs);
            result.put("startingArchitecture", definition(request).path("startingArchitecture").asText());
            result.put("plannedStimulusAtEpochMs", startedMs + warmupMs());
            result.put("expectedArchitecture", definition(request).path("expectedArchitecture").asText());
            result.put("policySha256", selection.status().get("policySha256"));
        }
        return result;
    }
    private void event(String name) {
        final var row = new LinkedHashMap<>(status());
        row.put("event", name); row.put("atEpochMs", clock.epochMillis());
        try {
            journal.append(row);
        } catch (IOException failure) {
            throw new SelectionService.ControlException(503, "experiment_journal_unavailable");
        }
    }
    private long version() {
        return ((Number)selection.status().get("version")).longValue();
    }
    private String id() {
        return randomUUID().toString();
    }
}
