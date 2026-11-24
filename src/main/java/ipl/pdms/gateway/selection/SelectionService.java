package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.CAPABILITIES;
import static ipl.pdms.gateway.selection.SelectionModel.Configuration.defaults;
import static ipl.pdms.gateway.selection.SelectionModel.DECLARED_CAPABILITIES;
import static ipl.pdms.gateway.selection.SelectionModel.WORKLOADS;
import static ipl.pdms.gateway.selection.SelectionPolicyVersion.document;
import static ipl.pdms.gateway.selection.SelectionPolicyVersion.sha256;
import static java.lang.Double.isFinite;
import static java.util.Collections.unmodifiableMap;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import ipl.pdms.gateway.selection.SelectionModel.Signals;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SelectionService {
    public static final class ControlException extends RuntimeException {
        public final int status;
        public ControlException(int status, String reason) {
            super(reason);
            this.status = status;
        }
    }
    private record Stored(String body, Map<String, Object> reply) {}
    private volatile Configuration config = defaults();
    private long version;
    private Map<String, Object> frozenPolicy = Map.of();
    private volatile Map<String, Object> last = Map.of();
    private long lastScheduledMs = Long.MIN_VALUE;
    private boolean applicationEnabled = true;
    private final SelectionPolicy policy = new SelectionPolicy();
    private final SelectionPolicy previewPolicy = new SelectionPolicy();
    private final Map<String, Stored> changes = new HashMap<>();
    private final Map<String, Stored> evaluations = new HashMap<>();
    private final RoutingTable routing;
    private final GatewayClock clock;
    private final ObjectMapper mapper;
    private final SelectionJournal journal;
    private final MeterRegistry registry;
    private final boolean allowApply;
    private final String runId;
    private final SelectionSignals signalReader;
    private final SelectionRouting routeChanger;
    private volatile String automaticPause = "";
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(r -> {
        final Thread thread = new Thread(r, "pdms-selection"); thread.setDaemon(true); return thread;
    });

    public SelectionService(RoutingTable routing, SelectionSignals signalReader, SelectionRouting routeChanger,
                            GatewayClock clock, ObjectMapper mapper, SelectionJournal journal, MeterRegistry registry,
                            @Value("${pdms.gateway.selection.allow-apply:false}") boolean allowApply,
                            ipl.pdms.gateway.config.GatewayProperties properties) {
        this.routing = routing; this.signalReader = signalReader; this.routeChanger = routeChanger;
        this.clock = clock; this.mapper = mapper; this.journal = journal; this.registry = registry;
        this.allowApply = allowApply; this.runId = properties.runId();
        Gauge.builder("pdms_dyn_selection_enabled", this, s -> s.enabled() ? 1 : 0).register(registry);
        Gauge.builder("pdms_dyn_selection_mock_mode", this, s -> s.mockMode() ? 1 : 0).register(registry);
        Gauge.builder("pdms_dyn_selection_apply_enabled", this, s -> s.applyEnabled() ? 1 : 0).register(registry);
        Gauge.builder("pdms_dyn_selection_journal_healthy", journal, j -> j.healthy() ? 1 : 0).register(registry);
        Gauge.builder("pdms_dyn_selection_last_evaluation_timestamp_seconds", this, s -> s.lastTimestamp()).register(registry);
        for (final String name : ARCHITECTURES) {
            Gauge.builder("pdms_dyn_selection_recommended", this, s -> s.recommended(name) ? 1 : 0)
                .tag("realisation", name).register(registry);
        }
    }
    private boolean enabled() {
        return config.enabled();
    }
    private boolean mockMode() {
        return config.source().equals("mock");
    }
    private boolean applyEnabled() {
        return config.applyChanges() && allowApply;
    }
    private double lastTimestamp() {
        final Object value = last.get("evaluatedAtEpochMs");
        return value instanceof Number n ? n.doubleValue() / 1000 : 0;
    }
    private boolean recommended(String name) {
        return name.equals(last.get("target"));
    }

    @PostConstruct void start() {
        executor.scheduleWithFixedDelay(() -> {
            synchronized (this) {
                if (!config.enabled() || !config.autoEvaluate()) {
                    return;
                }
                if (!automaticPause.isEmpty() || config.applyChanges() && frozenPolicy.isEmpty()) {
                    return;
                }
                final long now = clock.epochMillis();
                if (lastScheduledMs != Long.MIN_VALUE && now - lastScheduledMs < config.tuning().tickMs()) {
                    return;
                }
                lastScheduledMs = now;
                try {
                    evaluateInternal("scheduled-" + randomUUID(), routing.current().epoch(), true);
                }
                catch (RuntimeException failure) {
                    org.slf4j.LoggerFactory.getLogger(SelectionService.class).warn("Selection tick failed: {}", failure.getMessage());
                }
            }
        }, 1, 1, SECONDS);
    }
    @PreDestroy void stop() {
        executor.shutdownNow();
    }

    public synchronized void pauseExperiment(String reason) {
        if (!automaticPause.isEmpty() && !automaticPause.startsWith("experiment_")) {
            return;
        }
        automaticPause = "experiment_" + reason;
        append(Map.of("event", "selection_experiment_paused", "reason", automaticPause,
                "runId", runId, "atEpochMs", clock.epochMillis(), "policySha256", policySha256()));
    }

    public synchronized void setApplicationEnabled(boolean enabled) {
        if (applicationEnabled != enabled) {
            if (!enabled) {
                applicationEnabled = false;
            }
            append(Map.of("event", "selection_application_gate", "enabled", enabled,
                    "runId", runId, "atEpochMs", clock.epochMillis(), "policySha256", policySha256()));
            applicationEnabled = enabled;
        }
    }

    public synchronized Map<String, Object> status() {
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("version", version); result.put("configuration", config);
        result.put("allowApplyAtStartup", allowApply); result.put("lastDecision", last);
        result.put("journalFile", journal.fileName()); result.put("capabilities", DECLARED_CAPABILITIES);
        result.put("journalHealthy", journal.healthy());
        result.put("automaticPauseReason", automaticPause);
        result.put("automaticWaitingForFreeze", config.autoEvaluate() && config.applyChanges() && frozenPolicy.isEmpty());
        result.put("runId", runId); result.put("policySha256", policySha256());
        result.put("policyFrozen", !frozenPolicy.isEmpty()); result.put("policyFreeze", frozenPolicy);
        result.put("applicationEnabled", applicationEnabled);
        return result;
    }

    public synchronized Map<String, Object> configure(String id, long expected, Configuration value, String body) {
        validateId(id);
        final Stored replay = changes.get(id);
        if (replay != null) {
            return replay(replay, body);
        }
        if (expected != version) {
            throw new ControlException(409, "stale_selection_version");
        }
        if (!frozenPolicy.isEmpty()) {
            throw new ControlException(409, "selection_policy_frozen");
        }
        if (changes.size() >= 10000) {
            throw new ControlException(503, "configuration_history_full");
        }
        validate(value);
        if (value.applyChanges() && !allowApply) {
            throw new ControlException(409, "application_disabled_at_startup");
        }
        append(Map.of("event", "selection_configuration", "requestId", id, "version", version + 1,
                "configuration", value, "atEpochMs", clock.epochMillis(), "runId", runId,
                "policySha256", sha256(value, allowApply)));
        config = value; version++; policy.resetEvidence(); previewPolicy.resetEvidence(); lastScheduledMs = Long.MIN_VALUE;
        last = Map.of();
        final Map<String, Object> reply = status();
        changes.put(id, new Stored(body, reply));
        return reply;
    }

    /** Restart before replacing a frozen policy. */
    public synchronized Map<String, Object> freeze(String id, long expected, String requestedRunId, String body) {
        validateId(id);
        final Stored stored = changes.get(id);
        if (stored != null) {
            return replay(stored, body);
        }
        if (expected != version) {
            throw new ControlException(409, "stale_selection_version");
        }
        if (!frozenPolicy.isEmpty()) {
            throw new ControlException(409, "selection_policy_frozen");
        }
        if (requestedRunId == null || requestedRunId.isBlank() || !requestedRunId.equals(runId)) {
            throw new ControlException(409, "selection_run_id_mismatch");
        }
        if (changes.size() >= 10000) {
            throw new ControlException(503, "configuration_history_full");
        }
        final Map<String, Object> frozen = Map.of("runId", runId, "policySha256", policySha256(),
                "frozenAtEpochMs", clock.epochMillis(), "configurationVersion", version + 1,
                "policyDocument", document(config, allowApply));
        final Map<String, Object> record = new LinkedHashMap<>(frozen);
        record.put("event", "selection_policy_frozen"); record.put("requestId", id);
        record.put("initialWorkload", config.workload()); record.put("initialRequiredCapabilities", config.requiredCapabilities());
        append(record);
        frozenPolicy = frozen; version++; policy.resetEvidence(); previewPolicy.resetEvidence(); lastScheduledMs = Long.MIN_VALUE; last = Map.of();
        final Map<String, Object> reply = status(); changes.put(id, new Stored(body, reply)); return reply;
    }

    /** A frozen policy permits context changes, but not threshold, profile or execution-mode changes. */
    public synchronized Map<String, Object> updateContext(String id, long expected, String workload,
                                                          Set<String> requiredCapabilities, String body) {
        validateId(id);
        final Stored stored = changes.get(id);
        if (stored != null) {
            return replay(stored, body);
        }
        if (expected != version) {
            throw new ControlException(409, "stale_selection_version");
        }
        if (changes.size() >= 10000) {
            throw new ControlException(503, "configuration_history_full");
        }
        final var next = config.withWorkload(workload).withRequiredCapabilities(requiredCapabilities);
        validate(next);
        append(Map.of("event", "selection_context", "requestId", id, "version", version + 1,
                "workload", workload, "requiredCapabilities", requiredCapabilities, "atEpochMs", clock.epochMillis(),
                "runId", runId, "policySha256", policySha256(), "policyFrozen", !frozenPolicy.isEmpty()));
        config = next; version++; policy.resetEvidence(); previewPolicy.resetEvidence(); lastScheduledMs = Long.MIN_VALUE; last = Map.of();
        final Map<String, Object> reply = status(); changes.put(id, new Stored(body, reply)); return reply;
    }

    private String policySha256() {
        return frozenPolicy.isEmpty() ? sha256(config, allowApply)
                : (String)frozenPolicy.get("policySha256");
    }

    public synchronized Map<String, Object> updateMock(String id, long expected, MockSignals signals, String body) {
        validateId(id);
        final Stored replay = changes.get(id);
        if (replay != null) {
            return replay(replay, body);
        }
        if (expected != version) {
            throw new ControlException(409, "stale_selection_version");
        }
        if (changes.size() >= 10000) {
            throw new ControlException(503, "configuration_history_full");
        }
        if (!config.source().equals("mock")) {
            throw new ControlException(409, "mock_source_required");
        }
        final var next = config.withMock(signals);
        validate(next);
        append(Map.of("event", "selection_mock_inputs", "requestId", id, "version", version + 1,
                "mock", signals, "atEpochMs", clock.epochMillis(), "runId", runId,
                "policySha256", policySha256(), "policyFrozen", !frozenPolicy.isEmpty()));
        config = next; version++;
        final Map<String, Object> reply = status(); changes.put(id, new Stored(body, reply)); return reply;
    }

    public synchronized Map<String, Object> evaluate(String id, long expectedVersion, long expectedEpoch, String body) {
        validateId(id);
        final Stored replay = evaluations.get(id);
        if (replay != null) {
            return replay(replay, body);
        }
        if (expectedVersion != version) {
            throw new ControlException(409, "stale_selection_version");
        }
        if (evaluations.size() >= 10000) {
            throw new ControlException(503, "evaluation_history_full");
        }
        final Map<String, Object> result = evaluateInternal(id, expectedEpoch, false);
        evaluations.put(id, new Stored(body, result));
        return result;
    }

    private Map<String, Object> evaluateInternal(String id, long expectedEpoch, boolean scheduled) {
        final var snapshot = routing.current();
        if (snapshot.epoch() != expectedEpoch) {
            throw new ControlException(409, "stale_routing_epoch");
        }
        final long now = clock.epochMillis();
        final Signals signal = signalReader.read(config);
        // Paused flags identify the current owner, not which candidates can be activated.
        final var candidates = routeChanger.managed() ? new ipl.pdms.gateway.routing.RoutingSnapshot(snapshot.version(),
                snapshot.epoch(), snapshot.active(), snapshot.mode(), snapshot.backends(), Set.of()) : snapshot;
        final Decision decision = (scheduled ? policy : previewPolicy)
                .evaluate(config, signal, candidates, now);
        final Map<String, Object> record = new LinkedHashMap<>(mapper.convertValue(decision, new TypeReference<Map<String, Object>>() {}));
        record.put("event", "selection_decision"); record.put("requestId", id);
        record.put("configurationVersion", version); record.put("routingEpoch", snapshot.epoch());
        record.put("runId", runId); record.put("policySha256", policySha256());
        record.put("policyFrozen", !frozenPolicy.isEmpty());
        record.put("signals", signal); record.put("requiredCapabilities", config.requiredCapabilities());
        record.put("workload", config.workload()); record.put("tuning", config.tuning());
        record.put("applicationEnabled", applicationEnabled);
        record.put("applyRequested", scheduled && config.applyChanges() && applicationEnabled); record.put("applied", false);
        append(record);
        if (scheduled && decision.action().equals("switch") && config.applyChanges() && applicationEnabled) {
            final SelectionRouting.Outcome outcome = routeChanger.apply(decision, snapshot, id);
            record.putAll(outcome.details()); record.put("applied", outcome.applied());
            record.put("routingEpochAfter", routing.current().epoch());
            record.put("activeAfter", routing.current().active());
            if (outcome.applied()) {
                final long appliedAt = clock.epochMillis();
                policy.applied(appliedAt); record.put("appliedAtEpochMs", appliedAt);
            }
            if (outcome.reconcileRequired()) {
                automaticPause = "routing_reconciliation_required";
            }
            record.put("event", "selection_application_outcome");
            try {
                journal.append(record);
            }
            catch (java.io.IOException failure) {
                automaticPause = "outcome_journal_unavailable_reconcile_routing";
                record.put("outcomeUnknown", true);
                record.put("applicationResult", "outcome_journal_unavailable_reconcile_routing");
            }
        }
        last = unmodifiableMap(new LinkedHashMap<>(record));
        Counter.builder("pdms_dyn_selection_decisions").tags("source", decision.source(), "action", decision.action(), "reason", decision.reason())
                .register(registry).increment();
        return last;
    }

    private void append(Object value) {
        try {
            journal.append(value);
        }
        catch (java.io.IOException failure) {
            throw new ControlException(503, "decision_journal_unavailable");
        }
    }
    private static Map<String, Object> replay(Stored stored, String body) {
        if (!stored.body().equals(body)) {
            throw new ControlException(409, "request_id_reused");
        }
        return stored.reply();
    }
    private static void validateId(String id) {
        try {
            fromString(id);
        } catch (RuntimeException invalid) {
            throw new ControlException(400, "requestId_must_be_uuid");
        }
    }
    public static void validate(Configuration c) {
        if (c == null || c.source() == null || c.workload() == null || !Set.of("real", "mock").contains(c.source()) || !WORKLOADS.contains(c.workload())
                || c.requiredCapabilities() == null || !CAPABILITIES.containsAll(c.requiredCapabilities())
                || c.profiles() == null || c.tuning() == null) {
            throw new ControlException(400, "invalid_selection_configuration");
        }
        validateTuning(c.tuning());
        validateProfiles(c.profiles());
        if (c.source().equals("mock")) {
            validateMockSignals(c.mock());
        }
    }

    private static void validateTuning(Tuning t) {
        if (t.tickMs() < 100 || t.windowSeconds() < 1 || t.windowSeconds() > 120 || t.minSamples() < 1 || t.maxAgeMs() < 1
                || !positive(t.highP99Ms()) || !positive(t.recoveryP99Ms()) || t.recoveryP99Ms() >= t.highP99Ms()
                || t.breachTicks() < 1 || t.opportunityTicks() < 1 || t.dwellMs() < 0
                || !positive(t.maxUtilization()) || t.maxUtilization() > 1 || !isFinite(t.improvementFraction())
                || t.improvementFraction() < 0 || t.improvementFraction() >= 1 || !isFinite(t.improvementMs())
                || t.improvementMs() < 0 || t.maxSwitches() < 1 || t.flapWindowMs() < 1 || !positive(t.highErrorRatio()) || t.highErrorRatio() > 1) {
            throw new ControlException(400, "invalid_selection_tuning");
        }
    }

    private static void validateProfiles(Map<String, Map<String, Profile>> profiles) {
        for (final var arch : profiles.entrySet()) {
            if (!ARCHITECTURES.contains(arch.getKey()) || arch.getValue() == null) {
                throw new ControlException(400, "unknown_profile_architecture");
            }
            for (final var workload : arch.getValue().entrySet()) {
                final Profile p = workload.getValue();
                if (!WORKLOADS.contains(workload.getKey()) || p == null || p.evidenceKind() == null || !Set.of("synthetic", "measured", "configured").contains(p.evidenceKind())
                        || p.provenance() == null || p.provenance().isBlank() || p.provenance().length() > 256
                        || !positive(p.capacityRps()) || !positive(p.predictedP99Ms()) || !isFinite(p.minRateRps())
                        || p.minRateRps() < 0 || !isFinite(p.maxRateRps()) || p.maxRateRps() < p.minRateRps()
                        || p.maxRateRps() > p.capacityRps()) {
                    throw new ControlException(400, "invalid_candidate_profile");
                }
            }
        }
    }

    private static void validateMockSignals(MockSignals m) {
        if (m == null || m.sampleTimestampEpochMs() < 0 || m.samples() < 0 || m.pending() < 0
                || m.p99Ms() != null && (!isFinite(m.p99Ms()) || m.p99Ms() < 0)
                || !isFinite(m.arrivalRps()) || m.arrivalRps() < 0 || m.healthy() == null
                || !Set.copyOf(ARCHITECTURES).containsAll(m.healthy().keySet())
                || m.unexpectedErrorRatio() != null && (!isFinite(m.unexpectedErrorRatio()) || m.unexpectedErrorRatio() < 0 || m.unexpectedErrorRatio() > 1)) {
            throw new ControlException(400, "invalid_mock_signals");
        }
    }

    private static boolean positive(double value) {
        return isFinite(value) && value > 0;
    }
}
