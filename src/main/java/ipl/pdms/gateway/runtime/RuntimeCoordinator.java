package ipl.pdms.gateway.runtime;

import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static ipl.pdms.gateway.routing.Service.CONSENT;
import static ipl.pdms.gateway.routing.Service.PERSON;
import static ipl.pdms.gateway.routing.Service.SNAPSHOT;
import static java.lang.Math.addExact;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Map.Entry.comparingByKey;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.springframework.http.ResponseEntity.ok;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.routing.Service;
import ipl.pdms.gateway.selection.SelectionJournal;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RuntimeCoordinator {
    public static final Set<String> ORDINARY = Set.of("PERSON_CREATED", "PERSON_UPDATED", "PERSON_DELETED", "PERSON_SNAPSHOT_CREATED");
    public static final Set<String> ARCHITECTURES = Set.of("monolith", "faas", "microservices", "microservices-async", "decentralized");
    private static final Set<String> ANCHORS = Set.of("CONSENT_CREATED", "CONSENT_UPDATED", "CONSENT_WITHDRAWN", "CONSENT_DELETED");
    private final RoutingTable table;
    private final RuntimeControlClient client;
    private final BackendHealth health;
    private final SelectionJournal journal;
    private final Duration drainTimeout;
    private final boolean managed;
    private final MeterRegistry registry;
    private final String instanceId = randomUUID().toString();
    private final Object admission = new Object();
    private boolean closed = true;
    private int admitted;
    private volatile String owner;
    private volatile String phase = "unreconciled";
    private long version;
    private final Map<String, Set<String>> allowlists = new LinkedHashMap<>();
    private Map<String, ObjectNode> acknowledged = new LinkedHashMap<>();

    public RuntimeCoordinator(RoutingTable table, RuntimeControlClient client, BackendHealth health,
                              ObjectMapper mapper, RequestLog log, MeterRegistry registry,
                              @Value("${pdms.gateway.runtime.managed:true}") boolean managed,
                              @Value("${pdms.gateway.runtime.drain-timeout:30s}") Duration drainTimeout) throws IOException {
        this.table = table; this.client = client; this.health = health;
        this.managed = managed; this.drainTimeout = drainTimeout;
        this.registry = registry;
        if (drainTimeout == null || drainTimeout.isNegative() || drainTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid runtime drain timeout");
        }
        ARCHITECTURES.forEach(name -> allowlists.put(name, ORDINARY));
        journal = new SelectionJournal(mapper, log, "runtime");
        if (managed) {
            table.runtimeOwner(table.current().epoch(), null);
        }
        for (final String name : table.current().realisations()) {
            Gauge.builder("pdms_dyn_runtime_enabled", this, c -> c.allows(name) ? 1 : 0)
                    .description("Gateway business permission after a completed runtime handover")
                    .tag("realisation", name).register(registry);
        }
        Gauge.builder("pdms_dyn_runtime_transitioning", this, c -> c.phase.equals("transitioning") ? 1 : 0).register(registry);
        Gauge.builder("pdms_dyn_runtime_journal_healthy", journal, j -> j.healthy() ? 1 : 0).register(registry);
    }

    public boolean managed() {
        return managed;
    }
    public String instanceId() {
        return instanceId;
    }
    public String phase() {
        return phase;
    }
    public boolean allows(String name) {
        synchronized (admission) {
            return !managed || (!closed && Objects.equals(owner, name));
        }
    }
    public boolean enter(String name) {
        synchronized (admission) {
            if (!managed) {
                return true;
            }
            if (closed || !Objects.equals(owner, name)) {
                return false;
            }
            admitted++; return true;
        }
    }
    public void exit() {
        synchronized (admission) {
            if (managed) {
                admitted--;
                admission.notifyAll();
            }
        }
    }

    public synchronized Map<String, Object> status() {
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("managed", managed); result.put("controllerInstanceId", instanceId);
        result.put("phase", phase); result.put("snapshot", Map.of("version", version, "policy", policy()));
        result.put("epoch", table.current().epoch()); result.put("gatewayActiveArchitecture", table.current().active());
        result.put("active", table.current().active());
        result.put("storage", "process-memory"); result.put("journalHealthy", journal.healthy());
        synchronized (admission) {
            result.put("businessAdmissionOpen", !closed);
            result.put("businessRequests", admitted);
        }
        result.put("processes", Map.copyOf(acknowledged));
        result.put("observationsAreCached", true);
        return result;
    }

    public Map<String, Object> observedStatus() {
        Map<String, Object> result;
        Map<String, ObjectNode> expectedAcknowledgements;
        List<RuntimeControlClient.Process> targets;
        String expectedOwner;
        String expectedPhase;
        long expectedVersion;
        long expectedEpoch;
        synchronized (this) {
            result = new LinkedHashMap<>(status());
            expectedAcknowledgements = Map.copyOf(acknowledged);
            targets = processes(); expectedOwner = owner; expectedPhase = phase;
            expectedVersion = version; expectedEpoch = table.current().epoch();
        }
        final Map<String, ObjectNode> observed = new LinkedHashMap<>();
        boolean coherent = expectedPhase.equals("ready");
        coherent &= observeProcesses(targets, expectedAcknowledgements, expectedOwner, observed);
        synchronized (this) {
            coherent &= version == expectedVersion && table.current().epoch() == expectedEpoch
                    && phase.equals(expectedPhase) && Objects.equals(owner, expectedOwner);
        }
        result.put("processes", observed); result.put("observationsAreCached", false);
        result.put("observedCoherent", coherent); result.put("observedAtEpochMs", currentTimeMillis());
        return result;
    }

    private boolean observeProcesses(List<RuntimeControlClient.Process> targets,
                                     Map<String, ObjectNode> expectedAcknowledgements,
                                     String expectedOwner,
                                     Map<String, ObjectNode> observed) {
        boolean coherent = true;
        // Remote I/O must not hold the mutation monitor or delay a handover.
        for (final RuntimeControlClient.Process process : targets) {
            if (currentThread().isInterrupted()) {
                coherent = false;
                break;
            }
            try {
                final ObjectNode reply = client.status(process);
                observed.put(process.url(), reply);
                final ObjectNode ack = expectedAcknowledgements.get(process.url());
                final boolean expected = Objects.equals(expectedOwner, process.architecture());
                coherent &= matches(reply, ack, expected);
            } catch (RuntimeException unavailable) {
                coherent = false;
            }
        }
        return coherent;
    }

    @Scheduled(initialDelay = 1000, fixedDelay = 1000)
    public synchronized void initialize() {
        if (managed && phase.equals("unreconciled")
                && table.current().realisations().stream().allMatch(name -> health.realisation(name) == UP)) {
            switchTo(table.current().active(), table.current().epoch(), "startup-" + instanceId);
        }
    }

    public synchronized ResponseEntity<Map<String, Object>> switchTo(String target, long expectedEpoch, String decisionId) {
        final String rejection = switchRejection(target, expectedEpoch);
        if (rejection != null) {
            return error(409, rejection);
        }
        return handover(target, expectedEpoch, decisionId);
    }

    private String switchRejection(String target, long expectedEpoch) {
        if (!managed) {
            return "runtime_management_disabled";
        }
        if (phase.equals("unknown")) {
            return "runtime_reconciliation_required";
        }
        if (!ARCHITECTURES.contains(target) || !table.current().knows(target)) {
            return "unknown_architecture";
        }
        if (table.current().epoch() != expectedEpoch) {
            return "stale_routing_epoch";
        }
        if (!table.current().mode().equals("request")) {
            return "request_mode_required";
        }
        return health.realisation(target) == UP ? null : "target_not_up";
    }

    private ResponseEntity<Map<String, Object>> handover(String desired, long expectedEpoch, String requestId) {
        final String reason = "Automatic architecture selection: " + requestId;
        synchronized (admission) {
            closed = true;
            phase = "transitioning";
        }
        version = addExact(version, 1);
        final Map<String, Object> intent = new LinkedHashMap<>();
        intent.put("event", "runtime_intent"); intent.put("requestId", requestId); intent.put("controllerInstanceId", instanceId);
        intent.put("version", version); intent.put("architecture", desired);
        intent.put("desiredOwner", desired); intent.put("reason", reason); intent.put("allowedEventTypes", allowlists);
        ResponseEntity<Map<String, Object>> response;
        final List<RuntimeControlClient.Process> all = processes();
        try {
            journal.append(intent);
            final var paused = table.runtimeOwner(expectedEpoch, null);
            if (paused == null) {
                throw new IllegalStateException("Routing changed concurrently");
            }
            drainGateway();
            final Map<String, ObjectNode> nextAcknowledged = new LinkedHashMap<>();
            final List<String> failures = new ArrayList<>();
            for (final RuntimeControlClient.Process process : all) {
                try {
                    nextAcknowledged.put(process.url(), client.set(process, false, allowlists.get(process.architecture()), reason));
                }
                catch (RuntimeException failure) {
                    failures.add(process.architecture());
                }
            }
            if (!failures.isEmpty()) {
                throw new IllegalStateException("Disable/drain not acknowledged by: " + failures);
            }
            final var routed = table.runtimeOwner(paused.epoch(), desired);
            if (routed == null) {
                throw new IllegalStateException("Routing changed during worker drain");
            }
            for (final RuntimeControlClient.Process process : all.reversed()) {
                if (process.architecture().equals(desired)) {
                    nextAcknowledged.put(process.url(), client.set(process, true, allowlists.get(desired), reason));
                }
            }
            for (final RuntimeControlClient.Process process : all) {
                final ObjectNode observed = client.status(process);
                if (!matches(observed, nextAcknowledged.get(process.url()), Objects.equals(desired, process.architecture()))) {
                    throw new IllegalStateException("Process changed during handover: " + process.architecture());
                }
            }
            journal.append(Map.of("event", "runtime_workers_acknowledged", "requestId", requestId,
                    "controllerInstanceId", instanceId, "version", version, "epoch", routed.epoch(),
                    "processes", nextAcknowledged));
            acknowledged = nextAcknowledged;
            synchronized (admission) {
                owner = desired;
                phase = "ready";
                closed = false;
            }
            final Map<String, Object> reply = new LinkedHashMap<>(status());
            reply.put("status", "completed"); reply.put("name", desired);
            reply.put("request_id", requestId); response = ok(reply);
        } catch (Exception failure) {
            owner = null; phase = "unknown";
            synchronized (admission) {
                closed = true;
            }
            table.runtimeOwner(table.current().epoch(), null);
            // The target may be partly enabled; stop reachable processes and require reconciliation.
            for (final RuntimeControlClient.Process process : all) {
                try {
                    client.set(process, false, allowlists.get(process.architecture()), "Reconcile failed handover");
                }
                catch (RuntimeException ignored) { }
            }
            try { journal.append(Map.of("event", "runtime_outcome_unknown", "requestId", requestId,
                    "controllerInstanceId", instanceId, "version", version)); }
            catch (IOException ignored) { }
            final Map<String, Object> reply = new LinkedHashMap<>(status());
            reply.put("error", "runtime_reconciliation_required"); reply.put("outcomeUnknown", true);
            reply.put("detail", failure.getMessage() == null ? "Runtime handover did not complete" : failure.getMessage());
            response = ResponseEntity.status(503).body(reply);
            if (failure instanceof InterruptedException) {
                currentThread().interrupt();
            }
        }
        io.micrometer.core.instrument.Counter.builder("pdms_dyn_runtime_changes")
                .tag("result", response.getStatusCode().is2xxSuccessful() ? "completed" : "unknown")
                .register(registry).increment();
        return response;
    }

    private void drainGateway() throws InterruptedException {
        final long deadline = nanoTime() + drainTimeout.toNanos();
        synchronized (admission) {
            while (admitted != 0) {
                final long remaining = deadline - nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("Gateway business requests did not drain");
                }
                NANOSECONDS.timedWait(admission, remaining);
            }
        }
    }

    private List<RuntimeControlClient.Process> processes() {
        final List<RuntimeControlClient.Process> result = new ArrayList<>();
        final var routing = table.current();
        routing.backends().entrySet().stream().sorted(comparingByKey()).forEach(entry -> {
            final Set<String> urls = new java.util.LinkedHashSet<>();
            // Drain consent before person and snapshot so admitted callbacks can finish.
            for (final Service service : List.of(CONSENT, PERSON, SNAPSHOT)) {
                urls.add(routing.baseUrl(entry.getKey(), service));
            }
            urls.forEach(url -> result.add(new RuntimeControlClient.Process(entry.getKey(), url)));
        });
        return result;
    }

    private static boolean matches(ObjectNode observed, ObjectNode ack, boolean enabled) {
        return ack != null && observed.path("instanceId").equals(ack.path("instanceId"))
                && observed.path("snapshot").equals(ack.path("snapshot"))
                && observed.path("enabled").isBoolean() && observed.path("enabled").booleanValue() == enabled
                && (enabled || observed.path("drained").asBoolean(false));
    }

    private Map<String, Object> policy() {
        final Map<String, Object> p = new LinkedHashMap<>();
        p.put("enabled", phase.equals("ready") && owner != null); p.put("activeArchitecture", owner);
        p.put("allowedEventTypes", allowlists); p.put("ledgerProcessingEnabled", true);
        p.put("onchainProcessingEnabled", true); p.put("allowedOnchainAnchorTypes", ANCHORS);
        p.put("decentralizedOrdinaryProcessingEnabled", Objects.equals(owner, "decentralized"));
        return p;
    }

    private static ResponseEntity<Map<String, Object>> error(int code, String message) {
        return ResponseEntity.status(code).body(Map.of("error", message));
    }
    @PreDestroy public void close() throws IOException {
        journal.close();
    }
}
