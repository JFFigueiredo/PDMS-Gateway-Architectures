package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.health.HealthState.Status.UP;

import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SelectionRouting {
    public record Outcome(boolean applied, boolean reconcileRequired, Map<String, Object> details) {}
    private final RuntimeCoordinator runtime;
    private final BackendHealth health;
    private final boolean allowApply;

    public SelectionRouting(RuntimeCoordinator runtime, BackendHealth health,
                            @Value("${pdms.gateway.selection.allow-apply:false}") boolean allowApply) {
        this.runtime = runtime; this.health = health;
        this.allowApply = allowApply;
    }

    public boolean managed() {
        return runtime.managed();
    }

    public Outcome apply(Decision decision, RoutingSnapshot snapshot, String decisionId) {
        final String rejection = rejection(decision, snapshot);
        if (rejection != null) {
            return new Outcome(false, false, Map.of("applicationResult", rejection));
        }
        return switchArchitecture(decision.target(), snapshot.epoch(), decisionId);
    }

    private Outcome switchArchitecture(String target, long epoch, String decisionId) {
        try {
            final var response = runtime.switchTo(target, epoch, decisionId);
            final boolean applied = response.getStatusCode().is2xxSuccessful();
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put("applicationHttpStatus", response.getStatusCode().value());
            details.put("applicationResult", response.getBody());
            return new Outcome(applied, !applied, details);
        } catch (RuntimeException failure) {
            return new Outcome(false, true, Map.of("applicationResult", "routing_application_failed_reconcile", "outcomeUnknown", true));
        }
    }

    private String rejection(Decision decision, RoutingSnapshot snapshot) {
        if (!allowApply) {
            return "application_disabled_at_startup";
        }
        if (!managed()) {
            return "automatic_requires_managed_runtime";
        }
        if (!snapshot.mode().equals("request")) {
            return "request_mode_required";
        }
        if (!runtime.phase().equals("ready")) {
            return "runtime_not_ready";
        }
        return health.realisation(decision.target()) == UP ? null : "live_target_not_up";
    }
}
