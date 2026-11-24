package ipl.pdms.gateway;

import static ipl.pdms.gateway.health.HealthState.Status.DOWN;
import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;

import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import ipl.pdms.gateway.selection.SelectionRouting;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SelectionRoutingTest {
    final RuntimeCoordinator runtime = mock(RuntimeCoordinator.class);
    final BackendHealth health = mock(BackendHealth.class);
    final Decision decision = new Decision("switch", "latency_breach", "monolith", "faas", "mock", "aggregate_fixture", 1000, List.of(), 2, 0);
    final RoutingSnapshot snapshot = new RoutingSnapshot("routing/1", 7, "monolith", "request",
            Map.of("monolith", Map.of("default", "http://localhost:1"), "faas", Map.of("default", "http://localhost:2")), Set.of());

    SelectionRouting changer(boolean allowed) {
        return new SelectionRouting(runtime, health, allowed);
    }

    @BeforeEach void prepare() {
        when(runtime.managed()).thenReturn(true);
        when(runtime.phase()).thenReturn("ready");
        when(health.realisation("faas")).thenReturn(UP);
        when(runtime.switchTo(anyString(), anyLong(), anyString())).thenReturn(ok(Map.of("status", "completed")));
    }

    @Test void selectionCallsTheCoordinatorWithItsDecisionAndEpoch() {
        assertTrue(changer(true).apply(decision, snapshot, "decision-id").applied());
        verify(runtime).switchTo("faas", 7, "decision-id");
    }

    @Test void startupPermissionCannotBeBypassed() {
        assertEquals("application_disabled_at_startup", changer(false).apply(decision, snapshot, "id").details().get("applicationResult"));
    }

    @Test void automaticApplicationRequiresManagedRuntime() {
        when(runtime.managed()).thenReturn(false);
        assertEquals("automatic_requires_managed_runtime", changer(true).apply(decision, snapshot, "id").details().get("applicationResult"));
    }

    @Test void mockHealthCannotBypassLiveHealth() {
        when(health.realisation("faas")).thenReturn(DOWN);
        assertEquals("live_target_not_up", changer(true).apply(decision, snapshot, "id").details().get("applicationResult"));
    }

    @Test void unreconciledRuntimePreventsApplication() {
        when(runtime.phase()).thenReturn("unknown");
        assertEquals("runtime_not_ready", changer(true).apply(decision, snapshot, "id").details().get("applicationResult"));
    }

    @Test void rejectedOrUncertainCoordinatorCallRequiresReconciliation() {
        when(runtime.switchTo(anyString(), anyLong(), anyString())).thenReturn(status(503).body(Map.of("outcomeUnknown", true)));
        assertTrue(changer(true).apply(decision, snapshot, "id").reconcileRequired());
        when(runtime.switchTo(anyString(), anyLong(), anyString())).thenThrow(new IllegalStateException("isolated failure"));
        var outcome = changer(true).apply(decision, snapshot, "id2");
        assertFalse(outcome.applied()); assertTrue(outcome.reconcileRequired()); assertEquals(true, outcome.details().get("outcomeUnknown"));
    }
}
