package ipl.pdms.gateway.routing;

import static ipl.pdms.gateway.routing.PdmsEndpoints.all;
import static org.springframework.http.ResponseEntity.ok;

import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.health.BackendHealth;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.monitoring.RunProvenance;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class AdminController {

    private final RoutingTable table;
    private final TrafficStatistics stats;
    private final RequestLog log;
    private final AdminAuth auth;
    private final BackendHealth health;
    private final RunProvenance provenance;
    private final RuntimeCoordinator runtime;


    @GetMapping("/admin/status")
    public ResponseEntity<Map<String, Object>> status(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!auth.bearer(authorization)) {
            return error(401, "unauthorised");
        }
        final RoutingSnapshot s = table.current();
        final Map<String, Object> out = new LinkedHashMap<>();
        out.put("version", s.version());
        out.put("epoch", s.epoch());
        out.put("active", s.active());
        out.put("mode", s.mode());
        out.put("realisations", s.realisations());
        out.put("disabled", new java.util.TreeSet<>(s.disabled()));
        out.put("instance_id", log.instanceId());
        out.put("run_id", log.runId());
        out.put("config_hash", table.configHash());
        out.put("provenance", provenance.values());
        out.put("log_file", log.file().getFileName().toString());
        out.put("log_rows_written", log.rowsWritten());
        out.put("log_rows_dropped", log.rowsDropped());
        out.put("log_last_seq", log.lastSequence());
        out.put("log_complete", log.isComplete());
        out.put("stats_overflow", stats.overflowCount());
        out.put("runtime_managed", runtime.managed());
        out.put("runtime_controller_instance", runtime.instanceId());
        out.put("runtime_phase", runtime.phase());
        return ok(out);
    }

    @GetMapping("/admin/endpoints")
    public ResponseEntity<Map<String, Object>> endpoints(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!auth.bearer(authorization)) {
            return error(401, "unauthorised");
        }
        final RoutingSnapshot snapshot = table.current();
        final List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (final Endpoint e : all()) {
            final Map<String, Object> row = new LinkedHashMap<>();
            row.put("method", e.method());
            row.put("path", e.template());
            row.put("service", e.service().key());
            row.put("operation", e.operation());
            row.put("kind", e.kind().key());
            row.put("origin", e.origin());
            final Map<String, String> targets = new LinkedHashMap<>();
            for (final String realisation : snapshot.realisations()) {
                targets.put(realisation, snapshot.baseUrl(realisation, e.service()));
            }
            row.put("targets", targets);
            rows.add(row);
        }
        final Map<String, Object> out = new LinkedHashMap<>();
        out.put("epoch", snapshot.epoch());
        out.put("count", rows.size());
        out.put("endpoints", rows);
        return ok(out);
    }

    @GetMapping("/admin/health")
    public ResponseEntity<Map<String, Object>> healthReport(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!auth.bearer(authorization)) {
            return error(401, "unauthorised");
        }
        final Map<String, Object> out = new LinkedHashMap<>();
        out.put("epoch", table.current().epoch());
        out.put("realisations", health.report());
        return ok(out);
    }

    @GetMapping("/admin/architectures")
    public ResponseEntity<Map<String, Object>> architectures(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!auth.bearer(authorization)) {
            return error(401, "unauthorised");
        }
        final RoutingSnapshot s = table.current();
        final List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (final String name : new java.util.TreeSet<>(s.realisations())) {
            final Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            row.put("enabled", s.isEnabled(name));
            row.put("active", s.active().equals(name));
            row.put("health", health.realisation(name).name());
            row.put("backends", s.backends().get(name));
            rows.add(row);
        }
        final Map<String, Object> out = new LinkedHashMap<>();
        out.put("epoch", s.epoch());
        out.put("architectures", rows);
        return ok(out);
    }

    @GetMapping("/internal/stats")
    public ResponseEntity<Map<String, Object>> stats(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(name = "window_s", defaultValue = "30") int windowSeconds) {
        if (!auth.bearer(authorization)) {
            return error(401, "unauthorised");
        }
        if (windowSeconds < 1 || windowSeconds > 120) {
            return error(400, "window_s must be 1..120");
        }
        return ok(stats.snapshot(windowSeconds));
    }

    private static ResponseEntity<Map<String, Object>> error(int status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }

}
