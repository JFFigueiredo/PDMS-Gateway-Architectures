package ipl.pdms.gateway.health;

import static ipl.pdms.gateway.health.HealthState.Status.DOWN;
import static ipl.pdms.gateway.health.HealthState.Status.UNKNOWN;
import static ipl.pdms.gateway.health.HealthState.Status.UP;
import static java.lang.Math.max;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.experiment.Stimulus;
import ipl.pdms.gateway.health.HealthState.Status;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.routing.RoutingTable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.stereotype.Component;

@Component
public class BackendHealth {

    private static final String PATH = "/actuator/health";

    private final RoutingTable table;
    private final GatewayProperties properties;
    private final GatewayClock clock;
    private final ObjectMapper mapper;
    private final Map<String, HealthState> byUrl = new LinkedHashMap<>();
    private final HttpClient client;
    private final Stimulus stimulus;
    private ScheduledExecutorService executor;

    public BackendHealth(RoutingTable table, GatewayProperties properties, GatewayClock clock,
                         ObjectMapper mapper, MeterRegistry registry, Stimulus stimulus) {
        this.table = table;
        this.properties = properties;
        this.clock = clock;
        this.mapper = mapper;
        this.stimulus = stimulus;
        this.client = HttpClient.newBuilder().version(HTTP_1_1)
                .connectTimeout(properties.healthTimeout()).build();
        final RoutingSnapshot snapshot = table.current();
        for (final Map<String, String> urls : snapshot.backends().values()) {
            for (final String url : urls.values()) {
                byUrl.computeIfAbsent(url, u -> new HealthState());
            }
        }
        for (final String realisation : snapshot.realisations()) {
            Gauge.builder("pdms_dyn_realisation_up", this, h -> h.realisation(realisation) == UP ? 1 : 0)
                    .description("1 when every backend process of the realisation answers its liveness probe")
                    .tag("realisation", realisation).register(registry);
            for (final Map.Entry<String, String> e : snapshot.backends().get(realisation).entrySet()) {
                final HealthState state = byUrl.get(e.getValue());
                Gauge.builder("pdms_dyn_backend_up", state, s -> s.status() == UP ? 1 : 0)
                        .description("1 when the backend process answers its liveness probe")
                        .tag("realisation", realisation).tag("service", e.getKey()).register(registry);
            }
        }
    }

    @PostConstruct
    void start() {
        executor = newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "pdms-gateway-health");
            t.setDaemon(true);
            return t;
        });
        final long interval = properties.healthInterval().toMillis();
        executor.scheduleWithFixedDelay(this::probeAll, 0, max(interval, 1), MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    void probeAll() {
        final List<java.util.concurrent.CompletableFuture<Void>> pending = new ArrayList<>();
        for (final Map.Entry<String, HealthState> e : byUrl.entrySet()) {
            pending.add(probe(e.getKey(), e.getValue()));
        }
        for (final var f : pending) {
            f.join();
        }
    }

    private java.util.concurrent.CompletableFuture<Void> probe(String url, HealthState state) {
        final Duration timeout = properties.healthTimeout();
        final HttpRequest request = HttpRequest.newBuilder(URI.create(url + PATH)).GET().timeout(timeout).build();
        final Map<String, String> targets = table.current().backends().getOrDefault(stimulus.target(), Map.of());
        return stimulus.probe(url, () -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), Set.copyOf(targets.values()))
                .handle((response, error) -> {
                    boolean ok = false;
                    String message = null;
                    if (error != null) {
                        final Throwable cause = error.getCause() != null ? error.getCause() : error;
                        message = cause.getClass().getSimpleName();
                    } else if (response.statusCode() != 200) {
                        message = "HTTP " + response.statusCode();
                    } else {
                        try {
                            ok = "UP".equals(mapper.readTree(response.body()).path("status").asText());
                            if (!ok) {
                                message = "status not UP";
                            }
                        } catch (Exception e) {
                            message = "unreadable health body";
                        }
                    }
                    state.record(ok, message, clock.epochMillis(),
                            properties.healthFailThreshold(), properties.healthSuccessThreshold());
                    return (Void) null;
                });
    }

    public Status realisation(String realisation) {
        final Set<String> urls = new LinkedHashSet<>(table.current().backends().get(realisation).values());
        boolean unknown = false;
        for (final String url : urls) {
            final Status s = byUrl.get(url).status();
            if (s == DOWN) {
                return DOWN;
            }
            unknown |= s != UP;
        }
        return unknown ? UNKNOWN : UP;
    }

    public Map<String, Object> report() {
        final Map<String, Object> out = new LinkedHashMap<>();
        final RoutingSnapshot snapshot = table.current();
        for (final String realisation : snapshot.realisations()) {
            final Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("status", realisation(realisation).name());
            entry.put("enabled", snapshot.isEnabled(realisation));
            final Map<String, Object> processes = new LinkedHashMap<>();
            for (final Map.Entry<String, String> e : snapshot.backends().get(realisation).entrySet()) {
                final HealthState.View v = byUrl.get(e.getValue()).view();
                final Map<String, Object> p = new LinkedHashMap<>();
                p.put("url", e.getValue());
                p.put("status", v.status().name());
                p.put("consecutive_failures", v.consecutiveFailures());
                p.put("consecutive_successes", v.consecutiveSuccesses());
                p.put("last_check_epoch_ms", v.lastCheckEpochMs());
                p.put("last_change_epoch_ms", v.lastChangeEpochMs());
                p.put("last_error", v.lastError());
                processes.put(e.getKey(), p);
            }
            entry.put("processes", processes);
            out.put(realisation, entry);
        }
        return out;
    }
}
