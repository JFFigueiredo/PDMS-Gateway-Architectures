package ipl.pdms.gateway.monitoring;

import static java.lang.Math.max;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import ipl.pdms.gateway.routing.RoutingTable;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.cloud.gateway.config.HttpClientCustomizer;
import org.springframework.stereotype.Component;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;

/** Keep metric labels bounded; never use raw paths, client IDs or routing epochs. */
@Component
public class GatewayMetrics implements HttpClientCustomizer {

    private static final Duration[] BUCKETS = {
            ofMillis(1), ofMillis(2), ofMillis(5), ofMillis(10),
            ofMillis(20), ofMillis(50), ofMillis(100), ofMillis(200),
            ofMillis(250), ofMillis(500), ofSeconds(1), ofSeconds(2),
            ofSeconds(5), ofSeconds(10)};

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    private final Set<SingleThreadEventExecutor> observed = newKeySet();

    public GatewayMetrics(MeterRegistry registry, RoutingTable table, RequestLog log,
                          TrafficStatistics stats) {
        this.registry = registry;
        for (final String realisation : table.current().realisations()) {
            Gauge.builder("pdms_dyn_architecture_enabled", table, t -> t.current().isEnabled(realisation) ? 1 : 0)
                    .description("1 when the realisation is eligible for HTTP cutover")
                    .tag("realisation", realisation).register(registry);
            Gauge.builder("pdms_dyn_active", table, t -> t.current().active().equals(realisation) ? 1 : 0)
                    .description("1 when the realisation is active for business requests")
                    .tag("realisation", realisation).register(registry);
            inFlightFor(realisation);
        }
        Gauge.builder("pdms_dyn_epoch", table, t -> t.current().epoch()).register(registry);
        Gauge.builder("pdms_dyn_log_rows_dropped", log, l -> l.rowsDropped()).register(registry);
        Gauge.builder("pdms_dyn_log_queue_depth", log, RequestLog::queueDepth)
                .description("Log rows waiting for the writer; excludes its current write").register(registry);
        Gauge.builder("pdms_dyn_log_queue_capacity", log, RequestLog::queueCapacity).register(registry);
        Gauge.builder("pdms_dyn_log_writer_healthy", log, l -> l.writerHealthy() ? 1 : 0)
                .description("1 while the log writer is alive, open and has not failed").register(registry);
        FunctionCounter.builder("pdms_dyn_stats_overflow", stats, TrafficStatistics::overflowCount)
                .description("Arrival/completion samples refused by the bounded statistics ring")
                .register(registry);
        for (final String mode : new String[]{"request", "session"}) {
            Gauge.builder("pdms_dyn_routing_mode", table, t -> t.current().mode().equals(mode) ? 1 : 0)
                    .tag("mode", mode).register(registry);
        }
    }

    @Override
    public HttpClient customize(HttpClient client) {
        return client.doOnConnected(this::observeEventLoop);
    }

    private void observeEventLoop(Connection connection) {
        if (connection.channel().eventLoop() instanceof SingleThreadEventExecutor executor && observed.add(executor)) {
            Gauge.builder("pdms_dyn_event_loop_pending_tasks", executor, SingleThreadEventExecutor::pendingTasks)
                    .description("Queued tasks on an observed gateway HTTP-client event loop; excludes its running task")
                    .tag("event_loop", executor.threadProperties().name()).register(registry);
        }
    }

    public AtomicInteger inFlightFor(String realisation) {
        return inFlight.computeIfAbsent(realisation, r -> {
            final AtomicInteger value = new AtomicInteger();
            Gauge.builder("pdms_dyn_gateway_inflight", value, AtomicInteger::get)
                    .tag("realisation", r).register(registry);
            return value;
        });
    }

    public void arrival(String realisation, String plan, boolean probe) {
        Counter.builder("pdms_dyn_gateway_arrivals")
                .tag("realisation", realisation).tag("plan", plan).tag("probe", String.valueOf(probe))
                .register(registry).increment();
    }

    public void offered(String plan) {
        Counter.builder("pdms_dyn_gateway_offered")
                .description("Valid identified business requests offered before runtime admission")
                .tag("plan", plan).register(registry).increment();
    }

    public void completion(String realisation, String service, String operation, String statusClass,
                           boolean probe, long durationNanos) {
        Counter.builder("pdms_dyn_gateway_requests")
                .tag("realisation", realisation).tag("service", service).tag("operation", operation)
                .tag("status_class", statusClass).tag("probe", String.valueOf(probe))
                .register(registry).increment();
        Timer.builder("pdms_dyn_gateway_duration")
                .serviceLevelObjectives(BUCKETS)
                .tag("realisation", realisation).tag("operation", operation).tag("probe", String.valueOf(probe))
                .register(registry).record(ofNanos(durationNanos));
    }

    public void error(String realisation, String operation, String origin, String reason, boolean probe) {
        Counter.builder("pdms_dyn_gateway_errors")
                .tags("realisation", realisation, "operation", operation, "origin", origin,
                        "reason", reason, "probe", String.valueOf(probe))
                .register(registry).increment();
    }

    public void upstreamHeaders(String realisation, String operation, boolean probe, long durationNanos) {
        Timer.builder("pdms_dyn_upstream_headers_duration")
                .description("Forwarding step to upstream response headers, excluding response body")
                .serviceLevelObjectives(BUCKETS)
                .tags("realisation", realisation, "operation", operation, "probe", String.valueOf(probe))
                .register(registry).record(ofNanos(max(0, durationNanos)));
    }
}
