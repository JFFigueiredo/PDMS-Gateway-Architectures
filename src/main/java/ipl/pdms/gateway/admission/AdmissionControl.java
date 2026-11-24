package ipl.pdms.gateway.admission;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static reactor.core.scheduler.Schedulers.parallel;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.routing.RoutingTable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class AdmissionControl {

    private static final Duration[] BUCKETS = {
            ofMillis(1), ofMillis(5), ofMillis(20), ofMillis(100),
            ofMillis(500), ofSeconds(1), ofSeconds(2), ofSeconds(5)};

    private final Map<String, AdmissionLimiter> limiters = new ConcurrentHashMap<>();
    private final GatewayProperties properties;
    private final GatewayClock clock;
    private final MeterRegistry registry;

    public AdmissionControl(GatewayProperties properties, GatewayClock clock, MeterRegistry registry,
                            RoutingTable table) {
        this.properties = properties;
        this.clock = clock;
        this.registry = registry;
        table.current().backends().values().forEach(urls -> urls.values().forEach(this::limiter));
    }

    public AdmissionLimiter limiter(String backend) {
        return limiters.computeIfAbsent(backend, url -> {
            final AdmissionLimiter limiter = new AdmissionLimiter(properties.admissionMaxInFlight(),
                    properties.admissionQueue(), properties.admissionMaxWait(), parallel(), clock);
            Gauge.builder("pdms_dyn_admission_inflight", limiter, AdmissionLimiter::inFlight)
                    .description("Requests being forwarded to the backend process").tag("backend", url)
                    .register(registry);
            Gauge.builder("pdms_dyn_admission_queued", limiter, AdmissionLimiter::queued)
                    .description("Requests waiting for admission to the backend process").tag("backend", url)
                    .register(registry);
            return limiter;
        });
    }

    public void admitted(String backend, long waitedNanos) {
        Timer.builder("pdms_dyn_admission_wait").serviceLevelObjectives(BUCKETS).tag("backend", backend)
                .register(registry).record(ofNanos(waitedNanos));
    }

    public void rejected(String backend, String reason) {
        Counter.builder("pdms_dyn_admission_rejected").tag("backend", backend).tag("reason", reason)
                .register(registry).increment();
    }
}
