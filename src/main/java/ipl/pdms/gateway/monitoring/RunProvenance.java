package ipl.pdms.gateway.monitoring;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;

import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.routing.RoutingTable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class RunProvenance {

    private final Map<String, Object> values = new LinkedHashMap<>();
    private final RequestLog log;

    public RunProvenance(RequestLog log, GatewayProperties properties, RoutingTable table, GatewayClock clock,
                         Environment environment) {
        this.log = log;
        values.put("gateway_version", env("PDMS_GATEWAY_VERSION", "unknown"));
        values.put("revision", env("PDMS_GATEWAY_REVISION", "unknown"));
        values.put("image_digest", env("PDMS_GATEWAY_IMAGE_DIGEST", "unset"));
        values.put("boot_epoch_ms", clock.epochMillis());
        values.put("config_hash", table.configHash());
        values.put("java_version", getProperty("java.version"));
        values.put("java_vm", getProperty("java.vm.name") + " " + getProperty("java.vm.version"));
        values.put("jvm_input_arguments", getRuntimeMXBean().getInputArguments());
        values.put("available_processors", getRuntime().availableProcessors());
        values.put("max_heap_bytes", getRuntime().maxMemory());
        values.put("time_zone", getProperty("user.timezone"));
        values.put("os", getProperty("os.name") + " " + getProperty("os.version"));
        values.put("admission_max_in_flight", properties.admissionMaxInFlight());
        values.put("admission_queue", properties.admissionQueue());
        values.put("admission_max_wait_ms", properties.admissionMaxWait().toMillis());
        values.put("health_interval_ms", properties.healthInterval().toMillis());
        values.put("health_timeout_ms", properties.healthTimeout().toMillis());
        values.put("health_fail_threshold", properties.healthFailThreshold());
        values.put("health_success_threshold", properties.healthSuccessThreshold());
        final String client = "spring.cloud.gateway.server.webflux.httpclient.";
        values.put("connect_timeout_ms", environment.getProperty(client + "connect-timeout"));
        values.put("response_timeout", environment.getProperty(client + "response-timeout"));
        values.put("pool_type", environment.getProperty(client + "pool.type"));
        values.put("pool_max_connections", environment.getProperty(client + "pool.max-connections"));
        values.put("pool_max_idle_time", environment.getProperty(client + "pool.max-idle-time"));
        values.put("log_queue_capacity", properties.logQueueCapacity());
        values.put("stats_capacity", properties.statsCapacity());
    }

    public Map<String, Object> values() {
        return values;
    }

    @EventListener(ApplicationReadyEvent.class)
    void writeRunStart() {
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("event", "run_start");
        row.putAll(values);
        log.submit(row);
    }

    private static String env(String name, String fallback) {
        final String value = getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
