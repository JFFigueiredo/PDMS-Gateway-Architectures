package ipl.pdms.gateway.config;

import static java.time.Duration.ofSeconds;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "pdms.gateway")
public record GatewayProperties(
        @DefaultValue("config/routing.json") String routingFile,
        @DefaultValue("adhoc") String runId,
        @DefaultValue("") String instanceId,
        @DefaultValue("logs") String logDir,
        @DefaultValue("") String adminToken,
        @DefaultValue("65536") int logQueueCapacity,
        @DefaultValue("200000") int statsCapacity,
        @DefaultValue("2s") Duration healthInterval,
        @DefaultValue("1s") Duration healthTimeout,
        @DefaultValue("3") int healthFailThreshold,
        @DefaultValue("2") int healthSuccessThreshold,
        @DefaultValue("200") int admissionMaxInFlight,
        @DefaultValue("200") int admissionQueue,
        @DefaultValue("2s") Duration admissionMaxWait) {

    public static GatewayProperties forTest(String routingFile, int statsCapacity) {
        return new GatewayProperties(routingFile, "t", "i", "logs", "", 16, statsCapacity,
                ofSeconds(2), ofSeconds(1), 3, 2, 200, 200, ofSeconds(2));
    }
}
