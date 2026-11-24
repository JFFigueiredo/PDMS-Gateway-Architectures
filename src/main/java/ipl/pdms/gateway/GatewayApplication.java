package ipl.pdms.gateway;

import static org.springframework.boot.SpringApplication.run;

import ipl.pdms.gateway.config.GatewayProperties;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.config.HttpClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayApplication {

    /** Disable Netty retries to avoid repeating writes and hiding failures. */
    @Bean
    HttpClientCustomizer noHiddenRetry() {
        return client -> client.disableRetry(true);
    }

    public static void main(String[] args) {
        run(GatewayApplication.class, args);
    }
}
