package ipl.pdms.gateway.config;

import static java.lang.System.currentTimeMillis;

import org.springframework.stereotype.Component;

@Component
public class GatewayClock {
    public long epochMillis() {
        return currentTimeMillis();
    }

    public long nanoTime() {
        return System.nanoTime();
    }
}
