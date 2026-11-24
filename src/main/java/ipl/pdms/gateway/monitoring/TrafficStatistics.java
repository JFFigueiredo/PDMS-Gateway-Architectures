package ipl.pdms.gateway.monitoring;

import static org.apache.commons.statistics.descriptive.Quantile.EstimationMethod.HF7;
import static org.apache.commons.statistics.descriptive.Quantile.withDefaults;

import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.config.GatewayProperties;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.statistics.descriptive.Quantile;
import org.springframework.stereotype.Component;

/** Arrival windows use accept time; completion windows use end time. Overflow invalidates the window. */
@Component
public class TrafficStatistics {

    private static final long RETENTION_MS = 120_000L;
    private static final Quantile QUANTILES = withDefaults().with(HF7);

    private record Arrival(long acceptMs, String plan) {
    }

    private record Completion(long endMs, double durationMs, boolean error) {
    }

    private final ArrayDeque<Arrival> arrivals = new ArrayDeque<>();
    private final ArrayDeque<Completion> completions = new ArrayDeque<>();
    private final AtomicInteger pending = new AtomicInteger();
    private final GatewayClock clock;
    private final int capacity;
    private long overflowCount;
    private long overflowAtMs = -1;
    private long lastEndMs = -1;
    private Map<String, Object> pausedPublication;

    public TrafficStatistics(GatewayClock clock, GatewayProperties properties) {
        this.clock = clock;
        this.capacity = properties.statsCapacity();
    }

    public void arrival(String plan) {
        final long now = clock.epochMillis();
        pending.incrementAndGet();
        synchronized (this) {
            evict(now);
            if (arrivals.size() >= capacity) {
                overflow(now);
            } else {
                arrivals.addLast(new Arrival(now, plan));
            }
        }
    }

    public void completion(double durationMs, boolean error) {
        final long now = clock.epochMillis();
        pending.decrementAndGet();
        synchronized (this) {
            evict(now);
            if (completions.size() >= capacity) {
                overflow(now);
            } else {
                completions.addLast(new Completion(now, durationMs, error));
                lastEndMs = now;
            }
        }
    }

    public long overflowCount() {
        synchronized (this) {
            return overflowCount;
        }
    }

    public Map<String, Object> snapshot(int windowSeconds) {
        final var now = clock.epochMillis();
        final var start = now - windowSeconds * 1000L;
        final Map<String, Object> out = new LinkedHashMap<>();
        final var durations = collectWindow(out, start, now);
        final var quantiles = QUANTILES.evaluate(durations.stream().mapToDouble(Double::doubleValue).toArray(), 0.50, 0.99);
        out.put("window_start_epoch_ms", start);
        out.put("window_end_epoch_ms", now);
        out.put("window_s", windowSeconds);
        out.put("n", durations.size());
        out.put("p50_ms", durations.isEmpty() ? null : quantiles[0]);
        out.put("p99_ms", durations.isEmpty() ? null : quantiles[1]);
        out.put("pending", pending.get());
        return out;
    }

    public synchronized Map<String, Object> publishedSnapshot(int windowSeconds) {
        return pausedPublication == null ? snapshot(windowSeconds) : new LinkedHashMap<>(pausedPublication);
    }

    public synchronized void pausePublication(int windowSeconds) {
        if (pausedPublication == null) {
            pausedPublication = snapshot(windowSeconds);
        }
    }

    public synchronized void resumePublication() {
        pausedPublication = null;
    }

    private synchronized List<Double> collectWindow(Map<String, Object> out, long start, long end) {
        final List<Double> durations = new ArrayList<>();
        final Map<String, Integer> plans = new TreeMap<>();
        int arrivalCount = 0;
        int errors = 0;
        for (final var arrival : arrivals) {
            if (arrival.acceptMs() >= start && arrival.acceptMs() < end) {
                arrivalCount++;
                plans.merge(arrival.plan(), 1, Integer::sum);
            }
        }
        for (final var completion : completions) {
            if (completion.endMs() >= start && completion.endMs() < end) {
                durations.add(completion.durationMs());
                if (completion.error()) {
                    errors++;
                }
            }
        }
        out.put("errors", errors);
        out.put("n_arrivals", arrivalCount);
        out.put("lambda_request_rps", arrivalCount / ((end - start) / 1000.0));
        out.put("plan_arrivals", plans);
        out.put("source_timestamp_epoch_ms", lastEndMs < 0 ? null : lastEndMs);
        out.put("age_ms", lastEndMs < 0 ? null : end - lastEndMs);
        out.put("valid", overflowAtMs < start);
        return durations;
    }

    private void evict(long now) {
        final long cutoff = now - RETENTION_MS;
        while (!arrivals.isEmpty() && arrivals.peekFirst().acceptMs() < cutoff) {
            arrivals.removeFirst();
        }
        while (!completions.isEmpty() && completions.peekFirst().endMs() < cutoff) {
            completions.removeFirst();
        }
    }

    private void overflow(long now) {
        overflowCount++;
        overflowAtMs = now;
    }
}
