package ipl.pdms.gateway.experiment;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static reactor.core.publisher.Mono.defer;
import static reactor.core.publisher.Mono.delay;
import static reactor.core.publisher.Mono.error;
import static reactor.core.scheduler.Schedulers.parallel;

import com.fasterxml.jackson.databind.JsonNode;
import ipl.pdms.gateway.admission.AdmissionLimiter;
import ipl.pdms.gateway.admission.AdmissionRejectedException;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import java.net.ConnectException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class Stimulus {
    public static final String EVIDENCE = Stimulus.class.getName() + ".evidence";
    private record Active(String indicator, String target, long delayMs,
                          AdmissionLimiter limiter, long activatedAtEpochMs) {}
    private final GatewayClock clock;
    private final TrafficStatistics traffic;
    private final AtomicLong affectedRequests = new AtomicLong(), failedProbes = new AtomicLong(), firstAffectedAt = new AtomicLong();
    private volatile Active active = new Active("", "", 0, null, 0);

    public boolean applyCase(String indicator, JsonNode settings, long elapsedMs, int windowSeconds) {
        if (elapsedMs < 0) {
            return clear();
        }
        return switch (indicator) {
            case "T1" -> delayRequests("T1", settings);
            case "T2" -> limitCapacity(settings);
            case "T3" -> failRequestsAndProbes(settings);
            case "T4" -> introduceRequirement("T4", settings);
            case "T5" -> introduceRequirement("T5", settings);
            case "T6" -> pauseTelemetryAndDelay(settings, windowSeconds);
            case "T7" -> pulseDelay(settings, elapsedMs);
            default -> throw new IllegalArgumentException("Unknown stimulus case: " + indicator);
        };
    }
    private boolean delayRequests(String indicator, JsonNode settings) {
        return activate(indicator, settings, settings.path("delayMs").asLong());
    }
    private boolean limitCapacity(JsonNode settings) {
        return activate("T2", settings, settings.path("delayMs").asLong());
    }
    private boolean failRequestsAndProbes(JsonNode settings) {
        return activate("T3", settings, 0);
    }
    private boolean introduceRequirement(String indicator, JsonNode settings) {
        return activate(indicator, settings, 0);
    }
    private boolean pauseTelemetryAndDelay(JsonNode settings, int windowSeconds) {
        traffic.pausePublication(windowSeconds);
        return delayRequests("T6", settings);
    }
    private boolean pulseDelay(JsonNode settings, long offsetMs) {
        final long pulse = settings.path("pulseMs").asLong(), second = settings.path("secondPulseAtMs").asLong();
        final boolean shortPulse = offsetMs < pulse || offsetMs >= second && offsetMs < second + pulse;
        final boolean sustained = offsetMs >= settings.path("sustainedAtMs").asLong();
        return shortPulse || sustained ? delayRequests("T7", settings) : clear();
    }
    private synchronized boolean activate(String indicator, JsonNode settings, long delayMs) {
        final String target = settings.path("startingArchitecture").asText();
        if (active.indicator().equals(indicator) && active.target().equals(target) && active.delayMs() == delayMs) {
            return false;
        }
        final AdmissionLimiter limiter = indicator.equals("T2") ? new AdmissionLimiter(settings.path("maxInFlight").asInt(),
                settings.path("queueCapacity").asInt(), ofMillis(settings.path("maxWaitMs").asLong()), parallel(), clock) : null;
        active = new Active(indicator, target, delayMs, limiter, clock.epochMillis());
        return true;
    }
    public synchronized boolean clear() {
        traffic.resumePublication();
        if (active.indicator().isEmpty()) {
            return false;
        }
        active = new Active("", "", 0, null, 0);
        return true;
    }
    public Set<String> requiredCapabilities() {
        return switch (active.indicator()) {
            case "T4" -> Set.of("consent_integrity_anchor");
            case "T5" -> Set.of("read_your_writes_snapshot");
            default -> Set.of();
        };
    }

    public Mono<Void> forward(String architecture, boolean probe, String requestId,
                              Map<String, Object> attributes, Supplier<Mono<Void>> next) {
        return defer(() -> {
            final Active effect = active;
            if (probe || !effect.target().equals(architecture) || Set.of("T4", "T5").contains(effect.indicator())) {
                return next.get();
            }
            affectedRequests.incrementAndGet(); firstAffectedAt.compareAndSet(0, clock.epochMillis());
            final Map<String, Object> evidence = evidence(effect, requestId);
            attributes.put(EVIDENCE, evidence);
            return effect.indicator().equals("T3") ? error(new ConnectException("Injected upstream unavailability"))
                    : delayed(effect, evidence, next);
        });
    }
    private Mono<Void> delayed(Active effect, Map<String, Object> evidence, Supplier<Mono<Void>> next) {
        final long started = clock.nanoTime();
        final AdmissionLimiter.Ticket ticket = effect.limiter() == null ? null : effect.limiter().newTicket();
        final Mono<Long> admission = ticket == null ? Mono.just(0L) : effect.limiter().acquire(ticket);
        return admission.then(delay(ofMillis(effect.delayMs()))).then(defer(() -> {
            recordWait(evidence, started);
            return next.get();
        })).doOnCancel(() -> recordWait(evidence, started))
                .doOnError(failure -> recordWait(evidence, started))
                .doFinally(signal -> {
                    if (ticket != null) {
                        effect.limiter().release(ticket);
                    }
                }).onErrorMap(AdmissionRejectedException.class,
                        failure -> new ResponseStatusException(SERVICE_UNAVAILABLE, "Injected capacity queue exhausted", failure));
    }
    private void recordWait(Map<String, Object> evidence, long started) {
        evidence.putIfAbsent("injected_wait_ms", (clock.nanoTime() - started) / 1_000_000.0);
    }
    public <T> CompletableFuture<T> probe(String url, Supplier<CompletableFuture<T>> next, Set<String> targetUrls) {
        if (active.indicator().equals("T3") && targetUrls.contains(url)) {
            failedProbes.incrementAndGet(); firstAffectedAt.compareAndSet(0, clock.epochMillis());
            return failedFuture(new ConnectException("Injected probe unavailability"));
        }
        return next.get();
    }
    private Map<String, Object> evidence(Active effect, String requestId) {
        return new java.util.concurrent.ConcurrentHashMap<>(Map.of("indicator", effect.indicator(),
                "target", effect.target(), "request_id", requestId, "activated_at_epoch_ms", effect.activatedAtEpochMs(),
                "affected_at_epoch_ms", clock.epochMillis(), "configured_delay_ms", effect.delayMs()));
    }
    public String target() {
        return active.target();
    }
    public Map<String, Object> status() {
        final Active effect = active;
        return Map.of("indicator", effect.indicator(), "target", effect.target(),
                "activatedAtEpochMs", effect.activatedAtEpochMs(), "affectedRequests", affectedRequests.get(),
                "failedProbes", failedProbes.get(), "firstAffectedAtEpochMs", firstAffectedAt.get(),
                "inFlight", effect.limiter() == null ? 0 : effect.limiter().inFlight(),
                "queued", effect.limiter() == null ? 0 : effect.limiter().queued());
    }

    public static void validateCase(String indicator, JsonNode settings) {
        switch (indicator) {
            case "T1", "T6" -> number(settings, "delayMs", 1, 60000);
            case "T2" -> {
                number(settings, "delayMs", 1, 60000); number(settings, "maxInFlight", 1, 1000);
                number(settings, "queueCapacity", 0, 100000); number(settings, "maxWaitMs", 1, 3600000);
            }
            case "T3", "T4", "T5" -> { }
            case "T7" -> validatePulses(settings);
            default -> throw new IllegalArgumentException("Unknown stimulus case: " + indicator);
        }
    }
    private static void validatePulses(JsonNode settings) {
        number(settings, "delayMs", 1, 60000);
        final long pulse = number(settings, "pulseMs", 1, Integer.MAX_VALUE);
        final long second = number(settings, "secondPulseAtMs", pulse, Integer.MAX_VALUE);
        number(settings, "sustainedAtMs", second + pulse, Integer.MAX_VALUE);
    }
    private static long number(JsonNode settings, String field, long min, long max) {
        final JsonNode value = settings.path(field);
        if (!value.isInt() || value.asLong() < min || value.asLong() > max) {
            throw new IllegalArgumentException("Invalid stimulus parameter: " + field);
        }
        return value.asLong();
    }
}
