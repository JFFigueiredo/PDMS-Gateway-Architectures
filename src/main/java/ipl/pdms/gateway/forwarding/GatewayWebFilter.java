package ipl.pdms.gateway.forwarding;

import static ipl.pdms.gateway.admission.HandoverQueue.DISABLED;
import static ipl.pdms.gateway.forwarding.RequestContext.ATTRIBUTE;
import static ipl.pdms.gateway.routing.Endpoint.Kind.PLATFORM;
import static ipl.pdms.gateway.routing.RouteMatcher.decide;
import static java.lang.Boolean.TRUE;
import static java.lang.String.join;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static reactor.core.publisher.Mono.empty;
import static reactor.core.publisher.SignalType.CANCEL;

import ipl.pdms.gateway.admission.AdmissionControl;
import ipl.pdms.gateway.admission.AdmissionLimiter;
import ipl.pdms.gateway.admission.AdmissionRejectedException;
import ipl.pdms.gateway.admission.HandoverQueue;
import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.experiment.Stimulus;
import ipl.pdms.gateway.monitoring.GatewayMetrics;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RouteMatcher;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

@Component
@Order(HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class GatewayWebFilter implements WebFilter {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayWebFilter.class);
    private static final Set<String> PLANS = Set.of("mix", "read", "write");
    private static final Set<String> LOCAL_PREFIXES = Set.of("/admin", "/internal", "/gateway");
    private static final String MEASURED_WORKLOAD = GatewayWebFilter.class.getName() + ".measuredWorkload";
    private static final String CANCELLED = GatewayWebFilter.class.getName() + ".cancelled";
    private static final String RECEIVED_ROUTE = GatewayWebFilter.class.getName() + ".receivedRoute";

    private final RoutingTable table;
    private final TrafficStatistics stats;
    private final RequestLog log;
    private final GatewayMetrics metrics;
    private final GatewayClock clock;
    private final AdminAuth auth;
    private final AdmissionControl admission;
    private final GatewayProperties properties;
    private final RuntimeCoordinator runtime;
    private final HandoverQueue handovers;


    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        final ServerHttpRequest request = exchange.getRequest();
        final String rawPath = request.getURI().getRawPath();
        if (isLocal(rawPath)) {
            return chain.filter(exchange);
        }
        final long acceptMs = clock.epochMillis();
        final long acceptNs = clock.nanoTime();
        final String method = request.getMethod().name();

        final RouteMatcher.Decision decision = decide(method, rawPath);
        if (!decision.routed()) {
            if (decision.status() == 405) {
                exchange.getResponse().getHeaders().set("Allow", join(", ", decision.allow()));
            }
            return reject(exchange, decision.status(),
                    decision.status() == 405 ? "method_not_allowed" : "unknown_path",
                    method, rawPath, acceptMs, acceptNs);
        }

        return validateAndForward(exchange, chain, decision, acceptMs, acceptNs);
    }

    private Mono<Void> validateAndForward(ServerWebExchange exchange, WebFilterChain chain, RouteMatcher.Decision decision,
                                          long acceptMs, long acceptNs) {
        final var request = exchange.getRequest();
        final var rawPath = request.getURI().getRawPath();
        final var method = request.getMethod().name();
        // Platform endpoints bypass identity checks and workload statistics.
        final boolean platform = decision.match().kind() == PLATFORM;
        final boolean probeHeader = "1".equals(header(request, "X-Probe"));
        final String probeTarget = header(request, "X-Probe-Target");
        if ((probeHeader || probeTarget != null) && !auth.raw(header(request, "X-Probe-Token"))) {
            return reject(exchange, 401, "probe_unauthorised", method, rawPath, acceptMs, acceptNs);
        }
        if (probeTarget != null && !probeHeader) {
            return reject(exchange, 400, "probe_target_without_probe", method, rawPath, acceptMs, acceptNs);
        }
        final boolean probe = platform || probeHeader;
        final String runId = header(request, "X-Run-Id");
        String planId = header(request, "X-Plan-Id");
        final String sessionId = header(request, "X-Session-Id");
        final String requestId = header(request, "X-Request-Id");
        if (!probe && (blank(runId) || blank(sessionId) || blank(requestId) || !PLANS.contains(planId))) {
            return reject(exchange, 400, "missing_or_invalid_identity", method, rawPath, acceptMs, acceptNs);
        }
        if (!probe && !"adhoc".equals(properties.runId()) && !properties.runId().equals(runId)) {
            return reject(exchange, 409, "run_id_mismatch", method, rawPath, acceptMs, acceptNs);
        }
        if (probe && blank(planId)) {
            planId = platform ? "platform" : "probe";
        }

        exchange.getAttributes().put(RECEIVED_ROUTE, table.current());
        if (!probe) {
            stats.arrival(planId);
            exchange.getAttributes().put(MEASURED_WORKLOAD, true);
            metrics.offered(planId);
        }
        final String measuredPlan = planId;
        if (probe) {
            return route(exchange, chain, decision, true, measuredPlan, acceptMs, acceptNs, 0);
        }
        return handovers.forward(waited -> route(exchange, chain, decision, false,
                        measuredPlan, acceptMs, acceptNs, waited), () -> runtime.phase().equals("transitioning")
                        || runtime.allows(table.current().active()))
                .onErrorResume(HandoverQueue.Rejected.class, error -> reject(exchange, 503, error.getMessage(),
                        method, rawPath, acceptMs, acceptNs))
                .doOnCancel(() -> {
                    synchronized (exchange) {
                        exchange.getAttributes().put(CANCELLED, true);
                        if (exchange.getAttribute(ATTRIBUTE) == null
                                && TRUE.equals(exchange.getAttribute(MEASURED_WORKLOAD))) {
                            reject(exchange, 499, "client_abort", method, rawPath, acceptMs, acceptNs);
                        }
                    }
                });
    }

    /** Re-read routing after each wait; queued requests must not hold runtime permits. */
    private Mono<Void> route(ServerWebExchange exchange, WebFilterChain chain, RouteMatcher.Decision decision,
                             boolean probe, String planId, long acceptMs, long acceptNs, long handoverWait) {
        final ServerHttpRequest request = exchange.getRequest();
        final String rawPath = request.getURI().getRawPath();
        final String method = request.getMethod().name();
        final String runId = header(request, "X-Run-Id");
        final String sessionId = header(request, "X-Session-Id");
        final String requestId = header(request, "X-Request-Id");
        final String probeTarget = header(request, "X-Probe-Target");
        final boolean platform = decision.match().kind() == PLATFORM;
        final RoutingSnapshot snapshot = table.current();
        if (TRUE.equals(exchange.getAttribute(CANCELLED))) {
            return empty();
        }
        if (probe && probeTarget != null && !snapshot.knows(probeTarget)) {
            return reject(exchange, 400, "unknown_probe_target", method, rawPath, acceptMs, acceptNs);
        }

        AtomicInteger inFlight = null;
        boolean runtimeAdmitted = false;
        try {
            final String realisation = probe && probeTarget != null ? probeTarget : snapshot.active();
            if (!platform) {
                if (!runtime.enter(realisation)) {
                    return probe ? reject(exchange, 503, DISABLED, method, rawPath, acceptMs, acceptNs) : null;
                }
                runtimeAdmitted = true;
                // The epoch detects a cutover away and back while this request was waiting.
                if (!probe && table.current().epoch() != snapshot.epoch()) {
                    runtime.exit();
                    return null;
                }
            }
            final String base = snapshot.baseUrl(realisation, decision.match().service());
            final String rawQuery = request.getURI().getRawQuery();
            final URI target = URI.create(base + rawPath + (rawQuery != null ? "?" + rawQuery : ""));

            final RoutingSnapshot received = exchange.getAttribute(RECEIVED_ROUTE);
            final RequestContext ctx = new RequestContext(runId, planId, sessionId, requestId, method, rawPath,
                    decision.match().operation(), decision.match().service(), decision.match().kind(), realisation,
                    received.active(), snapshot.epoch(), probe, acceptMs, acceptNs, target);
            ctx.runtimeAdmitted = runtimeAdmitted;
            ctx.handoverWaitNanos = handoverWait;
            ctx.routeMs = clock.epochMillis();
            ctx.activeAtRoute = snapshot.active();
            ctx.receivedEpoch = received.epoch();
            // Cancellation before context publication has already logged the final outcome.
            synchronized (exchange) {
                if (TRUE.equals(exchange.getAttribute(CANCELLED))) {
                    if (runtimeAdmitted) {
                        runtime.exit();
                    }
                    return empty();
                }
                exchange.getAttributes().put(ATTRIBUTE, ctx);
            }

            metrics.arrival(realisation, planId, probe);
            inFlight = metrics.inFlightFor(realisation);
            inFlight.incrementAndGet();
            final AtomicInteger counter = inFlight;

            Mono<Void> forward;
            if (probe) {
                forward = chain.filter(exchange);
            } else {
                final AdmissionLimiter limiter = admission.limiter(base);
                final AdmissionLimiter.Ticket ticket = limiter.newTicket();
                forward = limiter.acquire(ticket)
                        .flatMap(waited -> {
                            ctx.admissionWaitNanos = waited;
                            admission.admitted(base, waited);
                            return chain.filter(exchange);
                        })
                        .onErrorResume(AdmissionRejectedException.class, e -> {
                            ctx.admissionWaitNanos = e.waitedNanos();
                            admission.rejected(base, e.reason());
                            return generate(exchange, ctx, SERVICE_UNAVAILABLE, e.reason());
                        })
                        .doFinally(signal -> limiter.release(ticket));
            }
            return forward
                    .onErrorResume(e -> !(e instanceof AdmissionRejectedException), e -> failed(exchange, ctx, e))
                    .doFinally(signal -> complete(exchange, ctx, signal, counter));
        } catch (RuntimeException e) {
            if (runtimeAdmitted) {
                runtime.exit();
            }
            if (inFlight != null) {
                inFlight.decrementAndGet();
            }
            if (TRUE.equals(exchange.getAttribute(CANCELLED))) {
                return empty();
            }
            LOG.warn("request setup failed ({}): {}", e.getClass().getSimpleName(), e.getMessage());
            return reject(exchange, 500, "gateway_internal", method, rawPath, acceptMs, acceptNs);
        }
    }

    private Mono<Void> generate(ServerWebExchange exchange, RequestContext ctx, HttpStatus status, String reason) {
        ctx.gatewayError = reason;
        ctx.statusGenerated = true;
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().set("X-Gateway-Error", reason);
        return exchange.getResponse().setComplete();
    }

    /** After response commit, propagate errors to reset the connection rather than complete a truncated body. */
    private Mono<Void> failed(ServerWebExchange exchange, RequestContext ctx, Throwable error) {
        final GatewayFailure failure = GatewayFailure.of(error);
        ctx.errored = true;
        LOG.warn("forwarding failed: {} {} -> {} ({}: {})", ctx.method, ctx.operation, failure.reason(),
                error.getClass().getSimpleName(), error.getMessage());
        if (exchange.getResponse().isCommitted()) {
            ctx.gatewayError = failure.reason();
            return Mono.error(error);
        }
        return generate(exchange, ctx, failure.status(), failure.reason());
    }

    private void complete(ServerWebExchange exchange, RequestContext ctx, SignalType signal,
                          AtomicInteger inFlight) {
        final long endNs = clock.nanoTime();
        final long endMs = clock.epochMillis();
        final long durationNs = endNs - ctx.acceptNs;
        inFlight.decrementAndGet();
        if (ctx.runtimeAdmitted) {
            runtime.exit();
        }
        final boolean clientAbort = signal == CANCEL;
        final HttpStatusCode code = exchange.getResponse().getStatusCode();
        final int status = code == null ? 0 : code.value();
        final boolean generated = ctx.statusGenerated;
        final String statusClass = generated ? statusClass(status)
                : (clientAbort || ctx.errored || status == 0 ? "transport" : statusClass(status));
        final boolean error = !"2xx".equals(statusClass) && !"3xx".equals(statusClass);
        final boolean gatewayFault = generated || ctx.errored;
        final String origin = clientAbort ? "client" : (gatewayFault ? "gateway" : "upstream");
        String errorClass;
        if (clientAbort) {
            errorClass = "client_abort";
        } else if (ctx.gatewayError != null) {
            errorClass = ctx.gatewayError;
        } else if (ctx.errored) {
            errorClass = "transport";
        } else if (status == 429) {
            errorClass = "upstream_429";
        } else {
            errorClass = error ? statusClass : "none";
        }

        completeMeasurement(exchange, durationNs, error);
        metrics.completion(ctx.realisation, ctx.service.key(), ctx.operation, statusClass, ctx.probe, durationNs);
        if (error) {
            metrics.error(ctx.realisation, ctx.operation, origin, errorClass, ctx.probe);
        }
        if (ctx.upstreamHeadersReceived && ctx.upstreamNanos >= 0) {
            metrics.upstreamHeaders(ctx.realisation, ctx.operation, ctx.probe, ctx.upstreamNanos);
        }

        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("attempt", 1);
        row.put("external_final", true);
        row.put("ts_accept_epoch_ms", ctx.acceptMs);
        row.put("ts_end_epoch_ms", endMs);
        row.put("ts_route_epoch_ms", ctx.routeMs);
        row.put("accept_mono_ns", ctx.acceptNs);
        row.put("end_mono_ns", endNs);
        row.put("client_run_id", ctx.runId);
        row.put("plan_id", ctx.planId);
        row.put("session_id", ctx.sessionId);
        row.put("request_id", ctx.requestId);
        row.put("method", ctx.method);
        row.put("path_hash", pathHash(ctx.rawPath));
        row.put("operation", ctx.operation);
        row.put("service", ctx.service.key());
        row.put("endpoint_kind", ctx.kind.key());
        row.put("realisation", ctx.realisation);
        row.put("active_at_accept", ctx.activeAtAccept);
        row.put("active_at_route", ctx.activeAtRoute);
        row.put("routing_epoch_at_accept", ctx.receivedEpoch);
        row.put("epoch", ctx.epoch);
        if (exchange.getAttribute(Stimulus.EVIDENCE) != null) {
            row.put("stimulus", exchange.getAttribute(Stimulus.EVIDENCE));
        }
        row.put("routing_mode", "request");
        row.put("status", status);
        row.put("status_class", statusClass);
        row.put("error_class", errorClass);
        row.put("error_origin", origin);
        row.put("gateway_duration_ms", durationNs / 1_000_000.0);
        row.put("upstream_headers_ms", ctx.upstreamNanos < 0 ? null : ctx.upstreamNanos / 1_000_000.0);
        row.put("admission_wait_ms", ctx.admissionWaitNanos < 0 ? null : ctx.admissionWaitNanos / 1_000_000.0);
        row.put("handover_wait_ms", ctx.handoverWaitNanos / 1_000_000.0);
        row.put("probe", ctx.probe);
        row.put("client_abort", clientAbort);
        log.submit(row);
    }

    private Mono<Void> reject(ServerWebExchange exchange, int status, String reason, String method,
                              String rawPath, long acceptMs, long acceptNs) {
        final long endNs = clock.nanoTime();
        final long endMs = clock.epochMillis();
        exchange.getResponse().setStatusCode(HttpStatusCode.valueOf(status));
        exchange.getResponse().getHeaders().set("X-Gateway-Reject", reason);
        completeMeasurement(exchange, endNs - acceptNs, true);
        metrics.completion("none", "none", "gateway_reject", statusClass(status), false, endNs - acceptNs);
        metrics.error("none", "gateway_reject", status == 499 ? "client" : "gateway", reason, false);

        final ServerHttpRequest request = exchange.getRequest();
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("attempt", 0);
        row.put("external_final", true);
        row.put("ts_accept_epoch_ms", acceptMs);
        row.put("ts_end_epoch_ms", endMs);
        row.put("accept_mono_ns", acceptNs);
        row.put("end_mono_ns", endNs);
        row.put("client_run_id", header(request, "X-Run-Id"));
        row.put("plan_id", header(request, "X-Plan-Id"));
        row.put("session_id", header(request, "X-Session-Id"));
        row.put("request_id", header(request, "X-Request-Id"));
        row.put("method", method);
        row.put("path_hash", pathHash(rawPath));
        row.put("operation", "gateway_reject");
        row.put("realisation", "none");
        row.put("status", status);
        row.put("status_class", statusClass(status));
        row.put("error_class", reason);
        row.put("error_origin", status == 499 ? "client" : "gateway");
        row.put("client_abort", status == 499);
        row.put("gateway_duration_ms", (endNs - acceptNs) / 1_000_000.0);
        row.put("upstream_headers_ms", null);
        log.submit(row);
        return exchange.getResponse().setComplete();
    }

    private void completeMeasurement(ServerWebExchange exchange, long durationNs, boolean error) {
        if (TRUE.equals(exchange.getAttributes().remove(MEASURED_WORKLOAD))) {
            stats.completion(durationNs / 1_000_000.0, error);
        }
    }

    static String statusClass(int status) {
        if (status >= 200 && status < 300) {
            return "2xx";
        }
        if (status >= 300 && status < 400) {
            return "3xx";
        }
        if (status >= 400 && status < 500) {
            return "4xx";
        }
        return status >= 500 ? "5xx" : "transport";
    }

    private static boolean isLocal(String rawPath) {
        for (final String prefix : LOCAL_PREFIXES) {
            if (rawPath.equals(prefix) || rawPath.startsWith(prefix + "/")) {
                return true;
            }
        }
        return false;
    }

    private static String header(ServerHttpRequest request, String name) {
        return request.getHeaders().getFirst(name);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static String pathHash(String rawPath) {
        return sha256Hex(rawPath).substring(0, 16);
    }
}
