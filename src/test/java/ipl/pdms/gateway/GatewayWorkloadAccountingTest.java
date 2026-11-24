package ipl.pdms.gateway;

import static ipl.pdms.gateway.config.GatewayProperties.forTest;
import static java.lang.Thread.currentThread;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.mock.web.server.MockServerWebExchange.from;
import static reactor.core.publisher.Mono.empty;
import static reactor.core.publisher.Mono.error;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

import ipl.pdms.gateway.admission.AdmissionControl;
import ipl.pdms.gateway.admission.AdmissionLimiter;
import ipl.pdms.gateway.admission.HandoverQueue;
import ipl.pdms.gateway.config.AdminAuth;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.forwarding.GatewayWebFilter;
import ipl.pdms.gateway.monitoring.GatewayMetrics;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.routing.RoutingTable;
import ipl.pdms.gateway.runtime.RuntimeCoordinator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

class GatewayWorkloadAccountingTest {
    final FakeClock clock = new FakeClock(1_000_000);
    final GatewayProperties properties = forTest("unused", 1000);
    final TrafficStatistics stats = new TrafficStatistics(clock, properties);
    final RuntimeCoordinator runtime = mock(RuntimeCoordinator.class);
    final GatewayMetrics metrics = mock(GatewayMetrics.class);
    final RequestLog log = mock(RequestLog.class);
    final AdmissionControl admission = mock(AdmissionControl.class);
    final RoutingTable routing = mock(RoutingTable.class);
    GatewayWebFilter filter;

    @BeforeEach void setup() {
        when(routing.current()).thenReturn(new RoutingSnapshot("routing/1", 0, "monolith", "request",
                Map.of("monolith", Map.of("default", "http://unused")), Set.of()));
        when(metrics.inFlightFor("monolith")).thenReturn(new AtomicInteger());
        when(admission.limiter("http://unused")).thenReturn(new AdmissionLimiter(1, 0, ofSeconds(1),
                parallel(), clock));
        filter = new GatewayWebFilter(routing, stats, log, metrics, clock,
                mock(AdminAuth.class), admission, properties, runtime,
                new HandoverQueue(2, ofMillis(100), new io.micrometer.core.instrument.simple.SimpleMeterRegistry()));
        when(runtime.phase()).thenReturn("ready");
    }

    private MockServerWebExchange request(String path, String run) {
        return from(MockServerHttpRequest.get(path)
                .header("X-Run-Id", run).header("X-Plan-Id", "mix")
                .header("X-Session-Id", "session").header("X-Request-Id", "request"));
    }

    private Map<String, Object> snapshot() {
        clock.advanceMillis(1);
        return stats.snapshot(30);
    }

    @Test void handoverRefusalsAreOffersAndErrorCompletions() {
        for (int i = 0; i < 50; i++) {
            var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
            filter.filter(exchange, ignored -> error(new AssertionError("Must not forward"))).block();
            assertEquals(503, exchange.getResponse().getStatusCode().value());
            clock.advanceMillis(1);
        }
        var window = snapshot();
        assertEquals(50, window.get("n_arrivals")); assertEquals(50, window.get("n"));
        assertEquals(50, window.get("errors")); assertEquals(0, window.get("pending"));
        verify(metrics, times(50)).offered("mix");
        verify(metrics, never()).arrival(anyString(), anyString(), anyBoolean());
        verify(log, times(50)).submit(anyMap());
    }

    @Test void forwardedRequestHasExactlyOneArrivalAndCompletion() {
        when(runtime.enter("monolith")).thenReturn(true);
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        filter.filter(exchange, e -> {
            clock.advanceMillis(5); e.getResponse().setStatusCode(OK); return e.getResponse().setComplete();
        }).block();
        var window = snapshot();
        assertEquals(1, window.get("n_arrivals")); assertEquals(1, window.get("n"));
        assertEquals(0, window.get("errors")); assertEquals(0, window.get("pending"));
        verify(metrics).offered("mix"); verify(metrics).arrival("monolith", "mix", false);
        verify(runtime).exit();
    }

    @Test void setupFailureCompletesThePreviouslyCountedOffer() {
        when(runtime.enter("monolith")).thenReturn(true);
        when(metrics.inFlightFor("monolith")).thenThrow(new IllegalStateException("isolated setup failure"));
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        filter.filter(exchange, ignored -> empty()).block();
        assertEquals(500, exchange.getResponse().getStatusCode().value());
        var window = snapshot();
        assertEquals(1, window.get("n_arrivals")); assertEquals(1, window.get("errors"));
        assertEquals(0, window.get("pending")); verify(runtime).exit();
    }

    @Test void invalidIdentityAndPlatformTrafficStayOutsideWorkloadStatistics() {
        filter.filter(request("/api/person/11111111-1111-4111-8111-111111111111", "wrong-run"), ignored -> empty()).block();
        var platform = request("/actuator/health", "t");
        filter.filter(platform, e -> {
            e.getResponse().setStatusCode(OK);
            return e.getResponse().setComplete();
        }).block();
        var window = snapshot();
        assertEquals(0, window.get("n_arrivals")); assertEquals(0, window.get("n"));
        assertEquals(0, window.get("pending")); verify(metrics, never()).offered(anyString());
    }

    @Test void queuedRequestRefreshesRouteAndEpochAndCountsOneOffer() throws Exception {
        when(runtime.phase()).thenReturn("transitioning");
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        var pending = filter.filter(exchange, e -> {
            e.getResponse().setStatusCode(OK); return e.getResponse().setComplete();
        }).toFuture();
        assertFalse(pending.isDone()); assertEquals(1, snapshot().get("pending"));
        when(routing.current()).thenReturn(new RoutingSnapshot("routing/1", 2, "faas", "request",
                Map.of("faas", Map.of("default", "http://unused")), Set.of()));
        when(metrics.inFlightFor("faas")).thenReturn(new AtomicInteger());
        when(runtime.enter("faas")).thenReturn(true);
        when(runtime.phase()).thenReturn("ready");
        pending.get(2, SECONDS);
        // doFinally runs after the terminal signal; wait for accounting to finish.
        verify(runtime, timeout(1000)).exit();
        var window = snapshot();
        assertEquals(1, window.get("n_arrivals")); assertEquals(1, window.get("n"));
        assertEquals(0, window.get("errors")); assertEquals(0, window.get("pending"));
        verify(metrics).offered("mix");
        verify(log, timeout(1000)).submit(argThat(row -> "faas".equals(row.get("realisation"))
                && Long.valueOf(2).equals(row.get("epoch")) && ((Double) row.get("handover_wait_ms")) > 0));
    }

    @Test void cancelledWaitCompletesOfferWithoutEnteringRuntimeOrForwarding() {
        when(runtime.phase()).thenReturn("transitioning");
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        var pending = filter.filter(exchange, ignored -> error(new AssertionError("Must not forward"))).subscribe();
        assertEquals(1, snapshot().get("pending")); pending.dispose();
        var window = snapshot();
        assertEquals(1, window.get("n_arrivals")); assertEquals(1, window.get("n"));
        assertEquals(1, window.get("errors")); assertEquals(0, window.get("pending"));
        verify(runtime, never()).exit();
        verify(log).submit(argThat(row -> Integer.valueOf(499).equals(row.get("status"))
                && "client".equals(row.get("error_origin"))));
    }

    @Test void cancellationWhileAdmissionIsBeingGrantedReleasesPermitWithoutSecondOutcome() throws Exception {
        when(runtime.phase()).thenReturn("transitioning");
        var entered = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        when(runtime.enter("monolith")).thenAnswer(ignored -> {
            if (attempts.incrementAndGet() <= 2) {
                return false;
            }
            entered.countDown();
            // Hold this boundary until cancellation has finished.
            boolean interrupted = false;
            for (;;) {
                try {
                    if (!resume.await(2, SECONDS)) {
                        fail("Fixture timed out");
                    }
                    break;
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                currentThread().interrupt();
            }
            return true;
        });
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        AtomicInteger forwards = new AtomicInteger();
        var subscription = filter.filter(exchange, ignored -> fromRunnable(forwards::incrementAndGet))
                .subscribeOn(boundedElastic()).subscribe();
        try {
            assertTrue(entered.await(2, SECONDS));
            subscription.dispose();
        } finally {
            resume.countDown();
        }
        verify(runtime, timeout(1000)).exit();
        verify(log, times(1)).submit(argThat(row -> Integer.valueOf(499).equals(row.get("status"))));
        assertEquals(0, forwards.get());
        var window = snapshot();
        assertEquals(1, window.get("n_arrivals")); assertEquals(1, window.get("n"));
        assertEquals(0, window.get("pending"));
    }

    @Test void sameOwnerAtNewEpochCannotUseTheStaleSnapshot() {
        AtomicInteger attempts = new AtomicInteger();
        when(runtime.allows("monolith")).thenReturn(true);
        when(runtime.enter("monolith")).thenAnswer(ignored -> {
            if (attempts.incrementAndGet() == 1) {
                when(routing.current()).thenReturn(new RoutingSnapshot("routing/1", 4, "monolith", "request",
                        Map.of("monolith", Map.of("default", "http://unused")), Set.of()));
            }
            return true;
        });
        var exchange = request("/api/person/11111111-1111-4111-8111-111111111111", "t");
        AtomicInteger forwards = new AtomicInteger();
        filter.filter(exchange, e -> {
            forwards.incrementAndGet(); e.getResponse().setStatusCode(OK); return e.getResponse().setComplete();
        }).block();
        assertEquals(1, forwards.get()); verify(runtime, times(2)).exit();
        verify(log).submit(argThat(row -> Long.valueOf(4).equals(row.get("epoch"))
                && Long.valueOf(0).equals(row.get("routing_epoch_at_accept"))));
        assertEquals(1, snapshot().get("n_arrivals"));
    }
}
