package ipl.pdms.gateway;

import static io.micrometer.core.instrument.Metrics.addRegistry;
import static io.micrometer.core.instrument.Metrics.removeRegistry;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.nio.file.Files.writeString;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.core.publisher.Mono.just;
import static reactor.netty.http.client.HttpClient.newConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import ipl.pdms.gateway.config.GatewayProperties;
import ipl.pdms.gateway.monitoring.GatewayMetrics;
import ipl.pdms.gateway.monitoring.RequestLog;
import ipl.pdms.gateway.monitoring.TrafficStatistics;
import ipl.pdms.gateway.routing.RoutingTable;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

class GatewayDiagnosticsTest {
    @TempDir Path dir;

    private static final class TestRegistry extends SimpleMeterRegistry implements AutoCloseable {}

    private GatewayProperties properties() throws Exception {
        Path routing = dir.resolve("routing.json");
        writeString(routing, """
                {"version":"routing/1","epoch":0,"active":"monolith","mode":"request",
                 "backends":{"monolith":{"default":"http://localhost:8080"}}}
                """);
        return new GatewayProperties(routing.toString(), "diagnostics", randomUUID().toString(),
                dir.toString(), "", 2, 2,
                ofSeconds(2), ofSeconds(1), 3, 2, 200, 200, ofSeconds(2));
    }

    private GatewayMetrics bind(SimpleMeterRegistry registry, GatewayProperties props, RequestLog log,
                                TrafficStatistics stats) throws Exception {
        return new GatewayMetrics(registry, new RoutingTable(props, new ObjectMapper()), log, stats);
    }

    @Test
    void queuePressureAndDropsRemainSeparateFromWriterHealth() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ObjectMapper blocked = new ObjectMapper() {
            @Override public byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
                entered.countDown();
                try {
                    if (!release.await(5, SECONDS)) {
                        throw new IllegalStateException("fixture timed out");
                    }
                } catch (InterruptedException e) {
                    currentThread().interrupt(); throw new IllegalStateException(e);
                }
                return super.writeValueAsBytes(value);
            }
        };
        GatewayProperties props = properties();
        FakeClock clock = new FakeClock(10_000);
        try (TestRegistry registry = new TestRegistry()) {
            RequestLog log = new RequestLog(props, blocked);
            try {
                bind(registry, props, log, new TrafficStatistics(clock, props));
                log.submit(Map.of("fixture", 1));
                assertTrue(entered.await(2, SECONDS));
                log.submit(Map.of("fixture", 2)); log.submit(Map.of("fixture", 3));
                log.submit(Map.of("fixture", 4));
                assertEquals(2, registry.get("pdms_dyn_log_queue_depth").gauge().value());
                assertEquals(2, registry.get("pdms_dyn_log_queue_capacity").gauge().value());
                assertEquals(1, registry.get("pdms_dyn_log_writer_healthy").gauge().value());
                assertEquals(1, registry.get("pdms_dyn_log_rows_dropped").gauge().value());
            } finally {
                release.countDown();
                log.close();
            }
            assertEquals(0, registry.get("pdms_dyn_log_writer_healthy").gauge().value());
        }
    }

    @Test
    void failedWriterIsUnhealthyEvenWithNoDroppedRows() throws Exception {
        ObjectMapper broken = new ObjectMapper() {
            @Override public byte[] writeValueAsBytes(Object value) {
                throw new IllegalStateException("deliberate isolated writer failure");
            }
        };
        GatewayProperties props = properties();
        FakeClock clock = new FakeClock(10_000);
        try (TestRegistry registry = new TestRegistry()) {
            RequestLog log = new RequestLog(props, broken);
            try {
                bind(registry, props, log, new TrafficStatistics(clock, props));
                log.submit(Map.of("fixture", "writer-failure"));
                await(() -> !log.writerHealthy());
                assertEquals(0, registry.get("pdms_dyn_log_writer_healthy").gauge().value());
                assertEquals(0, registry.get("pdms_dyn_log_rows_dropped").gauge().value());
                assertFalse(log.isComplete());
            } finally {
                log.close();
            }
        }
    }

    @Test
    void statisticsCounterTracksRefusedSamplesAndErrorLabelsStayBounded() throws Exception {
        GatewayProperties props = properties();
        FakeClock clock = new FakeClock(10_000);
        TrafficStatistics stats = new TrafficStatistics(clock, props);
        try (TestRegistry registry = new TestRegistry()) {
            RequestLog log = new RequestLog(props, new ObjectMapper());
            try {
                GatewayMetrics metrics = bind(registry, props, log, stats);
                stats.arrival("mix"); stats.arrival("mix"); stats.arrival("mix");
                assertEquals(1, registry.get("pdms_dyn_stats_overflow").functionCounter().count());
                assertEquals(false, stats.snapshot(30).get("valid"));
                metrics.error("monolith", "person_get", "gateway", "upstream_timeout", false);
                assertEquals(1, registry.get("pdms_dyn_gateway_errors").counter().count());
                assertEquals(5, registry.get("pdms_dyn_gateway_errors").counter().getId().getTags().size());
                metrics.upstreamHeaders("monolith", "person_get", false, 25_000_000);
                var timer = registry.get("pdms_dyn_upstream_headers_duration").timer();
                assertEquals(1, timer.count());
                assertEquals(25, timer.totalTime(MILLISECONDS), 0.001);
            } finally {
                log.close();
            }
        }
    }

    @Test
    void eventLoopGaugeObservesQueuedTasksAndReturnsToZero() throws Exception {
        var server = HttpServer.create().host("localhost").port(0)
                .handle((req, res) -> res.sendString(just("ok"))).bindNow();
        LoopResources loops = LoopResources.create("diagnostic-loop", 1, true);
        AtomicReference<SingleThreadEventExecutor> observed = new AtomicReference<>();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        var props = properties();
        var clock = new FakeClock(10_000);
        var log = new RequestLog(props, new ObjectMapper());
        try (var registry = new TestRegistry()) {
            HttpClient client = bind(registry, props, log, new TrafficStatistics(clock, props)).customize(newConnection()
                    .runOn(loops).doOnConnected(c -> observed.set((SingleThreadEventExecutor)c.channel().eventLoop())));
            assertEquals("ok", client.get().uri("http://localhost:" + server.port())
                    .responseSingle((res, bytes) -> bytes.asString()).block(ofSeconds(5)));
            var executor = observed.get();
            executor.execute(() -> {
                entered.countDown();
                try {
                    release.await(5, SECONDS);
                }
                catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            });
            assertTrue(entered.await(2, SECONDS));
            executor.execute(() -> {}); executor.execute(() -> {});
            var gauge = registry.get("pdms_dyn_event_loop_pending_tasks").gauge();
            assertTrue(gauge.value() >= 2, "queued tasks were not visible");
            release.countDown(); await(() -> gauge.value() == 0);
            assertEquals(1, registry.find("pdms_dyn_event_loop_pending_tasks").gauges().size());
        } finally {
            release.countDown(); loops.disposeLater(ZERO, ofSeconds(2)).block();
            server.disposeNow();
            log.close();
        }
    }

    @Test
    void nativePoolGaugeObservesPendingConnectionAcquisition() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        var release = reactor.core.publisher.Sinks.<String>one();
        var server = HttpServer.create().host("localhost").port(0)
                .handle((req, res) -> {
                    entered.countDown();
                    return res.sendString(release.asMono());
                }).bindNow();
        ConnectionProvider provider = ConnectionProvider.builder("diagnostic-pool")
                .maxConnections(1).pendingAcquireMaxCount(4).metrics(true).build();
        try (TestRegistry registry = new TestRegistry()) {
            addRegistry(registry);
            try {
                HttpClient client = HttpClient.create(provider);
                String uri = "http://localhost:" + server.port();
                var first = client.get().uri(uri).responseSingle((res, bytes) -> bytes.asString()).toFuture();
                assertTrue(entered.await(3, SECONDS));
                var second = client.get().uri(uri).responseSingle((res, bytes) -> bytes.asString()).toFuture();
                await(() -> registry.find("reactor.netty.connection.provider.pending.connections")
                        .tag("name", "diagnostic-pool").gauges().stream().anyMatch(g -> g.value() == 1));
                assertEquals(1, registry.get("reactor.netty.connection.provider.active.connections")
                        .tag("name", "diagnostic-pool").gauge().value());
                release.tryEmitValue("ok");
                assertEquals("ok", first.get(5, SECONDS));
                assertEquals("ok", second.get(5, SECONDS));
                await(() -> registry.get("reactor.netty.connection.provider.pending.connections")
                        .tag("name", "diagnostic-pool").gauge().value() == 0);
            } finally {
                release.tryEmitValue("ok");
                provider.disposeLater().block();
                removeRegistry(registry);
            }
        } finally {
            server.disposeNow();
        }
    }

    private static void await(BooleanSupplier condition) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }
}
