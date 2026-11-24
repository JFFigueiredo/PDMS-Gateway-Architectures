package ipl.pdms.gateway.monitoring;

import static java.lang.Boolean.TRUE;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.readString;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.util.ReflectionTestUtils.getField;

import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.config.GatewayProperties;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RequestLogShutdownTest {
    @TempDir Path directory;

    private GatewayProperties properties() {
        return new GatewayProperties("unused", "shutdown-test", randomUUID().toString(), directory.toString(),
                "", 2, 100, ofSeconds(2), ofSeconds(1), 3, 2,
                10, 10, ofSeconds(1));
    }

    @Test void fullQueueWithDeadWriterDoesNotBlockShutdown() throws Exception {
        ObjectMapper broken = new ObjectMapper() {
            @Override public byte[] writeValueAsBytes(Object value) {
                throw new IllegalStateException("isolated writer failure");
            }
        };
        RequestLog log = new RequestLog(properties(), broken, ofMillis(500));
        log.submit(Map.of("fixture", 1));
        Thread writer = (Thread) getField(log, "writer");
        writer.join(1000); assertFalse(writer.isAlive());
        log.submit(Map.of("fixture", 2)); log.submit(Map.of("fixture", 3));
        assertEquals(2, log.queueDepth());
        assertTimeoutPreemptively(ofSeconds(1), log::close);
        assertFalse(log.isComplete());
        assertFalse(readString(log.file()).contains("end_of_log"));
    }

    @Test void blockedWriterHasBoundedShutdownAndNoConcurrentFooterWriter() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ObjectMapper blocked = new ObjectMapper() {
            @Override public byte[] writeValueAsBytes(Object value) throws com.fasterxml.jackson.core.JsonProcessingException {
                entered.countDown();
                boolean done = false;
                while (!done) {
                    try {
                        done = release.await(1, SECONDS);
                    }
                    catch (InterruptedException ignored) { /* Model an uninterruptible external writer. */ }
                }
                return super.writeValueAsBytes(value);
            }
        };
        RequestLog log = new RequestLog(properties(), blocked, ofMillis(100));
        Thread writer = (Thread) getField(log, "writer");
        try {
            log.submit(Map.of("fixture", 1)); assertTrue(entered.await(1, SECONDS));
            assertTimeoutPreemptively(ofSeconds(1), log::close);
            assertFalse(log.isComplete());
            assertFalse(readString(log.file()).contains("end_of_log"));
        } finally {
            release.countDown(); writer.join(1000);
        }
        assertFalse(writer.isAlive());
        assertFalse(readString(log.file()).contains("end_of_log"));
    }

    @Test void footerSerializationCannotPublishCompleteAfterShutdownTimeout() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ObjectMapper blocked = new ObjectMapper() {
            @Override public byte[] writeValueAsBytes(Object value) throws com.fasterxml.jackson.core.JsonProcessingException {
                if (value instanceof Map<?, ?> row && TRUE.equals(row.get("end_of_log"))) {
                    entered.countDown();
                    boolean done = false;
                    while (!done) {
                        try {
                            done = release.await(1, SECONDS);
                        }
                        catch (InterruptedException ignored) { /* Model uninterruptible footer serialisation. */ }
                    }
                }
                return super.writeValueAsBytes(value);
            }
        };
        RequestLog log = new RequestLog(properties(), blocked, ofMillis(200));
        Thread writer = (Thread) getField(log, "writer");
        try {
            log.submit(Map.of("fixture", 1));
            var shutdown = runAsync(log::close);
            assertTrue(entered.await(1, SECONDS));
            shutdown.get(1, SECONDS);
            assertFalse(log.isComplete());
        } finally {
            release.countDown(); writer.join(1000);
        }
        assertFalse(writer.isAlive());
        assertFalse(readString(log.file()).contains("end_of_log"));
    }

    @Test void gracefulShutdownDrainsRowsAndWritesOneCompleteFinalMarker() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        RequestLog log = new RequestLog(properties(), mapper, ofSeconds(1));
        log.submit(Map.of("fixture", 1)); log.submit(Map.of("fixture", 2));
        log.close(); log.close();
        var rows = readAllLines(log.file());
        assertEquals(3, rows.size());
        var footer = mapper.readTree(rows.getLast());
        assertTrue(footer.path("end_of_log").asBoolean()); assertTrue(footer.path("complete").asBoolean());
        assertEquals(2, footer.path("rows_written").asLong()); assertEquals(2, footer.path("final_seq").asLong());
        assertEquals(0, footer.path("rows_dropped").asLong());
    }
}
