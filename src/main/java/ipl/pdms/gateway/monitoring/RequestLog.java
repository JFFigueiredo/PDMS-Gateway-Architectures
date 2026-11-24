package ipl.pdms.gateway.monitoring;

import static java.lang.Thread.currentThread;
import static java.nio.channels.Channels.newOutputStream;
import static java.nio.channels.FileChannel.open;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.config.GatewayProperties;
import jakarta.annotation.PreDestroy;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Logging never blocks requests; dropped rows or a missing end_of_log marker mean incomplete evidence. */
@Component
public class RequestLog {

    private static final Logger LOG = LoggerFactory.getLogger(RequestLog.class);

    private final ObjectMapper mapper;
    private final String instanceId;
    private final String runId;
    private final Path file;
    private final BlockingQueue<Map<String, Object>> queue;
    private final int queueCapacity;
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicBoolean writerFailed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final FileChannel channel;
    private final OutputStream out;
    private final Thread writer;
    private final Object lifecycle = new Object();
    private final long shutdownTimeoutMs;

    @org.springframework.beans.factory.annotation.Autowired
    public RequestLog(GatewayProperties properties, ObjectMapper mapper) throws IOException {
        this(properties, mapper, ofSeconds(20));
    }

    RequestLog(GatewayProperties properties, ObjectMapper mapper, Duration shutdownTimeout) throws IOException {
        if (shutdownTimeout.isNegative() || shutdownTimeout.isZero() || shutdownTimeout.toMillis() < 1) {
            throw new IllegalArgumentException("Log shutdown timeout must be positive");
        }
        this.shutdownTimeoutMs = shutdownTimeout.toMillis();
        this.mapper = mapper;
        this.instanceId = properties.instanceId().isBlank() ? randomUUID().toString() : properties.instanceId();
        this.runId = properties.runId();
        this.queueCapacity = properties.logQueueCapacity();
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        final Path dir = Path.of(properties.logDir());
        createDirectories(dir);
        this.file = dir.resolve("requests-" + runId + "-" + instanceId + ".jsonl");
        this.channel = open(file, CREATE_NEW, WRITE);
        this.out = new BufferedOutputStream(newOutputStream(channel), 1 << 16);
        this.writer = new Thread(this::writeLoop, "pdms-gateway-log-writer");
        this.writer.setDaemon(true);
        this.writer.start();
    }

    public String instanceId() {
        return instanceId;
    }

    public String runId() {
        return runId;
    }

    public Path file() {
        return file;
    }

    public long rowsDropped() {
        return dropped.get();
    }

    public long rowsWritten() {
        return written.get();
    }

    public long lastSequence() {
        return sequence.get();
    }

    public int queueDepth() {
        return queue.size();
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public boolean writerHealthy() {
        return !closed.get() && !writerFailed.get() && writer.isAlive();
    }

    public boolean isComplete() {
        return dropped.get() == 0 && !writerFailed.get();
    }

    public void submit(Map<String, Object> row) {
        final long seq = sequence.incrementAndGet();
        final Map<String, Object> full = new LinkedHashMap<>();
        full.put("seq", seq);
        full.put("instance_id", instanceId);
        full.put("run_id", runId);
        full.putAll(row);
        synchronized (lifecycle) {
            if (closed.get() || !queue.offer(full)) {
                dropped.incrementAndGet();
            }
        }
    }

    private void writeLoop() {
        try {
            while (true) {
                final Map<String, Object> row = queue.poll(100, MILLISECONDS);
                if (row == null) {
                    if (closed.get()) {
                        break;
                    }
                    out.flush();
                    continue;
                }
                out.write(mapper.writeValueAsBytes(row));
                out.write('\n');
                written.incrementAndGet();
            }
            // Only the writer owns the buffered stream and final marker.
            if (!writerFailed.get()) {
                final Map<String, Object> end = new LinkedHashMap<>();
                end.put("end_of_log", true);
                end.put("instance_id", instanceId);
                end.put("run_id", runId);
                end.put("final_seq", sequence.get());
                end.put("rows_written", written.get());
                end.put("rows_dropped", dropped.get());
                end.put("complete", isComplete());
                final byte[] footer = mapper.writeValueAsBytes(end);
                // Serialisation can outlive shutdown; recheck completeness before writing the final marker.
                if (!writerFailed.get()) {
                    out.write(footer);
                    out.write('\n');
                    out.flush();
                    channel.force(true);
                }
            }
        } catch (Exception e) {
            writerFailed.set(true);
            LOG.error("request log writer failed; run is invalid", e);
        } finally {
            try {
                out.close();
            }
            catch (IOException failure) {
                writerFailed.set(true);
                LOG.error("closing request log stream failed; run is invalid", failure);
            }
        }
    }

    @PreDestroy
    public void close() {
        synchronized (lifecycle) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
        }
        try {
            writer.join(shutdownTimeoutMs);
            if (writer.isAlive()) {
                writerFailed.set(true);
                writer.interrupt();
                LOG.error("Request log writer did not stop within the shutdown deadline; run is invalid");
            }
        } catch (InterruptedException e) {
            writerFailed.set(true);
            writer.interrupt();
            currentThread().interrupt();
            LOG.error("Request log shutdown interrupted; run is invalid", e);
        }
    }

    @Override
    public String toString() {
        return "RequestLog{" + file.toString().replace('\\', '/') + ", " + UTF_8 + "}";
    }
}
