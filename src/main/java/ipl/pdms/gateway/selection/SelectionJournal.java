package ipl.pdms.gateway.selection;

import static java.nio.ByteBuffer.allocate;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.monitoring.RequestLog;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

@Component
public class SelectionJournal {
    private final ObjectMapper mapper;
    private final FileChannel channel;
    private final Path path;
    private volatile boolean failed;
    @org.springframework.beans.factory.annotation.Autowired
    public SelectionJournal(ObjectMapper mapper, RequestLog log) throws IOException {
        this(mapper, log, "selection");
    }
    public SelectionJournal(ObjectMapper mapper, RequestLog log, String prefix) throws IOException {
        this.mapper = mapper;
        path = log.file().resolveSibling(prefix + "-" + log.instanceId() + ".jsonl");
        channel = open(path, CREATE_NEW, WRITE);
    }
    public synchronized void append(Object value) throws IOException {
        if (failed) {
            throw new IOException("Selection journal is unavailable");
        }
        try {
            final byte[] json = mapper.writeValueAsBytes(value);
            final ByteBuffer bytes = allocate(json.length + 1).put(json).put((byte)'\n');
            bytes.flip();
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        } catch (IOException error) {
            failed = true;
            throw error;
        }
    }
    public String fileName() {
        return path.getFileName().toString();
    }
    public boolean healthy() {
        return !failed && channel.isOpen();
    }
    @PreDestroy public synchronized void close() throws IOException {
        channel.close();
    }
}
