package ipl.pdms.gateway.routing;

import static ipl.pdms.gateway.routing.RoutingSnapshot.MODE_REQUEST;
import static java.lang.Math.addExact;
import static java.nio.file.Files.readAllBytes;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

import com.fasterxml.jackson.databind.ObjectMapper;
import ipl.pdms.gateway.config.GatewayProperties;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

@Component
public class RoutingTable {

    private final AtomicReference<RoutingSnapshot> current = new AtomicReference<>();
    private final String configHash;

    public RoutingTable(GatewayProperties properties, ObjectMapper mapper) throws IOException {
        final Path file = Path.of(properties.routingFile());
        final byte[] bytes = readAllBytes(file);
        final RoutingSnapshot snapshot = mapper.readValue(bytes, RoutingSnapshot.class);
        snapshot.validate();
        this.current.set(snapshot);
        this.configHash = sha256Hex(bytes);
    }

    public RoutingSnapshot current() {
        return current.get();
    }

    /** When all architectures are paused, active retains the last routing pointer. */
    public RoutingSnapshot runtimeOwner(long expectedEpoch, String owner) {
        final RoutingSnapshot before = current.get();
        if (before.epoch() != expectedEpoch || (owner != null && !before.knows(owner))) {
            return null;
        }
        final var disabled = new java.util.HashSet<>(before.realisations());
        if (owner != null) {
            disabled.remove(owner);
        }
        final RoutingSnapshot after = new RoutingSnapshot(before.version(), addExact(before.epoch(), 1),
                owner == null ? before.active() : owner, MODE_REQUEST, before.backends(), disabled);
        return current.compareAndSet(before, after) ? after : null;
    }

    public String configHash() {
        return configHash;
    }

}
