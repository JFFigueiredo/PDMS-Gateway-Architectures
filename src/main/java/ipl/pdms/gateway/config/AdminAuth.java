package ipl.pdms.gateway.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.security.MessageDigest.isEqual;

import org.springframework.stereotype.Component;

/** A blank admin token disables admin, internal and probe access. */
@Component
public class AdminAuth {

    private final byte[] token;

    public AdminAuth(GatewayProperties properties) {
        final String value = properties.adminToken();
        this.token = value == null || value.isBlank() ? null : value.getBytes(UTF_8);
    }

    public boolean bearer(String authorization) {
        return authorization != null && token != null
                && isEqual(authorization.getBytes(UTF_8),
                        ("Bearer " + new String(token, UTF_8)).getBytes(UTF_8));
    }

    public boolean raw(String presented) {
        return presented != null && token != null
                && isEqual(presented.getBytes(UTF_8), token);
    }
}
