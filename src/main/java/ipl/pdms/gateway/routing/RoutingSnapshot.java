package ipl.pdms.gateway.routing;

import static ipl.pdms.gateway.routing.Service.PERSON;
import static ipl.pdms.gateway.routing.Service.PLATFORM;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notEmpty;

import java.util.Map;
import java.util.Set;

public record RoutingSnapshot(String version, long epoch, String active, String mode,
                              Map<String, Map<String, String>> backends, Set<String> disabled) {

    public static final String VERSION = "routing/1";
    public static final String MODE_SESSION = "session";
    public static final String MODE_REQUEST = "request";

    public static boolean isMode(String mode) {
        return MODE_SESSION.equals(mode) || MODE_REQUEST.equals(mode);
    }

    public RoutingSnapshot {
        disabled = disabled == null ? Set.of() : Set.copyOf(disabled);
    }

    public Set<String> realisations() {
        return backends.keySet();
    }

    public boolean knows(String realisation) {
        return realisation != null && backends.containsKey(realisation);
    }

    public boolean isEnabled(String realisation) {
        return !disabled.contains(realisation);
    }

    public String baseUrl(String realisation, Service service) {
        final Map<String, String> b = backends.get(realisation);
        String url = b.get(service.key());
        if (url == null) {
            url = b.get("default");
        }
        if (url == null && service == PLATFORM) {
            url = b.get(PERSON.key());
        }
        return url;
    }

    public void validate() {
        validateFormat();
        validateArchitectures();
        backends.forEach(RoutingSnapshot::validateBackend);
    }

    private void validateFormat() {
        isTrue(VERSION.equals(version), () -> "routing table version must be " + VERSION + ", was " + version);
        isTrue(isMode(mode), () -> "mode must be session or request, was " + mode);
    }

    private void validateArchitectures() {
        notEmpty(backends, "backends must not be empty");
        isTrue(backends.containsKey(active), () -> "active realisation is not in backends: " + active);
        for (final String name : disabled) {
            isTrue(backends.containsKey(name), () -> "disabled realisation is not in backends: " + name);
        }
        isTrue(!disabled.contains(active), () -> "the active realisation cannot be disabled: " + active);
    }

    private static void validateBackend(String name, Map<String, String> b) {
        final boolean hasDefault = b.containsKey("default");
        final boolean hasAll = b.keySet().containsAll(Set.of("person", "snapshot", "consent"));
        isTrue(hasDefault || hasAll, () -> "backend " + name + " needs 'default' or all of person, snapshot, consent");
        b.values().forEach(RoutingSnapshot::validateUrl);
    }

    private static void validateUrl(String url) {
        isTrue(url.startsWith("http://") || url.startsWith("https://"), () -> "backend URL must be http(s): " + url);
        isTrue(!url.endsWith("/"), () -> "backend URL must not end with '/': " + url);
        try {
            isTrue(java.net.URI.create(url).getHost() != null, () -> "backend URL has no host: " + url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("backend URL is not valid: " + url, e);
        }
    }

}
