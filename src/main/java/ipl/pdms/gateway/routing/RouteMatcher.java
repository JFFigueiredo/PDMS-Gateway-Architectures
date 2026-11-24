package ipl.pdms.gateway.routing;

import static ipl.pdms.gateway.routing.PdmsEndpoints.all;

import java.util.ArrayList;
import java.util.List;

public final class RouteMatcher {

    private static final List<String> METHODS = List.of("GET", "POST", "PUT", "DELETE");

    private RouteMatcher() {
    }

    public record Match(Service service, String operation, Endpoint.Kind kind) {
    }

    public record Decision(Match match, int status, List<String> allow) {
        public boolean routed() {
            return match != null;
        }
    }

    public static Decision decide(String method, String rawPath) {
        Match found = null;
        final List<String> allowed = new ArrayList<>();
        for (final String m : METHODS) {
            final Match candidate = matchExact(m, rawPath);
            if (candidate != null) {
                allowed.add(m);
                if (m.equals(method)) {
                    found = candidate;
                }
            }
        }
        if (found != null) {
            return new Decision(found, 0, List.of());
        }
        if (!allowed.isEmpty()) {
            return new Decision(null, 405, allowed);
        }
        return new Decision(null, 404, List.of());
    }

    private static boolean isDotSegment(String segment) {
        final String s = segment.replace("%2e", ".").replace("%2E", ".");
        return s.equals(".") || s.equals("..");
    }

    private static Match matchExact(String method, String rawPath) {
        if (rawPath == null || rawPath.length() < 2 || rawPath.charAt(0) != '/' || rawPath.indexOf(';') >= 0) {
            return null; // Servlet containers strip path parameters; reject them to prevent traversal such as "..;".
        }
        final String[] segments = rawPath.substring(1).split("/", -1);
        for (final String segment : segments) {
            if (segment.isEmpty() || isDotSegment(segment)) {
                return null;
            }
        }
        for (final Endpoint endpoint : all()) {
            if (endpoint.method().equals(method) && endpoint.matches(segments)) {
                return new Match(endpoint.service(), endpoint.operation(), endpoint.kind());
            }
        }
        return null;
    }
}
