package ipl.pdms.gateway.routing;



/** Templates match literal segments, {name} for one segment, and a final ** for one or more segments. */
public record Endpoint(String method, String template, Service service, String operation, Kind kind,
                       String origin) {

    public enum Kind {
        API("api"),
        PLATFORM("platform");

        private final String key;

        Kind(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    boolean matches(String[] segments) {
        final String[] t = template.substring(1).split("/");
        for (int i = 0; i < t.length; i++) {
            if (t[i].equals("**")) {
                return segments.length > i;
            }
            if (i >= segments.length) {
                return false;
            }
            if (!t[i].startsWith("{") && !t[i].equals(segments[i])) {
                return false;
            }
        }
        return t.length == segments.length;
    }
}
