package ipl.pdms.gateway.routing;



public enum Service {
    PERSON("person"),
    CONSENT("consent"),
    SNAPSHOT("snapshot"),
    PLATFORM("platform");

    private final String key;

    Service(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
