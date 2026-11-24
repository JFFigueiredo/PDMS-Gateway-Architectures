package ipl.pdms.gateway.health;



public final class HealthState {

    public enum Status { UNKNOWN, UP, DOWN }

    public record View(Status status, int consecutiveFailures, int consecutiveSuccesses,
                       long lastCheckEpochMs, long lastChangeEpochMs, String lastError) {
    }

    private Status status = Status.UNKNOWN;
    private int failures;
    private int successes;
    private long lastCheckMs;
    private long lastChangeMs;
    private String lastError;

    public synchronized void record(boolean ok, String error, long nowMs, int failThreshold, int successThreshold) {
        lastCheckMs = nowMs;
        if (ok) {
            successes++;
            failures = 0;
            lastError = null;
            if (status != Status.UP && successes >= successThreshold) {
                status = Status.UP;
                lastChangeMs = nowMs;
            }
        } else {
            failures++;
            successes = 0;
            lastError = error;
            if (status != Status.DOWN && failures >= failThreshold) {
                status = Status.DOWN;
                lastChangeMs = nowMs;
            }
        }
    }

    public synchronized View view() {
        return new View(status, failures, successes, lastCheckMs, lastChangeMs, lastError);
    }

    public synchronized Status status() {
        return status;
    }
}
