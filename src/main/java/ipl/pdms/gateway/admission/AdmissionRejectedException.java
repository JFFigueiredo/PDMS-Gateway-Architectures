package ipl.pdms.gateway.admission;



public final class AdmissionRejectedException extends RuntimeException {

    public static final String FULL = "admission_full";
    public static final String TIMEOUT = "admission_timeout";

    private final String reason;
    private final long waitedNanos;

    public AdmissionRejectedException(String reason, long waitedNanos) {
        super(reason, null, false, false);
        this.reason = reason;
        this.waitedNanos = waitedNanos;
    }

    public String reason() {
        return reason;
    }

    public long waitedNanos() {
        return waitedNanos;
    }
}
