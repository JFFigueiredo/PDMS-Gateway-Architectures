package ipl.pdms.gateway;

import ipl.pdms.gateway.config.GatewayClock;

final class FakeClock extends GatewayClock {

    private long millis;
    private long nanos;

    FakeClock(long startMillis) {
        this.millis = startMillis;
    }

    void advanceMillis(long delta) {
        millis += delta;
        nanos += delta * 1_000_000L;
    }

    @Override
    public long epochMillis() {
        return millis;
    }

    @Override
    public long nanoTime() {
        return nanos;
    }
}
