package ipl.pdms.gateway.forwarding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.http.HttpStatus.BAD_GATEWAY;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static reactor.netty.http.client.PrematureCloseException.TEST_EXCEPTION;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class GatewayFailureTest {

    private static void expect(Throwable error, HttpStatus status, String reason) {
        GatewayFailure failure = GatewayFailure.of(error);
        assertEquals(status, failure.status(), error.toString());
        assertEquals(reason, failure.reason(), error.toString());
    }

    @Test
    void connectionRefusedAndUnresolvableNamesAreServiceUnavailable() {
        expect(new ConnectException("Connection refused"), SERVICE_UNAVAILABLE, "upstream_unreachable");
        expect(new RuntimeException(new UnknownHostException("x")), SERVICE_UNAVAILABLE,
                "upstream_unreachable");
    }

    @Test
    void connectTimeoutIsGatewayTimeout() {
        expect(new ConnectTimeoutException("connect timed out"), GATEWAY_TIMEOUT, "connect_timeout");
    }

    @Test
    void prematureCloseAndResetAreBadGateway() {
        expect(TEST_EXCEPTION, BAD_GATEWAY, "upstream_reset");
        expect(new IOException("Connection reset by peer"), BAD_GATEWAY, "upstream_reset");
    }

    @Test
    void responseTimeoutIsGatewayTimeout() {
        expect(ReadTimeoutException.INSTANCE, GATEWAY_TIMEOUT, "upstream_timeout");
        expect(new TimeoutException("x"), GATEWAY_TIMEOUT, "upstream_timeout");
        expect(new ResponseStatusException(GATEWAY_TIMEOUT), GATEWAY_TIMEOUT,
                "upstream_timeout");
    }

    /** Models Netty's shaded pool exception without depending on its package. */
    static final class PoolAcquireTimeoutException extends TimeoutException {
        PoolAcquireTimeoutException() {
            super("pool");
        }
    }

    /** Models a channel closing before complete response headers. */
    static final class PrematureChannelClosureException extends IOException {
        PrematureChannelClosureException() {
            super("closed in mid-headers");
        }
    }

    @Test
    void poolTimeoutIsRecognisedBeforeTheGenericTimeout() {
        expect(new PoolAcquireTimeoutException(), SERVICE_UNAVAILABLE, "pool_timeout");
    }

    @Test
    void closeInMidHeadersIsBadGateway() {
        expect(new PrematureChannelClosureException(), BAD_GATEWAY, "upstream_reset");
        expect(new java.nio.channels.ClosedChannelException(), BAD_GATEWAY, "upstream_reset");
    }

    @Test
    void anythingElseIsAnInternalGatewayError() {
        expect(new IllegalStateException("boom"), INTERNAL_SERVER_ERROR, "gateway_internal");
    }

    @Test void capacityRefusalIsServiceUnavailable() {
        expect(new ResponseStatusException(SERVICE_UNAVAILABLE), SERVICE_UNAVAILABLE, "upstream_overloaded");
    }
}
