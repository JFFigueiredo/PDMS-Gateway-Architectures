package ipl.pdms.gateway.forwarding;

import static java.util.Locale.ROOT;
import static org.springframework.http.HttpStatus.BAD_GATEWAY;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.netty.http.client.PrematureCloseException;

record GatewayFailure(HttpStatus status, String reason) {

    static GatewayFailure of(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof ConnectTimeoutException) {
                return new GatewayFailure(GATEWAY_TIMEOUT, "connect_timeout");
            }
            if (t instanceof ConnectException || t instanceof UnknownHostException || t instanceof NoRouteToHostException) {
                return new GatewayFailure(SERVICE_UNAVAILABLE, "upstream_unreachable");
            }
            if (t instanceof PrematureCloseException) {
                return new GatewayFailure(BAD_GATEWAY, "upstream_reset");
            }
            // Pool-acquire timeouts also extend TimeoutException, so classify them first.
            final String name = t.getClass().getSimpleName();
            if (name.equals("PoolAcquireTimeoutException") || name.equals("PoolAcquirePendingLimitException")) {
                return new GatewayFailure(SERVICE_UNAVAILABLE, "pool_timeout");
            }
            if (name.equals("PrematureChannelClosureException") || t instanceof ClosedChannelException) {
                return new GatewayFailure(BAD_GATEWAY, "upstream_reset");
            }
            if (t instanceof ReadTimeoutException || t instanceof TimeoutException) {
                return new GatewayFailure(GATEWAY_TIMEOUT, "upstream_timeout");
            }
            if (t instanceof ResponseStatusException rse && rse.getStatusCode().value() == 504) {
                return new GatewayFailure(GATEWAY_TIMEOUT, "upstream_timeout");
            }
            if (t instanceof ResponseStatusException rse && rse.getStatusCode().value() == 503) {
                return new GatewayFailure(SERVICE_UNAVAILABLE, "upstream_overloaded");
            }
            if (t instanceof java.io.IOException io && io.getMessage() != null
                    && io.getMessage().toLowerCase(ROOT).contains("reset")) {
                return new GatewayFailure(BAD_GATEWAY, "upstream_reset");
            }
        }
        return new GatewayFailure(INTERNAL_SERVER_ERROR, "gateway_internal");
    }
}
