package ipl.pdms.gateway.forwarding;

import static ipl.pdms.gateway.forwarding.RequestContext.ATTRIBUTE;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

import ipl.pdms.gateway.config.GatewayClock;
import ipl.pdms.gateway.experiment.Stimulus;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/** Run after RouteToRequestUrlFilter (10000) and before NettyRoutingFilter to preserve the chosen target. */
@Component
@RequiredArgsConstructor
public class TargetUriGlobalFilter implements GlobalFilter, Ordered {

    private final GatewayClock clock;
    private final Stimulus stimulus;


    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE - 100;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        final RequestContext ctx = exchange.getAttribute(ATTRIBUTE);
        if (ctx == null) {
            return chain.filter(exchange);
        }
        exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, ctx.targetUri);
        final long start = clock.nanoTime();
        // Measure until response headers arrive; the body is streamed afterwards.
        return stimulus.forward(ctx.realisation, ctx.probe, ctx.requestId, exchange.getAttributes(), () -> chain.filter(exchange))
                .doOnSuccess(ignored -> {
                    ctx.upstreamNanos = clock.nanoTime() - start;
                    ctx.upstreamHeadersReceived = true;
                })
                .doOnError(error -> ctx.upstreamNanos = clock.nanoTime() - start)
                .doOnCancel(() -> ctx.upstreamNanos = clock.nanoTime() - start);
    }
}
