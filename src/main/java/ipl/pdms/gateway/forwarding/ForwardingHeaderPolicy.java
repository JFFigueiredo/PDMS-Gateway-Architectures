package ipl.pdms.gateway.forwarding;

import static org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.Type.REQUEST;
import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

import java.net.InetSocketAddress;
import java.util.List;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

/** Replace client-supplied forwarding headers after all other gateway header filters. */
@Component
public class ForwardingHeaderPolicy implements HttpHeadersFilter, Ordered {

    static final List<String> STRIPPED = List.of("Forwarded", "X-Forwarded-For", "X-Forwarded-Host",
            "X-Forwarded-Proto", "X-Forwarded-Port", "X-Forwarded-Prefix", "X-Real-IP", "True-Client-IP",
            "X-Client-IP", "X-Cluster-Client-IP", "X-Original-Forwarded-For");

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Override
    public boolean supports(Type type) {
        return type == REQUEST;
    }

    @Override
    public HttpHeaders filter(HttpHeaders input, ServerWebExchange exchange) {
        final HttpHeaders headers = new HttpHeaders();
        headers.addAll(input);
        STRIPPED.forEach(headers::remove);
        final InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
        if (remote != null && remote.getAddress() != null) {
            headers.set("X-Forwarded-For", remote.getAddress().getHostAddress());
        }
        final String scheme = exchange.getRequest().getURI().getScheme();
        if (scheme != null) {
            headers.set("X-Forwarded-Proto", scheme);
        }
        final String host = exchange.getRequest().getHeaders().getFirst("Host");
        if (host != null) {
            headers.set("X-Forwarded-Host", host);
        }
        return headers;
    }
}
