package ipl.pdms.gateway.runtime;

import static org.springframework.http.ResponseEntity.ok;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.publisher.Mono.just;
import static reactor.core.scheduler.Schedulers.boundedElastic;

import ipl.pdms.gateway.config.AdminAuth;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class RuntimeController {
    private final RuntimeCoordinator runtime;
    private final AdminAuth auth;
    @GetMapping("/admin/runtime")
    public Mono<ResponseEntity<Map<String, Object>>> status(@RequestHeader(value="Authorization", required=false) String authorization) {
        return call(authorization, () -> ok(runtime.observedStatus()));
    }
    private Mono<ResponseEntity<Map<String, Object>>> call(String authorization,
            java.util.concurrent.Callable<ResponseEntity<Map<String, Object>>> work) {
        if (!auth.bearer(authorization)) {
            return just(ResponseEntity.status(401).body(Map.of("error", "unauthorised")));
        }
        return fromCallable(work).subscribeOn(boundedElastic());
    }
}
