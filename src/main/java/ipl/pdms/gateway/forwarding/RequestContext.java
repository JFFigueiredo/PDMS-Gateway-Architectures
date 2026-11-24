package ipl.pdms.gateway.forwarding;

import ipl.pdms.gateway.routing.Endpoint;
import ipl.pdms.gateway.routing.Service;
import java.net.URI;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
final class RequestContext {

    static final String ATTRIBUTE = RequestContext.class.getName();

    final String runId;
    final String planId;
    final String sessionId;
    final String requestId;
    final String method;
    final String rawPath;
    final String operation;
    final Service service;
    final Endpoint.Kind kind;
    final String realisation;
    final String activeAtAccept;
    final long epoch;
    final boolean probe;
    final long acceptMs;
    final long acceptNs;
    final URI targetUri;

    volatile long upstreamNanos = -1;
    volatile boolean upstreamHeadersReceived;
    volatile boolean errored;
    /** Nanoseconds; -1 when backend admission was bypassed. */
    volatile long admissionWaitNanos = -1;
    long handoverWaitNanos;
    long routeMs;
    long receivedEpoch;
    String activeAtRoute;
    volatile String gatewayError;
    volatile boolean statusGenerated;
    boolean runtimeAdmitted;

}
