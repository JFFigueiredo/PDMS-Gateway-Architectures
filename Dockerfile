# Base images are pinned by tag and digest.

# -----------------------------------------------------------------------------
# Stage 1: build and test. A failing test fails the image build.
# -----------------------------------------------------------------------------
FROM maven:3.9.16-eclipse-temurin-25@sha256:93b8a14ea2f412782e4e842651273b4d903e35cc496284f178fbbe2d67d00976 AS builder

WORKDIR /build

# Same time zone as the local build, so the jar is the same on every machine.
ENV MAVEN_OPTS="-Duser.timezone=UTC"

# Dependencies first: this layer is reused while pom.xml is unchanged.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -q dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp verify

# -----------------------------------------------------------------------------
# Stage 2: run
# -----------------------------------------------------------------------------
FROM eclipse-temurin:25.0.3_9-jre-noble@sha256:1a99f30a84d58729f1184c1c693f25fb6f91970ddf05395515540bc5773b8edd

ARG VERSION=0.1.0
ARG REVISION=unknown
# Recorded in the run_start row of every request log (telemetry/RunProvenance).
ENV PDMS_GATEWAY_VERSION=${VERSION} PDMS_GATEWAY_REVISION=${REVISION}
LABEL org.opencontainers.image.title="pdms-gateway" \
      org.opencontainers.image.description="Routing gateway for the PDMS deployment variants" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0"

# Fixed numeric ids so volume ownership is stable across hosts. No login shell, no home directory.
RUN groupadd --system --gid 10001 pdms \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pdms \
    && mkdir -p /app/config /app/logs \
    && chown -R 10001:10001 /app

WORKDIR /app
COPY --from=builder --chown=10001:10001 /build/target/pdms-gateway-${VERSION}.jar /app/app.jar

USER 10001:10001
EXPOSE 8080

# Readiness check without extra packages: bash /dev/tcp against the Actuator health endpoint.
# It reports only that this process is up; it does not aggregate backend health.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/localhost/8080 && printf 'GET /gateway/actuator/health HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"UP\"' <&3"]

# Record any heap or GC overrides supplied through JAVA_TOOL_OPTIONS.
ENTRYPOINT ["java", \
            "-XX:+ExitOnOutOfMemoryError", \
            "-Duser.timezone=UTC", \
            "-Dfile.encoding=UTF-8", \
            "-jar", "/app/app.jar"]
