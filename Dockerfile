FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock LICENSE ./
COPY crates/monitor-core crates/monitor-core
COPY crates/monitor-probe crates/monitor-probe
RUN cargo test --locked --release --workspace \
    && cargo build --locked --release -p monitor-probe --bin monitor-probe

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates=20250419~deb12u2 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 monitor \
    && mkdir -p /var/lib/monitor-probe /usr/share/licenses/monitor-probe \
    && chown monitor:monitor /var/lib/monitor-probe
COPY --from=build /src/target/release/monitor-probe /usr/local/bin/monitor-probe
COPY LICENSE /usr/share/licenses/monitor-probe/LICENSE
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/tikpok/harvest-scope-probe" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.licenses="LicenseRef-Harvest-Scope-Participation"
ENV PROBE_MODE=community \
    COMMUNITY_STATE_DIR=/var/lib/monitor-probe/community
USER 10001:10001
WORKDIR /var/lib/monitor-probe
ENTRYPOINT ["/usr/local/bin/monitor-probe"]
