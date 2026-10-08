# syntax=docker/dockerfile:1

# --------------------------------------------------------------------------- build
FROM rust:1.85-bookworm AS builder
WORKDIR /usr/src/coalix
COPY . .
# First match wins: --locked when a Cargo.lock is committed, otherwise resolve fresh.
RUN cargo build --release --locked || cargo build --release

# ------------------------------------------------------------------------ runtime
FROM gcr.io/distroless/cc-debian12:nonroot
LABEL org.opencontainers.image.title="coalix" \
      org.opencontainers.image.description="Zero-config request-coalescing reverse proxy" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=builder /usr/src/coalix/target/release/coalix /usr/local/bin/coalix
COPY config/coalix.example.yaml /etc/coalix/config.yaml
ENV RUST_LOG=info
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/coalix"]
CMD ["--config", "/etc/coalix/config.yaml"]
