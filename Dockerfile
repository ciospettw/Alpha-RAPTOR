# syntax=docker/dockerfile:1.7
FROM rust:1.88-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV CARGO_TARGET_DIR=/app/target

COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    set -eux; \
    mkdir -p src; \
    printf 'fn main() {}\n' > src/main.rs; \
    cargo build --release --locked; \
    rm -rf src

COPY src ./src
COPY public ./public

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --locked \
    && cp /app/target/release/alpha_raptor_engine /usr/local/bin/alpha_raptor_engine

COPY alpha-raptor.toml ./alpha-raptor.toml

RUN mkdir -p /app/.alpha-raptor /app/data /shared

ENV ALPHA_BIND=0.0.0.0:7878

EXPOSE 7878

CMD ["/usr/local/bin/alpha_raptor_engine"]
