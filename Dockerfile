FROM rust:1.87-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY public ./public
COPY alpha-raptor.toml ./alpha-raptor.toml

RUN cargo build --release --locked

RUN mkdir -p /app/.alpha-raptor /app/data /shared

ENV ALPHA_BIND=0.0.0.0:7878

EXPOSE 7878

CMD ["cargo", "run", "--release", "--locked"]
