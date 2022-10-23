ARG RUST_VERSION=1.64.0-slim-buster
ARG DEBIAN_VERSION=stable-slim

FROM rust:$RUST_VERSION AS build

# --- build-requirements ---
RUN apt-get -yq update \
    && apt install -y \
	build-essential \
	clang \
	libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# --- build from workdir ---
#WORKDIR /app
#COPY . .
#RUN cargo build --release && \
#    mv -v /app/target/release/pq /usr/local/cargo/bin/

# --- build from crates.io ---
#ARG PQ_VERSION
#RUN cargo install prql-query --version $PQ_VERSION
RUN cargo install prql-query

# --- image ---
FROM debian:$DEBIAN_VERSION
WORKDIR /data
COPY --from=build /usr/local/cargo/bin/pq /usr/local/bin/pq

ENTRYPOINT ["pq"]
