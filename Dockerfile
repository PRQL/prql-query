ARG RUST_VERSION=1.64.0-slim-buster
ARG DEBIAN_VERSION=stable-slim

# --- crates_io ---
# Once we have pq published on crates.io we can use this
#FROM rust:$RUST_VERSION AS crates_io
#ARG PQ_VERSION
#RUN cargo install pq --version $PQ_VERSION

# --- build ---
FROM rust:$RUST_VERSION AS build
RUN apt-get -yq update \
    && apt install -y \
	build-essential \
	clang \
	libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cargo build --release

# --- image ---
FROM debian:$DEBIAN_VERSION
WORKDIR /data
COPY --from=build /app/target/release/pq /usr/local/bin/pq

ENTRYPOINT ["pq"]
