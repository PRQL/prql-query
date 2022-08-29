FROM rust:1.63 as builder

RUN apt-get update && apt-get install -y build-essential
WORKDIR /usr/src/prql
COPY ./Cargo.toml ./Cargo.toml
#COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src

RUN rustup component add rustfmt
RUN cargo build --release --all-features

FROM debian:bullseye-slim
RUN apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates && \
    # apt-get install -y extra-runtime-dependencies && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/prql/target/release/prql /usr/local/bin

ENTRYPOINT ["prql"]
