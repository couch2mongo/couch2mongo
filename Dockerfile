FROM public.ecr.aws/docker/library/rust:1.70-bullseye AS builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

#############

FROM public.ecr.aws/docker/library/debian:bullseye-slim AS runtime

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/target/release/streamcouch .

CMD ["./streamcouch"]