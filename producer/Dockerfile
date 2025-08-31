FROM rust:1.88-bookworm as build

#ARG upx_version=5.0.2
#
#RUN apt-get update && apt-get install -y --no-install-recommends xz-utils && \
#  curl -Ls https://github.com/upx/upx/releases/download/v${upx_version}/upx-${upx_version}-amd64_linux.tar.xz -o - | tar xvJf - -C /tmp && \
#  cp /tmp/upx-${upx_version}-amd64_linux/upx /usr/local/bin/ && \
#  chmod +x /usr/local/bin/upx && \
#  apt-get remove -y xz-utils && \
#  rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

#RUN cargo build --relese && upx --best /app/target/release/tasks-queue-producer
RUN cargo build

FROM debian:12.6-slim

COPY --from=build /app/target/debug/tasks-queue-producer /producer

CMD ["/producer"]