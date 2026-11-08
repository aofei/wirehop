FROM golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0 AS tools

WORKDIR /src
COPY test/integration/iperf3-test.patch ./
RUN curl --fail --location --silent --show-error \
      https://downloads.es.net/pub/iperf/iperf-3.18.tar.gz -o /tmp/iperf.tar.gz && \
    echo 'c0618175514331e766522500e20c94bfb293b4424eb27d7207fb427b88d20bab  /tmp/iperf.tar.gz' | sha256sum -c - && \
    tar -xzf /tmp/iperf.tar.gz -C /tmp && \
    cd /tmp/iperf-3.18 && \
    git apply /src/iperf3-test.patch && \
    ./configure --disable-shared --without-openssl && \
    make -j2

COPY go.mod go.sum ./
RUN go mod download
COPY test/integration/flow/ ./test/integration/flow/
RUN CGO_ENABLED=0 go build -trimpath -o /wirehop-test-flow ./test/integration/flow

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

COPY --from=tools /wirehop-test-flow /tmp/iperf-3.18/src/iperf3 /usr/local/bin/
COPY --from=tools /tmp/iperf-3.18/LICENSE /usr/local/share/licenses/iperf3/LICENSE

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    iproute2 openssl wireguard-tools && \
    rm -rf /var/lib/apt/lists/*
