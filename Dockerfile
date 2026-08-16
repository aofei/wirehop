FROM golang:1.26-alpine3.24 AS build

ARG TARGETPLATFORM
ARG USE_GORELEASER_ARTIFACTS=0

WORKDIR /usr/local/src/wirehop
COPY . .

RUN <<'EOF'
set -eux

mkdir -p bin

if [ "${USE_GORELEASER_ARTIFACTS}" -eq 1 ]; then
	cp -p "${TARGETPLATFORM}/bin/wirehop" bin/
else
	apk add --no-cache git
	go mod download
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/ ./cmd/wirehop
fi
EOF

FROM alpine:3.24

COPY --from=build /usr/local/src/wirehop/bin/ /usr/local/bin/

RUN apk add --no-cache ca-certificates

WORKDIR /wirehop

USER nobody
ENTRYPOINT ["/usr/local/bin/wirehop"]
