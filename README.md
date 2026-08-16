# WireHop

[![Test](https://github.com/aofei/wirehop/actions/workflows/test.yaml/badge.svg)](https://github.com/aofei/wirehop/actions/workflows/test.yaml)
[![codecov](https://codecov.io/gh/aofei/wirehop/branch/master/graph/badge.svg)](https://codecov.io/gh/aofei/wirehop)
[![Go Reference](https://pkg.go.dev/badge/github.com/aofei/wirehop.svg)](https://pkg.go.dev/github.com/aofei/wirehop)

A multipath relay for WireGuard over TCP-based transports.

WireHop carries WireGuard UDP packets across one or more long-lived TCP, TLS, WebSocket, or secure WebSocket lanes. It
is intended for networks where native UDP is blocked, unstable, rate limited, or otherwise less usable than available
TCP paths.

WireHop is WireGuard-aware but is not a WireGuard peer. It classifies public WireGuard message structures so it can
prioritize handshake traffic, duplicate control packets, enforce short packet deadlines, and schedule transport data. It
never decrypts or authenticates WireGuard cryptographic content. WireHop preserves complete datagrams unless the client
is configured to translate the public three-byte WireGuard reserved field for a standard local WireGuard implementation.

## Carrier schemes

| Scheme | Carrier | Intended use |
| --- | --- | --- |
| `tcp://` | Raw TCP | Trusted networks and performance baselines |
| `tls://` | TLS over TCP | Direct production connections |
| `ws://` | WebSocket over TCP | Trusted networks and local reverse-proxy tests |
| `wss://` | WebSocket over TLS | Restricted networks and production reverse proxies |

Plaintext `tcp://` and `ws://` endpoints require the explicit `--allow-insecure` flag. WebSocket compression is always
disabled because WireGuard ciphertext is not usefully compressible.

`ws://` and `wss://` lanes honor the standard `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` environment variables. Secure
WebSocket lanes use HTTPS proxy selection and a CONNECT tunnel through HTTP or HTTPS proxies. Selected `socks5` and
`socks5h` proxy URLs use native SOCKS tunneling.

A reverse proxy must preserve the exact path, admission request headers, WebSocket Upgrade, selected subprotocol, and
`WireHop-Rejection` response header. It must not cache or intercept WireHop admission responses.

WebSocket URLs use port `80` for `ws://` and port `443` for `wss://` when the port is omitted. Raw `tcp://` and `tls://`
URLs require an explicit port.

Client lanes may fix the DNS result for one logical hostname:

```sh
--lane 'url=wss://relay.example.com/_wirehop,resolve=203.0.113.10'
```

`resolve` accepts one unbracketed IPv4 or IPv6 address without a zone. IPv6 link-local addresses therefore cannot be
used as fixed results. It changes only the socket destination. Unless `--tls-server-name` explicitly overrides TLS
identity, the URL continues to control the port, HTTP Host, TLS SNI, certificate hostname, carrier scheme, and WebSocket
path. A resolved WebSocket lane must connect directly. If proxy selection finds a forward proxy, WireHop rejects the
configuration and requires a matching `NO_PROXY` entry.

## Build and test

WireHop requires Go 1.26 or later.

```sh
go build ./cmd/wirehop
go test -race ./...
```

Tests require neither a kernel WireGuard interface nor administrator privileges.

## Basic usage

Set the same high-entropy token on the client and server:

```sh
export WIREHOP_TOKEN="$(openssl rand -hex 32)"
```

The token must use the RFC 6750 Bearer token character set and may contain at most 4096 bytes. The hexadecimal command
above produces a compatible 256-bit random token.

Start a secure WebSocket server and allow one exact logical WireGuard UDP target:

```sh
wirehop server \
  --listen wss://:443/_wirehop \
  --tls-cert /path/to/fullchain.pem \
  --tls-key /path/to/private-key.pem \
  --allow-target wg.example.com:51820
```

Start a client-side UDP listener:

```sh
wirehop client \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820 \
  --lane wss://relay.example.com/_wirehop
```

Configure the local WireGuard peer endpoint as `127.0.0.1:51821`. The client authenticates the logical target to the
server, and the server resolves and reaches it from the server network.

When the requested `--listen` port is `0`, the client prints the selected UDP address to standard output as soon as the
socket is ready. Carrier establishment continues asynchronously, and fresh WireGuard packets remain in a bounded
priority queue while the first lane is starting. Successful fixed-port client startup, server startup, and graceful
shutdown are silent.

Help is written to standard output. Diagnostics and warning logs are written to standard error. Command-line and
environment validation errors exit with status `2`, runtime failures exit with status `1`, and help or signal-driven
graceful shutdown exits with status `0`.

## Reserved field translation

WireHop recognizes the one-byte WireGuard message type independently from the following three-byte reserved field. When
`--reserved` is omitted, it preserves all three reserved bytes in both directions. A WireGuard implementation that
already handles a nonzero reserved value therefore uses WireHop transparently and must leave `--reserved` unset.

The client can instead translate between a standard local WireGuard implementation and a remote endpoint that applies
the inverse boundary translation for one fixed nonzero reserved value. `--reserved` accepts the canonical Base64
encoding of exactly three bytes and rejects the all-zero value. For example, `AQID` represents the bytes `01 02 03`:

```sh
wirehop client \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820 \
  --lane wss://relay.example.com/_wirehop \
  --reserved AQID
```

In this mode, the local WireGuard implementation must use the standard zero reserved field. The client overwrites that
field on packets read from the local endpoint. On packets returning from the target, it requires the configured value
and clears the field before local UDP delivery. A mismatch is dropped as an invalid target datagram. The server remains
unaware of the configured value and relays the complete on-wire packet unchanged. The reserved field is public header
metadata rather than authentication material.

## Target resolution and failover

`--target` and `--allow-target` accept an IP literal or ASCII DNS hostname with an explicit, nonzero UDP port. The
server authorizes the canonical logical target before resolving it. Resolution uses only the server's name service and
network view.

A DNS target may return multiple IPv4 and IPv6 addresses. All records must represent the same logical WireGuard peer,
whether they reach one dual-stack server or multiple servers configured with the same WireGuard identity. WireHop sends
handshake initiations to the bounded candidate set and uses WireGuard's public sender and receiver indexes to route
responses and subsequent transport data to the candidate selected by the local WireGuard implementation. Transport data
is never sprayed across candidates.

Handshake Initiation packets and target-side UDP errors request rate-limited DNS refreshes without delaying the current
packet. A successful refresh replaces the handshake fan-out set, while a temporary lookup failure retains the last
successful result. Transport affinity moves only after a successful WireGuard handshake with another candidate. Target
address changes do not replace the WireHop session or reconnect its carrier lanes.

## Multipath lanes

Repeat `--lane` to create independent full-duplex carrier connections in one session:

```sh
wirehop client \
  --listen 127.0.0.1:51821 \
  --target 203.0.113.10:51820 \
  --lane 'url=wss://relay.example.com/_wirehop,resolve=203.0.113.10' \
  --lane 'url=wss://relay.example.com/_wirehop,resolve=203.0.113.11' \
  --lane 'url=wss://relay.example.com/_wirehop,resolve=2001:db8::10'
```

Each flag occurrence is one stable lane identity and one TCP connection. Repeating the same canonical URL and `resolve`
value creates multiple connections in the same path group. A different URL or fixed resolution creates a different path
group. WireHop prefers a stable low-delay lane for sparse transport traffic, spills load when predicted queueing makes
another lane faster, and sends at most two copies of a WireGuard control packet. The second copy prefers another path
group.

The client prepares all configured first-hop connections concurrently. Only one prepared lane may create the session.
The remaining prepared lanes join that session concurrently. Lane-scoped rejections close only the affected lane while
healthy lanes continue. Session-scoped failures coordinate complete session replacement or termination. Retryable lane
failures reconnect independently with increasing generations and full-jitter backoff. After the first session succeeds,
retryable session replacement continues without an overall recovery deadline. A permanent lane failure writes one
warning when another lane supervisor keeps the client running. Retryable connection failures remain silent.

## Route exclusion

Carrier traffic must not enter the WireGuard tunnel that depends on WireHop. On Linux, `--fwmark` applies `SO_MARK` to
every carrier socket before `connect` and to DNS resolver sockets used for carrier hostnames:

```sh
wirehop client \
  --listen 127.0.0.1:51821 \
  --target 203.0.113.10:51820 \
  --lane tls://relay.example.com:443 \
  --fwmark 51820
```

The corresponding policy-routing rule is deployment-specific. On other platforms, deployments require external routes
that exclude carrier endpoints, forward proxies, and the DNS path used to resolve their hostnames.

## Overhead and MTU

Each WireGuard datagram receives a 16-byte WireHop data header, a one-byte frame type, and a four-byte content length.
The exact WireHop framing overhead is therefore 21 bytes per datagram. TCP/IP, TLS records, and WebSocket frames add
carrier overhead. Coalescing can amortize TLS and WebSocket overhead across several already-ready WireHop frames without
waiting for more traffic.

WireHop does not change the WireGuard interface MTU. A deployment should account for the most restrictive active path,
IP family, TCP options, the 21-byte WireHop frame overhead, WireGuard's own transport overhead, and any TLS or WebSocket
framing. Correctness does not depend on one frame fitting one TCP segment, but a conservative MTU reduces TCP segment
loss amplification and head-of-line delay.

## Security model

The server permits only exact canonical `--allow-target` host and port entries. Authorizing a DNS target delegates its
address selection to the server's configured name service, so the operator must trust that hostname's DNS authority and
all returned records. Session creation uses the long-term token. The server returns a random ephemeral session secret
for subsequent lane joins. Raw stream handshakes and joins use HMAC-SHA256, fresh nonces, bounded timestamp skew, replay
caches, stable lane identifiers, and strictly increasing connection generations. Authenticated responses bind the
request nonce and server time. If an otherwise authenticated request falls outside the timestamp window, the client
learns the server time and retries without changing the system clock.

The client UDP listener has no WireHop authentication. Bind it to loopback or another protected interface unless
deliberately serving trusted hosts. A reachable sender can consume relay capacity with structurally valid WireGuard
packets even though the remote WireGuard endpoint still authenticates and rejects forged ciphertext.

WireGuard packets are already encrypted, but TLS still protects authorization material, target metadata, session
secrets, lifecycle controls, timing information, and the carrier against observable manipulation. TLS carriers require
TLS 1.2 or later. Raw HMAC admission does not encrypt the returned session secret or later control frames, and `ws://`
sends the bearer token in its HTTP request. Use `tls://` or `wss://` on untrusted networks.
