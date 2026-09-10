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
never decrypts or authenticates WireGuard cryptographic content and preserves complete datagrams by default.

See the [design document](docs/design.md) for the scheduling model, wire protocol, and recovery rules.

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
`WireHop-Rejection` response header. It must not cache or intercept WireHop admission responses. For nginx, use
`proxy_intercept_errors off` in the WireHop location.

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

WireHop requires Go 1.27 or later.

```sh
go build ./cmd/wirehop
go test -race ./...
```

Ordinary Go tests require neither a kernel WireGuard interface nor administrator privileges. The optional
[kernel WireGuard integration tests](test/integration/README.md) use isolated Docker containers to check TCP and UDP
traffic, route failure recovery, and socket interruption handling.

## Basic usage

### Client and server

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

### Direct UDP forwarding

When no TCP-based carrier is needed, forward directly to the remote WireGuard UDP endpoint:

```sh
wirehop forward \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820
```

Configure the local WireGuard peer endpoint as `127.0.0.1:51821`. The forwarder resolves the target locally and needs no
WireHop server or `WIREHOP_TOKEN`.

### Startup and diagnostics

Each client or forward listener serves one local WireGuard UDP source at a time. Replies go to the address and port of
the latest structurally valid local packet, allowing source changes after a restart. Give concurrent local WireGuard
devices separate listeners and instances, since sharing one listener can send replies to the wrong device.

When the requested `--listen` port is `0`, the client prints the selected UDP address to standard output as soon as the
socket is ready. Carrier establishment continues asynchronously, and fresh WireGuard packets remain in a bounded
priority queue while the first lane is starting. The `forward` command prints a dynamic address only after its target is
also ready. Successful startup is silent for a fixed-port client, a server, and a fixed-port direct forwarder. Graceful
shutdown is also silent.

The client independently retries recoverable connection and session-creation failures on each lane, including DNS
failures, without an overall startup deadline. The direct forwarder likewise keeps retrying target DNS failures. Each
attempt is bounded and retries use jittered backoff. Prolonged preparation failures produce rate-limited warnings, with
one recovery message if a warning was emitted. Repeated short-lived lane connections follow the same warning policy, and
recovery requires 30 seconds of sustained service. UDP socket errors also produce rate-limited diagnostics. Invalid
options, local bind failures, and terminal rejections are not treated as transient DNS failures. Server listeners must
all be prepared within a shared 30-second budget before any begin serving.

Help is written to standard output. Diagnostics and warning logs are written to standard error. Command-line and
environment validation errors exit with status `2`, runtime failures exit with status `1`, and help or signal-driven
graceful shutdown exits with status `0`.

## WireGuard MTU and UDP receive capacity

### WireGuard interface MTU

Set an explicit MTU on the WireGuard interface whose endpoint points at WireHop. With `wg-quick`, automatic MTU
detection can follow the route to `127.0.0.1` or `::1` and derive an oversized MTU from loopback. The actual UDP leg
between WireHop and the remote WireGuard peer can have a much smaller path MTU.

For example, when every external UDP leg has a path MTU of at least 1500 bytes, use:

```ini
[Interface]
MTU = 1420
```

Choose a smaller value when the actual UDP path requires it. Check the active interface with `ip link show wg0` and its
endpoint with `wg show wg0 endpoints`. WireHop preserves complete encrypted datagrams and cannot change the inner
WireGuard interface MTU or clamp encrypted TCP MSS. TCP carrier frames can span TCP segments, so the outer TCP MSS alone
does not determine the WireGuard MTU.

### Carrier overhead

Each WireGuard datagram receives a 16-byte WireHop data header, a one-byte frame type, and a four-byte content length.
The exact WireHop framing overhead is therefore 21 bytes per datagram. TCP/IP, TLS records, and WebSocket frames add
carrier overhead. Coalescing can amortize TLS and WebSocket overhead across several already-ready WireHop frames without
waiting for more traffic.

WireHop framing and TCP, TLS, or WebSocket headers are absent from the datagram delivered to WireGuard, so do not
subtract them again from that UDP path's MTU. TCP segmentation and write coalescing do not preserve WireHop frame
boundaries. Smaller packets can change loss recovery and latency, but tuning requires workload measurements rather than
assuming one frame per TCP segment.

The `forward` command uses direct UDP and adds no WireHop framing or packet-length overhead. Reserved translation does
not change the WireGuard datagram length.

### UDP receive capacity and errors

WireHop requests a 4 MiB receive buffer for each local and target UDP socket. This absorbs bursts while retaining system
socket limits. Linux may silently cap the request at `net.core.rmem_max`, and a rejected request retains the OS default
with a warning. Check `sysctl net.core.rmem_max`, `ss -u -a -m`, and `nstat -az UdpRcvbufErrors UdpSndbufErrors` on the
machine or network namespace running WireHop when evaluating sustained throughput. A rising `UdpRcvbufErrors` counter
indicates kernel receive drops even if no UDP operation returned an error. WireHop does not modify system-wide limits.

On servers, account for up to two target UDP sockets per session when adjusting system receive limits. Kernel socket
memory is separate from WireHop's aggregate packet retention budget.

Temporary UDP network failures drop affected datagrams while preserving the endpoint for later traffic. This includes
ICMP errors, route rejection or blackholing, buffer pressure, and write deadlines. The same recovery policy applies to
the client, server, and direct forwarder. It preserves the socket, but cannot prevent inner connections from timing out
if the network remains unavailable long enough.

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

The `forward` command has the same route-exclusion requirement when the local WireGuard configuration captures the
default route. The kernel knows only the local forwarding address, so the forwarder's real target traffic must bypass
that tunnel. Its `--fwmark` applies to upstream UDP sockets and DNS resolver sockets:

```sh
wirehop forward \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820 \
  --fwmark 51820
```

Ensure the target never resolves to an address and port covered by the forwarder's own listener. Otherwise, outbound
datagrams re-enter the listener and form a local UDP feedback loop.

The corresponding policy-routing rule is deployment-specific. On other platforms, deployments require external routes
that exclude carrier endpoints, direct forwarding targets, forward proxies, and the DNS paths used to resolve them.

Setting `--fwmark` requires `CAP_NET_ADMIN`, or `CAP_NET_RAW` on Linux 5.17 and later. A non-root process or container
must receive that capability explicitly. The option marks sockets but does not install policy-routing rules.

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

Start with one lane and measure each candidate separately. Two repeated declarations can isolate a single carrier stream
stall, but additional connections do not establish additional physical bandwidth. Paths with different delays can
reorder packets and reduce inner TCP goodput below the best single-lane result. WireHop forwards received datagrams
without waiting for packet ID order, and a higher-capacity lane can remain underused when the offered traffic keeps the
preferred lane's queue short. Compare sustained goodput and latency in both directions, with both single and concurrent
inner flows, before retaining extra lanes.

The client races carrier preparation and session admission across configured lanes, selects the first successful
candidate, and cancels the others. Only the selected session carries WireGuard packets. The remaining lanes reconnect
and join that session concurrently. Lane-scoped rejections close only the affected lane while healthy lanes continue.
Session-scoped failures coordinate complete session replacement or termination. Retryable lane failures reconnect
independently with increasing generations and full-jitter backoff. After the first session succeeds, retryable session
replacement continues without an overall recovery deadline. A permanent lane failure writes one warning when another
lane supervisor keeps the client running. Individual lane reconnects remain silent.

## Target resolution and failover

`--target` and `--allow-target` accept an IP literal or ASCII DNS hostname with an explicit, nonzero UDP port. For a
client session, the server authorizes the canonical logical target before resolving it through the server's name service
and network view. The `forward` command resolves its target locally and has no target allowlist.

DNS target names are resolved as absolute names, without appending resolver search suffixes. Use the complete DNS name
when the deployment normally relies on a search domain, such as a Kubernetes service's full cluster DNS name. Carrier
hostnames use the dialer's normal resolver behavior.

A DNS target may return multiple IPv4 and IPv6 addresses. All records must represent the same logical WireGuard peer,
whether they reach one dual-stack server or multiple servers configured with the same WireGuard identity. WireHop
initially sends handshake initiations to the bounded candidate set and uses WireGuard's public sender and receiver
indexes to route responses and subsequent transport data to the candidate selected by the local WireGuard
implementation. Transport data is never sprayed across candidates.

Handshake Initiation packets and target-side UDP errors request rate-limited DNS refreshes without delaying the current
packet. A successful refresh replaces the handshake fan-out set, while a lookup failure retains the last successful
result. Established transport traffic keeps its selected candidate across answer reordering and rotating subsets. A
routine rekey first uses that confirmed candidate. Only an unanswered handshake retry after five seconds discovers
alternatives. Delayed old-key traffic cannot move the confirmed selection back to an earlier backend. Backends sharing a
WireGuard identity still have independent NAT and TCP connection state, so failover cannot preserve that state.

The direct forwarder binds its local listener immediately and discards packets while waiting for initial target
resolution. Missing records and answers containing no usable target addresses remain retryable. Target socket
preparation errors are fatal. Target address changes do not replace a WireHop session, reconnect carrier lanes, or
restart a direct forwarder.

## Reserved field translation

WireHop recognizes the one-byte WireGuard message type independently from the following three-byte reserved field. When
`--reserved` is omitted, it preserves all three reserved bytes in both directions. A WireGuard implementation that
already handles a nonzero reserved value therefore uses WireHop transparently and must leave `--reserved` unset.

The client and direct forwarder can instead translate between a standard local WireGuard implementation and a remote
endpoint that applies the inverse boundary translation for one fixed nonzero reserved value. `--reserved` accepts the
canonical Base64 encoding of exactly three bytes and rejects the all-zero value. For example, `AQID` represents the
bytes `01 02 03`:

```sh
wirehop client \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820 \
  --lane wss://relay.example.com/_wirehop \
  --reserved AQID
```

The same option enables translation on the [direct UDP forwarding](#direct-udp-forwarding) path:

```sh
wirehop forward \
  --listen 127.0.0.1:51821 \
  --target wg.example.com:51820 \
  --reserved AQID
```

With `--reserved`, the local WireGuard implementation must use the standard zero reserved field. The client or forwarder
overwrites that field on packets read from the local endpoint. On packets returning from the target, it requires the
configured value and clears the field before local UDP delivery. A mismatch is dropped as an invalid target datagram. A
WireHop server remains unaware of a client's configured value and relays the complete on-wire packet unchanged. The
reserved field is public header metadata rather than authentication material.

## Security model

The server permits only exact canonical `--allow-target` host and port entries. Authorizing a DNS target delegates its
address selection to the server's configured name service, so the operator must trust that hostname's DNS authority and
all returned records. Session creation uses the long-term token. The server returns a random ephemeral session secret
for subsequent lane joins. Raw stream handshakes and joins use HMAC-SHA256, fresh nonces, bounded timestamp skew, replay
caches, stable lane identifiers, and strictly increasing connection generations. Authenticated responses bind the
request nonce and server time. If an otherwise authenticated request falls outside the timestamp window, the client
learns the server time and retries without changing the system clock.

The client and forward UDP listeners have no WireHop authentication. Bind them to loopback or another protected
interface unless deliberately serving trusted hosts. A reachable sender can consume forwarding capacity with
structurally valid WireGuard packets even though the remote WireGuard endpoint still authenticates and rejects forged
ciphertext. Direct forwarding relies only on WireGuard for remote packet authentication and does not use the WireHop
admission protocol.

WireGuard packets are already encrypted, but TLS still protects authorization material, target metadata, session
secrets, lifecycle controls, timing information, and the carrier against observable manipulation. TLS carriers require
TLS 1.2 or later. Raw HMAC admission does not encrypt the returned session secret or later control frames, and `ws://`
sends the bearer token in its HTTP request. Use `tls://` or `wss://` on untrusted networks.
