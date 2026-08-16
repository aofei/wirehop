// Package command implements the WireHop command-line interface.
package command

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/socketopts"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
	"github.com/aofei/wirehop/internal/wsheader"
)

var (
	// ErrUsage indicates missing or inconsistent invocation configuration.
	ErrUsage = errors.New("invalid command usage")
	// versionOverride is set by the linker for release builds.
	versionOverride string
)

const (
	// defaultHandshakeTimeout bounds carrier admission operations.
	defaultHandshakeTimeout = 5 * time.Second
	// defaultAuthenticationSkew bounds authenticated wall-clock timestamps.
	defaultAuthenticationSkew = 2 * time.Minute
	// defaultReconnectGrace retains detached server sessions.
	defaultReconnectGrace = 30 * time.Second
	// defaultControlDeadline bounds WireGuard handshake and cookie packets.
	defaultControlDeadline = 2 * time.Second
	// defaultTransportDeadline bounds WireGuard transport packets.
	defaultTransportDeadline = time.Second
	// defaultDeduplicationWindow bounds received packet sequence retention.
	defaultDeduplicationWindow = 1_048_576
	// defaultIngressPackets bounds one session ingress queue by packet count.
	defaultIngressPackets = 1024
	// defaultIngressBytes bounds one session ingress queue by byte count.
	defaultIngressBytes = 4 * 1024 * 1024
	// defaultLanePackets bounds one lane queue by packet count.
	defaultLanePackets = 16_384
	// defaultLaneBytes bounds one lane queue by byte count.
	defaultLaneBytes = 32 * 1024 * 1024
	// defaultRetainedPackets bounds aggregate server packet retention.
	defaultRetainedPackets = 262_144
	// defaultRetainedBytes bounds aggregate server accounted packet bytes.
	defaultRetainedBytes = 256 * 1024 * 1024
	// defaultReplayEntries bounds global creation nonces.
	defaultReplayEntries = 65_536
	// defaultJoinNonceEntries bounds join nonces retained by one session.
	defaultJoinNonceEntries = 4096
	// defaultMaxSessions bounds concurrent attached and detached sessions.
	defaultMaxSessions = 1024
	// defaultMaxLanesPerSession bounds stable lane identifiers per session.
	defaultMaxLanesPerSession = 16
	// defaultMaxPendingAdmissions bounds shared unauthenticated server handshake work.
	defaultMaxPendingAdmissions = 512
	// defaultTLSClientSessionCacheEntries bounds resumable TLS state retained by one client process.
	defaultTLSClientSessionCacheEntries = 64
	// rootUsage describes the stable top-level command shape.
	rootUsage = `Usage:
  wirehop client [options]
  wirehop server [options]
  wirehop version
  wirehop help [client|server|version]

Commands:
  client   Run a local WireGuard relay client
  server   Run a WireGuard relay server
  version  Print the WireHop version

Options:
  -h, --help  Show this help
  --version   Print the WireHop version

Run "wirehop help <command>" for command options.
`
	// clientUsage describes the client command and its complete option surface.
	clientUsage = `Usage:
  wirehop client --listen IP:PORT --target HOST:PORT --lane SPEC [--lane SPEC ...] [options]

Options:
  --listen IP:PORT          Local WireGuard UDP listen address (required)
  --target HOST:PORT        Remote WireGuard UDP target (required)
  --lane SPEC               Carrier URL or url=URL,resolve=IP declaration (required, repeatable, maximum 16)
  --reserved BASE64         Apply a fixed nonzero three-byte WireGuard reserved value
  --tls-server-name HOST    TLS hostname override for every secure lane
  --fwmark UINT32           Linux carrier and DNS socket firewall mark
  --allow-insecure          Allow tcp:// and ws:// lanes
  -h, --help                Show this help

Environment:
  WIREHOP_TOKEN                       Shared authentication token (required)
  HTTP_PROXY, HTTPS_PROXY, NO_PROXY   Standard WebSocket proxy selection
`
	// serverUsage describes the server command and its complete option surface.
	serverUsage = `Usage:
  wirehop server --listen URL --allow-target HOST:PORT [options]

Options:
  --listen URL              Carrier listen URL (required, repeatable)
  --allow-target HOST:PORT  Exact allowed WireGuard UDP target (required, repeatable)
  --tls-cert FILE           TLS certificate chain file (required for secure listeners)
  --tls-key FILE            TLS private key file (required for secure listeners)
  --allow-insecure          Allow tcp:// and ws:// listeners
  -h, --help                Show this help

Environment:
  WIREHOP_TOKEN             Shared authentication token (required)
`
	// versionUsage describes the version command.
	versionUsage = `Usage:
  wirehop version
`
)

// usageError carries a clean diagnostic and the subcommand whose help resolves it.
type usageError struct {
	command string
	message string
	cause   error
}

// Error returns the user-facing diagnostic without an internal sentinel prefix.
func (e *usageError) Error() string {
	return e.message
}

// Unwrap returns the underlying validation cause when one exists.
func (e *usageError) Unwrap() error {
	return e.cause
}

// Is classifies every usageError as [ErrUsage].
func (e *usageError) Is(target error) bool {
	return target == ErrUsage
}

// repeatFlag collects repeatable command-line values without deduplication.
type repeatFlag []string

// String returns a comma-separated diagnostic representation.
func (f *repeatFlag) String() string {
	return strings.Join(*f, ",")
}

// Set appends one flag occurrence exactly as supplied.
func (f *repeatFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// singleOption records whether one standard-library flag was set more than once.
type singleOption struct {
	name     string
	value    flag.Value
	seen     bool
	repeated bool
}

// String delegates the diagnostic representation to the underlying flag value.
func (o *singleOption) String() string {
	return o.value.String()
}

// Set updates the underlying flag value and records every successful occurrence after the first.
func (o *singleOption) Set(value string) error {
	if err := o.value.Set(value); err != nil {
		return err
	}
	o.repeated = o.seen
	o.seen = true
	return nil
}

// IsBoolFlag preserves the standard parser's valueless Boolean option handling.
func (o *singleOption) IsBoolFlag() bool {
	boolean, ok := o.value.(interface{ IsBoolFlag() bool })
	return ok && boolean.IsBoolFlag()
}

// Execute runs the command, renders one diagnostic on failure, and returns a process exit code.
func Execute(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	err := Run(ctx, args, getenv, stdout, stderr)
	if err == nil {
		return 0
	}
	diagnostics := stderr
	if diagnostics == nil {
		diagnostics = io.Discard
	}
	fmt.Fprintf(diagnostics, "wirehop: %v\n", err)
	if usage, ok := errors.AsType[*usageError](err); ok {
		command := ""
		if usage.command != "" {
			command = " " + usage.command
		}
		fmt.Fprintf(diagnostics, "Try 'wirehop%s --help' for more information.\n", command)
		return 2
	}
	return 1
}

// Run executes one WireHop subcommand.
func Run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	if getenv == nil || stdout == nil || stderr == nil {
		return newUsageError("", "invalid command environment", nil)
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		_, err := io.WriteString(stdout, rootUsage)
		return err
	}
	if len(args) == 1 && args[0] == "--version" {
		return writeVersion(stdout)
	}
	if len(args) == 0 {
		return newUsageError("", "expected client, server, or version subcommand", nil)
	}
	switch args[0] {
	case "help":
		return runHelp(args[1:], stdout)
	case "client":
		return runClient(ctx, args[1:], getenv, stdout, stderr)
	case "server":
		return runServer(ctx, args[1:], getenv, stdout, stderr)
	case "version":
		return runVersion(args[1:], stdout)
	default:
		return newUsageError("", fmt.Sprintf("unknown subcommand %q", args[0]), nil)
	}
}

// runVersion writes version output or command help.
func runVersion(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return writeVersion(stdout)
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		_, err := io.WriteString(stdout, versionUsage)
		return err
	}
	return newUsageError("version", fmt.Sprintf("unexpected argument %q", args[0]), nil)
}

// writeVersion writes the effective build version.
func writeVersion(output io.Writer) error {
	version := versionOverride
	if version == "" {
		version = "devel"
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
	}
	_, err := fmt.Fprintf(output, "wirehop %s\n", version)
	return err
}

// runHelp writes root or subcommand help selected by positional arguments.
func runHelp(args []string, stdout io.Writer) error {
	switch len(args) {
	case 0:
		_, err := io.WriteString(stdout, rootUsage)
		return err
	case 1:
		switch args[0] {
		case "client":
			_, err := io.WriteString(stdout, clientUsage)
			return err
		case "server":
			_, err := io.WriteString(stdout, serverUsage)
			return err
		case "version":
			_, err := io.WriteString(stdout, versionUsage)
			return err
		default:
			return newUsageError("", fmt.Sprintf("unknown help topic %q", args[0]), nil)
		}
	default:
		return newUsageError("", fmt.Sprintf("unexpected argument %q", args[1]), nil)
	}
}

// runClient parses and runs one multipath client session.
func runClient(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("wirehop client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	var lanes repeatFlag
	var listenValue string
	var targetValue string
	var reserved wgpacket.Reserved
	var tlsServerName string
	var firewallMark uint64
	var allowInsecure bool
	flags.Var(&lanes, "lane", "carrier URL or url=URL,resolve=IP declaration, repeatable")
	flags.StringVar(&listenValue, "listen", "", "local WireGuard UDP listen address")
	flags.StringVar(&targetValue, "target", "", "remote WireGuard UDP target")
	flags.TextVar(&reserved, "reserved", wgpacket.Reserved{}, "fixed nonzero three-byte WireGuard reserved value")
	flags.StringVar(&tlsServerName, "tls-server-name", "", "TLS certificate hostname override")
	flags.Uint64Var(&firewallMark, "fwmark", 0, "Linux carrier and DNS socket firewall mark")
	flags.BoolVar(&allowInsecure, "allow-insecure", false, "allow tcp:// and ws:// lanes")
	singleOptions := trackSingleOptions(
		flags, "listen", "target", "reserved", "tls-server-name", "fwmark", "allow-insecure",
	)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, writeErr := io.WriteString(stdout, clientUsage)
			return writeErr
		}
		return newUsageError("client", formatFlagError(err), err)
	}
	if err := rejectRepeatedOptions("client", singleOptions); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return newUsageError("client", fmt.Sprintf("unexpected argument %q", flags.Arg(0)), nil)
	}
	var missing []string
	if listenValue == "" {
		missing = append(missing, "--listen")
	}
	if targetValue == "" {
		missing = append(missing, "--target")
	}
	if len(lanes) == 0 {
		missing = append(missing, "--lane")
	}
	if len(missing) != 0 {
		return missingOptionsError("client", missing)
	}
	if len(lanes) > defaultMaxLanesPerSession {
		return newUsageError("client",
			fmt.Sprintf("at most %d --lane options are allowed", defaultMaxLanesPerSession), nil)
	}
	listen, err := netip.ParseAddrPort(listenValue)
	if err != nil {
		return newUsageError("client",
			fmt.Sprintf("invalid client listen address %q: expected an IP literal and port", listenValue), err)
	}
	if reason := invalidClientListenReason(listen); reason != "" {
		return newUsageError("client", fmt.Sprintf("invalid client listen address %q: %s", listenValue, reason), nil)
	}
	remoteTarget, err := target.Parse(targetValue)
	if err != nil {
		return newUsageError("client", fmt.Sprintf("invalid WireGuard target %q: %v", targetValue, err), err)
	}
	secure := false
	specs := make([]lanespec.Spec, 0, len(lanes))
	for index, value := range lanes {
		spec, err := lanespec.Parse(value)
		if err != nil {
			return newUsageError("client", fmt.Sprintf("--lane %d: %v", index+1, err), err)
		}
		url := spec.URL()
		if !url.Scheme().Secure() && !allowInsecure {
			return newUsageError("client",
				fmt.Sprintf("--lane %d: %s requires --allow-insecure", index+1, url.Scheme()), nil)
		}
		secure = secure || url.Scheme().Secure()
		specs = append(specs, spec)
	}
	if tlsServerName != "" && !secure {
		return newUsageError("client", "--tls-server-name requires a tls:// or wss:// lane", nil)
	}
	tlsConfig := newClientTLSConfig(tlsServerName)
	if firewallMark > uint64(^uint32(0)) {
		return newUsageError("client", "--fwmark exceeds 32 bits", nil)
	}
	dialer, err := socketopts.NewDialer(nil, uint32(firewallMark))
	if err != nil {
		if errors.Is(err, socketopts.ErrUnsupportedMark) {
			return newUsageError("client", "--fwmark is supported only on Linux", err)
		}
		return err
	}
	token := getenv("WIREHOP_TOKEN")
	if err := wsheader.ValidateBearerToken(token); err != nil {
		message := "WIREHOP_TOKEN is invalid"
		if token == "" {
			message = "WIREHOP_TOKEN is required"
		}
		return newUsageError("client", message, err)
	}
	if ctx.Err() != nil {
		return nil
	}
	instance, err := client.Start(context.WithoutCancel(ctx), client.Config{
		Lanes: specs, Listen: listen, Target: remoteTarget, Reserved: reserved, Token: []byte(token),
		TLSConfig: tlsConfig,
		Dialer:    dialer, Logger: slog.New(slog.NewTextHandler(stderr, nil)), MaxLanes: defaultMaxLanesPerSession,
		HandshakeTimeout:    defaultHandshakeTimeout,
		IngressLimits:       packetqueue.Limits{Packets: defaultIngressPackets, Bytes: defaultIngressBytes},
		LaneLimits:          packetqueue.Limits{Packets: defaultLanePackets, Bytes: defaultLaneBytes},
		Deadlines:           relay.DeadlinePolicy{Control: defaultControlDeadline, Transport: defaultTransportDeadline},
		DeduplicationWindow: defaultDeduplicationWindow,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, client.ErrInvalidConfig) {
			return newUsageError("client", err.Error(), err)
		}
		return err
	}
	defer instance.Close()
	if ctx.Err() != nil {
		return nil
	}
	if listen.Port() == 0 {
		if _, err := fmt.Fprintln(stdout, instance.LocalAddr()); err != nil {
			return fmt.Errorf("write client listen address: %w", err)
		}
	}
	result := make(chan error, 1)
	go func() { result <- instance.Wait() }()
	select {
	case err = <-result:
		if ctx.Err() != nil {
			return nil
		}
		return err
	case <-ctx.Done():
		return nil
	}
}

// newClientTLSConfig enables bounded session resumption for secure carriers and HTTPS proxies.
func newClientTLSConfig(serverName string) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		ClientSessionCache: tls.NewLRUClientSessionCache(defaultTLSClientSessionCacheEntries),
	}
}

// runServer parses listeners and runs one shared session registry.
func runServer(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("wirehop server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	var listenValues repeatFlag
	var targetValues repeatFlag
	var certificatePath string
	var keyPath string
	var allowInsecure bool
	flags.Var(&listenValues, "listen", "carrier listen URL, repeatable")
	flags.Var(&targetValues, "allow-target", "allowed WireGuard UDP target, repeatable")
	flags.StringVar(&certificatePath, "tls-cert", "", "TLS certificate chain file")
	flags.StringVar(&keyPath, "tls-key", "", "TLS private key file")
	flags.BoolVar(&allowInsecure, "allow-insecure", false, "allow tcp:// and ws:// listeners")
	singleOptions := trackSingleOptions(flags, "tls-cert", "tls-key", "allow-insecure")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, writeErr := io.WriteString(stdout, serverUsage)
			return writeErr
		}
		return newUsageError("server", formatFlagError(err), err)
	}
	if err := rejectRepeatedOptions("server", singleOptions); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return newUsageError("server", fmt.Sprintf("unexpected argument %q", flags.Arg(0)), nil)
	}
	var missing []string
	if len(listenValues) == 0 {
		missing = append(missing, "--listen")
	}
	if len(targetValues) == 0 {
		missing = append(missing, "--allow-target")
	}
	if len(missing) != 0 {
		return missingOptionsError("server", missing)
	}
	listeners := make([]laneurl.URL, 0, len(listenValues))
	listenerAddresses := make(map[string]int, len(listenValues))
	secure := false
	for index, value := range listenValues {
		url, err := laneurl.ParseListen(value)
		if err != nil {
			return newUsageError("server", fmt.Sprintf("--listen %d: %v", index+1, err), err)
		}
		if url.Scheme().Secure() {
			secure = true
		} else if !allowInsecure {
			return newUsageError("server",
				fmt.Sprintf("--listen %d: %s requires --allow-insecure", index+1, url.Scheme()), nil)
		}
		if previous, ok := listenerAddresses[url.Address()]; ok {
			return newUsageError("server", fmt.Sprintf(
				"--listen %d conflicts with --listen %d: both bind %s", index+1, previous, url.Address(),
			), nil)
		}
		listenerAddresses[url.Address()] = index + 1
		listeners = append(listeners, url)
	}
	if secure && (certificatePath == "" || keyPath == "") {
		return newUsageError("server", "--tls-cert and --tls-key are required for secure listeners", nil)
	}
	if !secure && (certificatePath != "" || keyPath != "") {
		return newUsageError("server", "--tls-cert and --tls-key require a tls:// or wss:// listener", nil)
	}
	targets := make([]target.Endpoint, 0, len(targetValues))
	for _, value := range targetValues {
		endpoint, err := target.Parse(value)
		if err != nil {
			return newUsageError("server", fmt.Sprintf("invalid allowed target %q: %v", value, err), err)
		}
		targets = append(targets, endpoint)
	}
	allowlist, err := policy.NewTargetSet(targets)
	if err != nil {
		return err
	}
	token := getenv("WIREHOP_TOKEN")
	if err := wsheader.ValidateBearerToken(token); err != nil {
		message := "WIREHOP_TOKEN is invalid"
		if token == "" {
			message = "WIREHOP_TOKEN is required"
		}
		return newUsageError("server", message, err)
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	instance, err := server.New(server.Config{
		Token: []byte(token), Targets: allowlist, Logger: logger,
		AuthenticationSkew: defaultAuthenticationSkew, HandshakeTimeout: defaultHandshakeTimeout,
		ReplayEntries: defaultReplayEntries, JoinNonceEntries: defaultJoinNonceEntries,
		MaxSessions: defaultMaxSessions, MaxLanesPerSession: defaultMaxLanesPerSession,
		MaxPendingAdmissions: defaultMaxPendingAdmissions,
		ReconnectGrace:       defaultReconnectGrace,
		IngressLimits:        packetqueue.Limits{Packets: defaultIngressPackets, Bytes: defaultIngressBytes},
		LaneLimits:           packetqueue.Limits{Packets: defaultLanePackets, Bytes: defaultLaneBytes},
		RetentionLimits:      retention.Limits{Packets: defaultRetainedPackets, Bytes: defaultRetainedBytes},
		Deadlines:            relay.DeadlinePolicy{Control: defaultControlDeadline, Transport: defaultTransportDeadline},
		DeduplicationWindow:  defaultDeduplicationWindow,
	})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	var certificate tls.Certificate
	if secure {
		loaded, err := tls.LoadX509KeyPair(certificatePath, keyPath)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		certificate = loaded
	}
	return serveListeners(ctx, instance, listeners, certificate, logger)
}

// newUsageError constructs a classified usage error with a clean display message.
func newUsageError(command, message string, cause error) error {
	return &usageError{command: command, message: message, cause: cause}
}

// missingOptionsError reports every absent required option in declaration order.
func missingOptionsError(command string, options []string) error {
	return newUsageError(command, "required options are missing: "+strings.Join(options, ", "), nil)
}

// formatFlagError normalizes standard-library diagnostics to the documented long-option spelling.
func formatFlagError(err error) string {
	message := err.Error()
	if name, ok := strings.CutPrefix(message, "flag provided but not defined: -"); ok {
		return fmt.Sprintf("unknown option %q", "--"+name)
	}
	if name, ok := strings.CutPrefix(message, "flag needs an argument: -"); ok {
		return fmt.Sprintf("option %q requires a value", "--"+name)
	}
	if strings.Contains(message, " for flag -") {
		return strings.Replace(message, " for flag -", " for --", 1)
	}
	return strings.Replace(message, " for -", " for --", 1)
}

// trackSingleOptions wraps existing flags so the same parser also records their occurrence counts.
func trackSingleOptions(flags *flag.FlagSet, names ...string) []*singleOption {
	options := make([]*singleOption, 0, len(names))
	for _, name := range names {
		defined := flags.Lookup(name)
		option := &singleOption{name: name, value: defined.Value}
		defined.Value = option
		options = append(options, option)
	}
	return options
}

// rejectRepeatedOptions rejects the first single-value option that occurred more than once.
func rejectRepeatedOptions(command string, options []*singleOption) error {
	for _, option := range options {
		if option.repeated {
			return newUsageError(command, fmt.Sprintf("--%s may be specified only once", option.name), nil)
		}
	}
	return nil
}

// runtimeListener owns one prepared carrier listener and optional HTTP server.
type runtimeListener struct {
	listener   net.Listener
	httpServer *http.Server
	handlers   *httpHandlerTracker
}

// httpHandlerTracker waits for hijacked WebSocket handlers that net/http no longer owns.
type httpHandlerTracker struct {
	mu       sync.Mutex
	active   int
	stopping bool
	done     chan struct{}
}

// newHTTPHandlerTracker returns an accepting handler tracker.
func newHTTPHandlerTracker() *httpHandlerTracker {
	return &httpHandlerTracker{done: make(chan struct{})}
}

// handler wraps one HTTP handler with command-lifetime ownership.
func (t *httpHandlerTracker) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.mu.Lock()
		if t.stopping {
			t.mu.Unlock()
			writer.Header().Set("Cache-Control", "no-store")
			http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		t.active++
		t.mu.Unlock()

		defer func() {
			t.mu.Lock()
			t.active--
			if t.stopping && t.active == 0 {
				close(t.done)
			}
			t.mu.Unlock()
		}()
		next.ServeHTTP(writer, request)
	})
}

// stop prevents new owned handlers and returns a channel closed after active handlers exit.
func (t *httpHandlerTracker) stop() <-chan struct{} {
	t.mu.Lock()
	t.stopping = true
	if t.active == 0 {
		close(t.done)
	}
	t.mu.Unlock()
	return t.done
}

// httpErrorWriter routes net/http diagnostics through the command logger while suppressing routine TLS scans.
type httpErrorWriter struct {
	logger *slog.Logger
}

// Write emits one recovered HTTP server failure as a structured warning.
func (w httpErrorWriter) Write(value []byte) (int, error) {
	message := strings.TrimSpace(string(value))
	if strings.HasPrefix(message, "http: TLS handshake error from ") {
		return len(value), nil
	}
	w.logger.Warn("HTTP server error", "error", strings.TrimPrefix(message, "http: "))
	return len(value), nil
}

// serveListeners prepares every socket before serving and tears all down on the first terminal result.
func serveListeners(ctx context.Context, instance *server.Server, urls []laneurl.URL,
	certificate tls.Certificate, logger *slog.Logger) error {
	runtimeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	prepared := make([]runtimeListener, 0, len(urls))
	listenConfig := net.ListenConfig{}
	for _, url := range urls {
		if ctx.Err() != nil {
			closeRuntimeListeners(prepared)
			return nil
		}
		listener, err := listenConfig.Listen(ctx, "tcp", url.Address())
		if err != nil {
			cancel()
			closeRuntimeListeners(prepared)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("listen on %s: %w", url, err)
		}
		configured := net.Listener(carrier.NewTCPOptionsListener(listener))
		if url.Scheme().WebSocket() {
			configured = instance.WebSocketListener(configured)
		}
		if url.Scheme().Secure() {
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
			}
			if url.Scheme().WebSocket() {
				tlsConfig.NextProtos = []string{"http/1.1"}
			}
			configured = tls.NewListener(configured, tlsConfig)
		}
		runtime := runtimeListener{}
		if url.Scheme().WebSocket() {
			protocols := new(http.Protocols)
			protocols.SetHTTP1(true)
			runtime.handlers = newHTTPHandlerTracker()
			handler := exactPathHandler(url.EscapedPath(), instance.WebSocketHandler(runtimeContext))
			runtime.httpServer = &http.Server{
				Handler:           runtime.handlers.handler(handler),
				Protocols:         protocols,
				ReadHeaderTimeout: defaultHandshakeTimeout, WriteTimeout: defaultHandshakeTimeout,
				MaxHeaderBytes: 16 * 1024,
				ConnContext:    instance.WebSocketConnContext,
				ErrorLog:       log.New(httpErrorWriter{logger: logger}, "", 0),
			}
			runtime.httpServer.SetKeepAlivesEnabled(false)
		}
		runtime.listener = configured
		prepared = append(prepared, runtime)
	}
	results := make(chan error, len(prepared))
	for index := range prepared {
		runtime := &prepared[index]
		go func() {
			if runtime.httpServer != nil {
				err := runtime.httpServer.Serve(runtime.listener)
				if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
					err = nil
				}
				results <- err
				return
			}
			results <- instance.Serve(runtimeContext, runtime.listener)
		}()
	}
	var result error
	received := 0
	select {
	case <-ctx.Done():
	case result = <-results:
		received = 1
	}
	cancel()
	closeRuntimeListeners(prepared)
	for received < len(prepared) {
		err := <-results
		received++
		if result == nil && err != nil {
			result = err
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return result
}

// closeRuntimeListeners closes every prepared socket and HTTP server.
func closeRuntimeListeners(listeners []runtimeListener) {
	done := make([]<-chan struct{}, 0, len(listeners))
	for index := range listeners {
		if listeners[index].handlers != nil {
			done = append(done, listeners[index].handlers.stop())
		}
	}
	for index := range listeners {
		if listeners[index].httpServer != nil {
			listeners[index].httpServer.Close()
		}
		listeners[index].listener.Close()
	}
	for _, handlerDone := range done {
		<-handlerDone
	}
}

// exactPathHandler rejects requests outside one configured WebSocket lane path.
func exactPathHandler(path string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		if request.URL.EscapedPath() != path || request.URL.ForceQuery || request.URL.RawQuery != "" {
			http.NotFound(writer, request)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// invalidClientListenReason describes a parsed address rejected by the client UDP bind policy.
func invalidClientListenReason(address netip.AddrPort) string {
	if address.Addr() != address.Addr().Unmap() {
		return "IPv4-mapped IPv6 addresses are not allowed"
	}
	if address.Addr().IsMulticast() {
		return "multicast addresses are not allowed"
	}
	return ""
}

// HostEnvironment returns the current process environment value for name.
func HostEnvironment(name string) string {
	return os.Getenv(name)
}
