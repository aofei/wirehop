// Command latency measures fixed-cadence UDP round-trip latency during an independent load.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// probeMagic identifies latency probes independently of their sequence.
	probeMagic = "WHLAT001"
	// probeHeaderSize includes the magic and a big-endian sequence number.
	probeHeaderSize = 16
	// maximumDatagramSize is the largest IPv4 UDP payload.
	maximumDatagramSize = 65_507
	// maximumProbes bounds timestamp and sample storage to approximately 17 MB.
	maximumProbes = 1_000_000
)

// options contains one listener or probe invocation.
type options struct {
	listen   string
	target   string
	size     int
	duration time.Duration
	interval time.Duration
	drain    time.Duration
}

// result records unique replies and monotonic local round-trip measurements.
type result struct {
	Sent                     uint64  `json:"sent"`
	Received                 uint64  `json:"received"`
	Lost                     uint64  `json:"lost"`
	Duplicates               uint64  `json:"duplicates"`
	Unexpected               uint64  `json:"unexpected"`
	P50RTTMillis             float64 `json:"p50_rtt_ms"`
	P95RTTMillis             float64 `json:"p95_rtt_ms"`
	P99RTTMillis             float64 `json:"p99_rtt_ms"`
	MaxRTTMillis             float64 `json:"max_rtt_ms"`
	RequestedDurationSeconds float64 `json:"requested_duration_seconds"`
	ActualDurationSeconds    float64 `json:"actual_duration_seconds"`
	RequestedIntervalSeconds float64 `json:"requested_interval_seconds"`
	ActualIntervalSeconds    float64 `json:"actual_interval_seconds"`
	RequestedSizeBytes       int     `json:"requested_size_bytes"`
	ActualSizeBytes          int     `json:"actual_size_bytes"`
	DrainTimeoutSeconds      float64 `json:"drain_timeout_seconds"`
}

// replyTracker owns sequence-indexed replies while the sender publishes local timestamps atomically.
type replyTracker struct {
	size       int
	sentAt     []atomic.Int64
	seen       []bool
	rtts       []time.Duration
	received   uint64
	duplicates uint64
	unexpected uint64
}

// sendWindow retains the completed send count and final socket-write completion time.
type sendWindow struct {
	count int
	last  time.Time
}

// main parses flags and runs one isolated-network latency listener or client.
func main() {
	var config options
	flag.StringVar(&config.listen, "listen", "", "UDP address to echo valid probes")
	flag.StringVar(&config.target, "target", "", "UDP listener address to probe")
	flag.IntVar(&config.size, "size", 64, "Datagram size in bytes, from 16 through 65507")
	flag.DurationVar(&config.duration, "duration", 20*time.Second, "Probe sending duration")
	flag.DurationVar(&config.interval, "interval", 10*time.Millisecond, "Requested interval between probes")
	flag.DurationVar(&config.drain, "drain", 2*time.Second, "Reply timeout after the final successful send")
	flag.Parse()
	if err := config.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if config.listen != "" {
		err = serve(ctx, config)
	} else {
		var measured result
		measured, err = probe(ctx, config)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(measured)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// validate enforces the invocation mode and bounded probe storage.
func (c options) validate() error {
	if (c.listen == "") == (c.target == "") {
		return errors.New("specify exactly one of -listen and -target")
	}
	if c.size < probeHeaderSize || c.size > maximumDatagramSize {
		return fmt.Errorf("size must be between %d and %d bytes", probeHeaderSize, maximumDatagramSize)
	}
	if c.duration <= 0 || c.interval <= 0 || c.drain < 0 {
		return errors.New("duration and interval must be positive and drain must be nonnegative")
	}
	if c.probeCount() > maximumProbes {
		return fmt.Errorf("duration and interval must request at most %d probes", maximumProbes)
	}
	return nil
}

// probeCount includes the initial probe and every interval strictly before the sending deadline.
func (c options) probeCount() int64 {
	count := int64(c.duration / c.interval)
	if c.duration%c.interval != 0 {
		count++
	}
	return count
}

// parseProbe accepts only exact-sized, nonzero-sequence latency datagrams.
func parseProbe(payload []byte, size int) (uint64, bool) {
	if len(payload) != size || string(payload[:len(probeMagic)]) != probeMagic {
		return 0, false
	}
	sequence := binary.BigEndian.Uint64(payload[len(probeMagic):probeHeaderSize])
	return sequence, sequence != 0
}

// serve echoes valid probes until its owning context closes the UDP socket.
func serve(ctx context.Context, config options) error {
	address, err := net.ResolveUDPAddr("udp", config.listen)
	if err != nil {
		return fmt.Errorf("resolve listener: %w", err)
	}
	connection, err := net.ListenUDP("udp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer connection.Close()
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	buffer := make([]byte, config.size+1)
	for {
		length, peer, err := connection.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive listener datagram: %w", err)
		}
		if _, ok := parseProbe(buffer[:length], config.size); !ok {
			continue
		}
		written, err := connection.WriteToUDPAddrPort(buffer[:length], peer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("echo probe: %w", err)
		}
		if written != length {
			return fmt.Errorf("echo probe: %w", io.ErrShortWrite)
		}
	}
}

// probe sends independently of reply processing and drains until the last send's fixed timeout.
func probe(parent context.Context, config options) (result, error) {
	address, err := net.ResolveUDPAddr("udp", config.target)
	if err != nil {
		return result{}, fmt.Errorf("resolve target: %w", err)
	}
	connection, err := net.DialUDP("udp", nil, address)
	if err != nil {
		return result{}, fmt.Errorf("dial target: %w", err)
	}
	defer connection.Close()
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	count := int(config.probeCount())
	tracker := replyTracker{
		size: config.size, sentAt: make([]atomic.Int64, count+1),
		seen: make([]bool, count+1), rtts: make([]time.Duration, count+1),
	}
	payload := make([]byte, config.size)
	copy(payload, probeMagic)
	buffer := make([]byte, config.size+1)
	start := time.Now()
	if err := connection.SetWriteDeadline(start.Add(config.duration)); err != nil {
		return result{}, fmt.Errorf("set sending deadline: %w", err)
	}
	replies := make(chan error, 1)
	go func() {
		err := tracker.receive(connection, buffer, start)
		if err != nil {
			cancel(err)
		}
		replies <- err
	}()
	window, err := send(ctx, connection, config, tracker.sentAt, payload, start)
	actualDuration := time.Since(start)
	if err != nil {
		connection.Close()
		<-replies
		return result{}, err
	}
	drainDeadline := start
	if window.count > 0 {
		drainDeadline = window.last.Add(config.drain)
	}
	if err := connection.SetReadDeadline(drainDeadline); err != nil {
		connection.Close()
		<-replies
		if ctx.Err() != nil {
			return result{}, context.Cause(ctx)
		}
		return result{}, fmt.Errorf("set reply deadline: %w", err)
	}
	if err := <-replies; err != nil {
		return result{}, context.Cause(ctx)
	}
	return tracker.summarize(window.count, config, actualDuration), nil
}

// send uses its own ticker so reply loss and delayed replies cannot slow probe scheduling.
func send(ctx context.Context, connection *net.UDPConn, config options,
	sentAt []atomic.Int64, payload []byte, start time.Time) (sendWindow, error) {
	end := start.Add(config.duration)
	ticker := time.NewTicker(config.interval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Until(end))
	defer timer.Stop()
	var window sendWindow
	for time.Now().Before(end) {
		if ctx.Err() != nil {
			return window, context.Cause(ctx)
		}
		sequence := window.count + 1
		binary.BigEndian.PutUint64(payload[len(probeMagic):probeHeaderSize], uint64(sequence))
		sentAt[sequence].Store(time.Since(start).Nanoseconds() + 1)
		written, err := connection.Write(payload)
		if err != nil {
			if ctx.Err() != nil {
				return window, context.Cause(ctx)
			}
			if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() && !time.Now().Before(end) {
				return window, nil
			}
			return window, fmt.Errorf("send probe: %w", err)
		}
		if written != len(payload) {
			return window, fmt.Errorf("send probe: %w", io.ErrShortWrite)
		}
		window.count++
		window.last = time.Now()
		select {
		case <-ctx.Done():
			return window, context.Cause(ctx)
		case <-timer.C:
			return window, nil
		case <-ticker.C:
		}
	}
	return window, nil
}

// receive records replies until the controlled drain deadline or a hard socket failure.
func (t *replyTracker) receive(connection *net.UDPConn, buffer []byte, start time.Time) error {
	for {
		length, err := connection.Read(buffer)
		receivedAt := time.Since(start)
		if err != nil {
			if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
				return nil
			}
			return fmt.Errorf("receive probe: %w", err)
		}
		t.record(buffer[:length], receivedAt)
	}
}

// record accounts for one reply using only the local sequence-indexed send timestamp.
func (t *replyTracker) record(payload []byte, receivedAt time.Duration) {
	sequence, ok := parseProbe(payload, t.size)
	if !ok || sequence >= uint64(len(t.sentAt)) {
		t.unexpected++
		return
	}
	sentAt := t.sentAt[sequence].Load()
	if sentAt == 0 {
		t.unexpected++
		return
	}
	rtt := receivedAt - time.Duration(sentAt-1)
	if rtt < 0 {
		t.unexpected++
		return
	}
	if t.seen[sequence] {
		t.duplicates++
		return
	}
	t.seen[sequence] = true
	t.rtts[sequence] = rtt
	t.received++
}

// summarize compacts completed send samples after the receiver exits and computes nearest-rank quantiles.
func (t *replyTracker) summarize(sent int, config options, actualDuration time.Duration) result {
	count := 0
	for sequence := 1; sequence <= sent; sequence++ {
		if t.seen[sequence] {
			t.rtts[count] = t.rtts[sequence]
			count++
		}
	}
	samples := t.rtts[:count]
	slices.Sort(samples)
	measured := result{
		Sent: uint64(sent), Received: uint64(count), Lost: uint64(sent - count), Duplicates: t.duplicates,
		Unexpected:               t.unexpected + t.received - uint64(count),
		RequestedDurationSeconds: config.duration.Seconds(), ActualDurationSeconds: actualDuration.Seconds(),
		RequestedIntervalSeconds: config.interval.Seconds(),
		RequestedSizeBytes:       config.size, ActualSizeBytes: config.size, DrainTimeoutSeconds: config.drain.Seconds(),
	}
	if sent > 1 {
		measured.ActualIntervalSeconds = float64(t.sentAt[sent].Load()-t.sentAt[1].Load()) / float64(sent-1) / 1e9
	}
	if len(samples) > 0 {
		measured.P50RTTMillis = float64(percentile(samples, 50)) / float64(time.Millisecond)
		measured.P95RTTMillis = float64(percentile(samples, 95)) / float64(time.Millisecond)
		measured.P99RTTMillis = float64(percentile(samples, 99)) / float64(time.Millisecond)
		measured.MaxRTTMillis = float64(samples[len(samples)-1]) / float64(time.Millisecond)
	}
	return measured
}

// percentile selects the nearest-rank percentile from a nonempty sorted sample vector.
func percentile(samples []time.Duration, percent int) time.Duration {
	return samples[(len(samples)*percent+99)/100-1]
}
