package main

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"
)

func TestOptionsValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		change  func(*options)
		wantErr bool
	}{
		{name: "Client"},
		{name: "Listener", change: func(c *options) { c.target, c.listen = "", "127.0.0.1:9000" }},
		{name: "MissingMode", change: func(c *options) { c.target = "" }, wantErr: true},
		{name: "ConflictingModes", change: func(c *options) { c.listen = "127.0.0.1:9000" }, wantErr: true},
		{name: "MinimumSize", change: func(c *options) { c.size = probeHeaderSize }},
		{name: "MaximumSize", change: func(c *options) { c.size = maximumDatagramSize }},
		{name: "ShortSize", change: func(c *options) { c.size = probeHeaderSize - 1 }, wantErr: true},
		{name: "Oversized", change: func(c *options) { c.size = maximumDatagramSize + 1 }, wantErr: true},
		{name: "ZeroDuration", change: func(c *options) { c.duration = 0 }, wantErr: true},
		{name: "ZeroInterval", change: func(c *options) { c.interval = 0 }, wantErr: true},
		{name: "NegativeDrain", change: func(c *options) { c.drain = -1 }, wantErr: true},
		{name: "NoDrain", change: func(c *options) { c.drain = 0 }},
		{name: "MaximumCount", change: func(c *options) {
			c.duration, c.interval = maximumProbes*time.Nanosecond, time.Nanosecond
		}},
		{name: "ExcessiveCount", change: func(c *options) {
			c.duration, c.interval = (maximumProbes+1)*time.Nanosecond, time.Nanosecond
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := options{target: "127.0.0.1:9000", size: 64, duration: 20 * time.Second,
				interval: 10 * time.Millisecond, drain: 2 * time.Second}
			if tt.change != nil {
				tt.change(&config)
			}
			if err := config.validate(); (err != nil) != tt.wantErr {
				t.Fatalf("validate() = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestOptionsProbeCount(t *testing.T) {
	for _, tt := range []struct {
		name     string
		duration time.Duration
		want     int64
	}{
		{name: "WholeIntervals", duration: 20 * time.Second, want: 2000},
		{name: "PartialInterval", duration: 25 * time.Millisecond, want: 3},
		{name: "ShorterThanInterval", duration: time.Millisecond, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := options{duration: tt.duration, interval: 10 * time.Millisecond}
			if count := config.probeCount(); count != tt.want {
				t.Fatalf("probeCount() = %d, want %d", count, tt.want)
			}
		})
	}
}

func TestReplyTrackerRecord(t *testing.T) {
	tracker := replyTracker{size: 24, sentAt: make([]atomic.Int64, 5),
		seen: make([]bool, 5), rtts: make([]time.Duration, 5)}
	for sequence := 1; sequence <= 3; sequence++ {
		tracker.sentAt[sequence].Store(int64(sequence)*int64(10*time.Millisecond) + 1)
	}
	tracker.record(testProbe(24, 3), 60*time.Millisecond)
	tracker.record(testProbe(24, 1), 80*time.Millisecond)
	tracker.record(testProbe(24, 3), 90*time.Millisecond)
	tracker.record(testProbe(24, 4), 90*time.Millisecond)
	tracker.record(testProbe(24, 99), 90*time.Millisecond)
	tracker.record(testProbe(24, 0), 90*time.Millisecond)
	tracker.record(testProbe(23, 2), 90*time.Millisecond)
	malformed := testProbe(24, 2)
	malformed[0] = 0
	tracker.record(malformed, 90*time.Millisecond)
	if tracker.received != 2 || tracker.duplicates != 1 || tracker.unexpected != 5 {
		t.Fatalf("accounting = %d received, %d duplicates, %d unexpected", tracker.received, tracker.duplicates, tracker.unexpected)
	}
	if tracker.rtts[1] != 70*time.Millisecond || tracker.rtts[3] != 30*time.Millisecond {
		t.Fatalf("local RTT samples = %v", tracker.rtts)
	}
	config := options{size: 24, duration: 40 * time.Millisecond, interval: 10 * time.Millisecond, drain: time.Second}
	measured := tracker.summarize(3, config, 42*time.Millisecond)
	if measured.Sent != 3 || measured.Received != 2 || measured.Lost != 1 || measured.Duplicates != 1 || measured.Unexpected != 5 {
		t.Fatalf("result accounting = %+v", measured)
	}
	if measured.P50RTTMillis != 30 || measured.P95RTTMillis != 70 || measured.P99RTTMillis != 70 || measured.MaxRTTMillis != 70 {
		t.Fatalf("result quantiles = %+v", measured)
	}
	if measured.ActualIntervalSeconds != 0.01 || measured.ActualDurationSeconds != 0.042 {
		t.Fatalf("actual sending schedule = %+v", measured)
	}
}

func TestReplyTrackerSummarize(t *testing.T) {
	t.Run("ReplyBeforeSendTimestamp", func(t *testing.T) {
		tracker := replyTracker{size: 16, sentAt: make([]atomic.Int64, 2),
			seen: make([]bool, 2), rtts: make([]time.Duration, 2)}
		tracker.sentAt[1].Store(int64(10*time.Millisecond) + 1)
		tracker.record(testProbe(16, 1), 9*time.Millisecond)
		if tracker.seen[1] || tracker.received != 0 || tracker.unexpected != 1 {
			t.Fatal("reply received before its local send timestamp became a latency sample")
		}
		tracker.record(testProbe(16, 1), 20*time.Millisecond)
		measured := tracker.summarize(1, options{}, 0)
		if measured.Received != 1 || measured.Lost != 0 || measured.Duplicates != 0 || measured.Unexpected != 1 || measured.P50RTTMillis != 10 {
			t.Fatalf("measurement after the real reply = %+v", measured)
		}
	})
	t.Run("NoReplies", func(t *testing.T) {
		tracker := replyTracker{sentAt: make([]atomic.Int64, 2), seen: make([]bool, 2), rtts: make([]time.Duration, 2)}
		measured := tracker.summarize(1, options{}, 0)
		if measured.Sent != 1 || measured.Received != 0 || measured.Lost != 1 || measured.P99RTTMillis != 0 {
			t.Fatalf("empty measurement = %+v", measured)
		}
	})
	t.Run("UncommittedSend", func(t *testing.T) {
		tracker := replyTracker{size: 16, sentAt: make([]atomic.Int64, 3),
			seen: make([]bool, 3), rtts: make([]time.Duration, 3)}
		tracker.sentAt[2].Store(1)
		tracker.record(testProbe(16, 2), time.Millisecond)
		measured := tracker.summarize(1, options{}, 0)
		if measured.Received != 0 || measured.Lost != 1 || measured.Unexpected != 1 {
			t.Fatalf("uncommitted send measurement = %+v", measured)
		}
	})
}

func TestPercentile(t *testing.T) {
	for _, tt := range []struct {
		name    string
		samples []time.Duration
		want    [3]time.Duration
	}{
		{name: "Single", samples: []time.Duration{time.Millisecond}, want: [3]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}},
		{name: "Two", samples: []time.Duration{time.Millisecond, 2 * time.Millisecond}, want: [3]time.Duration{time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond}},
		{name: "Hundred", samples: func() []time.Duration {
			samples := make([]time.Duration, 100)
			for index := range samples {
				samples[index] = time.Duration(index+1) * time.Millisecond
			}
			return samples
		}(), want: [3]time.Duration{50 * time.Millisecond, 95 * time.Millisecond, 99 * time.Millisecond}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for index, percent := range []int{50, 95, 99} {
				if got := percentile(tt.samples, percent); got != tt.want[index] {
					t.Fatalf("percentile(%d) = %v, want %v", percent, got, tt.want[index])
				}
			}
		})
	}
}

func testProbe(size int, sequence uint64) []byte {
	payload := make([]byte, size)
	copy(payload, probeMagic)
	binary.BigEndian.PutUint64(payload[len(probeMagic):probeHeaderSize], sequence)
	for index := probeHeaderSize; index < len(payload); index++ {
		payload[index] = 0xff
	}
	return payload
}
