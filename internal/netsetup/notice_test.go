package netsetup

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRetryNotice(t *testing.T) {
	t.Run("QuietRecovery", func(t *testing.T) {
		var output bytes.Buffer
		notice := NewRetryNotice(slog.New(slog.NewTextHandler(&output, nil)), "test operation")
		notice.Failed("attempt", 1)
		notice.Recovered()
		if output.Len() != 0 {
			t.Fatalf("unexpected early recovery output: %s", &output)
		}
	})
	t.Run("RateLimitedWarningsAndRecovery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var output bytes.Buffer
			notice := NewRetryNotice(slog.New(slog.NewTextHandler(&output, nil)), "test operation")
			for range 90 {
				notice.Failed("attempt", 1)
				time.Sleep(time.Second)
			}
			if got := strings.Count(output.String(), "level=WARN"); got != 1 {
				t.Fatalf("warnings before 90s = %d, want 1: %s", got, &output)
			}
			notice.Failed("attempt", 2)
			notice.Recovered()
			notice.Recovered()
			notice.Failed("attempt", 3)
			if strings.Count(output.String(), "level=WARN") != 2 || strings.Count(output.String(), "level=INFO") != 1 {
				t.Fatalf("unexpected warning or recovery count: %s", &output)
			}
			time.Sleep(30 * time.Second)
			notice.Failed("attempt", 4)
			if got := strings.Count(output.String(), "level=WARN"); got != 3 {
				t.Fatalf("new outage warnings = %d, want 3 total", got)
			}
		})
	})
	t.Run("NoLogger", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			notice := NewRetryNotice(nil, "test operation")
			time.Sleep(time.Minute)
			notice.Failed()
			notice.Recovered()
		})
	})
}
