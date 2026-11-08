//go:build linux

package main

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIPerfControlReadBudget(t *testing.T) {
	if os.Getenv("WIREHOP_TEST_TOOLS") != "1" {
		t.Skip("requires the kernel test image's iperf3")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logPath := filepath.Join(t.TempDir(), "iperf-server.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	command := exec.CommandContext(ctx, "iperf3", "-s", "-1", "-B", "127.0.0.1", "-p", strconv.Itoa(port))
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		command.Process.Kill()
		command.Wait()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("iperf3: %s", data)
		}
	}()
	waitForIPerfListener(t, ctx, port)
	connection, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	deadline, _ := ctx.Deadline()
	if err := connection.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	// Withhold the final cookie byte beyond stock iperf3's ten-second control-read budget.
	if _, err := io.WriteString(connection, "00000000-0000-0000-0000-000000000000"); err != nil {
		t.Fatal(err)
	}
	waitForPartialCookieRead(t, ctx, command.Process.Pid, port)
	timer := time.NewTimer(11 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	if _, err := connection.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	var state [1]byte
	if _, err := io.ReadFull(connection, state[:]); err != nil {
		t.Fatalf("control exchange failed after the delayed cookie: %v", err)
	}
	if state[0] != 9 {
		t.Fatalf("control state = %d, want PARAM_EXCHANGE (9)", state[0])
	}
}

func waitForIPerfListener(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	for {
		output, err := exec.CommandContext(ctx, "ss", "-ltnH", "sport = :"+strconv.Itoa(port)).Output()
		if err != nil {
			t.Fatalf("inspect control listener: %v", err)
		}
		if len(strings.TrimSpace(string(output))) > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("iperf3 did not listen: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForPartialCookieRead(t *testing.T, ctx context.Context, pid, port int) {
	t.Helper()
	byteCounter := regexp.MustCompile(`\bbytes_received:([0-9]+)`)
	for {
		output, err := exec.CommandContext(ctx, "ss", "-tinH", "state", "established", "sport = :"+strconv.Itoa(port)).Output()
		if err != nil {
			t.Fatalf("inspect control socket: %v", err)
		}
		fields := strings.Fields(strings.SplitN(string(output), "\n", 2)[0])
		match := byteCounter.FindSubmatch(output)
		if len(fields) >= 4 && fields[0] == "0" && match != nil {
			received, err := strconv.ParseUint(string(match[1]), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
			if err != nil {
				t.Fatal(err)
			}
			state := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
			// Consumed cookie bytes and a sleeping single-threaded server identify its next control read.
			if received >= 36 && len(state) > 0 && state[0] == "S" {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("server did not consume the partial cookie: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
