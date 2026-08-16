package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestSignalContextRestoresDefaultHandling(t *testing.T) {
	if os.Getenv("WIREHOP_SIGNAL_HELPER") == "1" {
		ctx, stop := signalContext(context.Background())
		defer stop()
		fmt.Println("ready")
		<-ctx.Done()
		fmt.Println("canceled")
		select {}
	}
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not support signaling a child process with os.Interrupt")
	}
	command := exec.Command(os.Args[0], "-test.run=^TestSignalContextRestoresDefaultHandling$")
	command.Env = append(os.Environ(), "WIREHOP_SIGNAL_HELPER=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	t.Cleanup(func() {
		if command.ProcessState == nil {
			command.Process.Kill()
			<-wait
		}
	})
	lines := make(chan string, 2)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	expectLine(t, lines, "ready")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	expectLine(t, lines, "canceled")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-wait:
		if err == nil {
			t.Fatal("second interrupt exited successfully instead of using default signal handling")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("second interrupt did not terminate process, stderr %q", stderr.String())
	}
}

func expectLine(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	select {
	case got := <-lines:
		if got != want {
			t.Fatalf("subprocess line = %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for subprocess line %q", want)
	}
}
