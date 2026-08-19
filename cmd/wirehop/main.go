// Command wirehop runs the WireHop client, server, and direct forwarder.
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aofei/wirehop/internal/command"
)

// main runs WireHop until completion or process termination.
func main() {
	os.Exit(run())
}

// run owns process signal handling and returns the command exit status.
func run() int {
	ctx, stop := signalContext(context.Background())
	defer stop()
	return command.Execute(ctx, os.Args[1:], command.HostEnvironment, os.Stdout, os.Stderr)
}

// signalContext cancels on the first termination signal after restoring default signal handling.
func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			signal.Stop(signals)
			cancel()
		})
	}
	go func() {
		select {
		case <-signals:
			stop()
		case <-ctx.Done():
			stop()
		}
	}()
	return ctx, stop
}
