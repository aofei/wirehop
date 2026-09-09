// Command verify checks completed kernel TCP flows and prints their measured goodput and retransmissions.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// flowResult contains the iperf3 fields needed to distinguish completed transfers from nominal test completion.
type flowResult struct {
	Error string
	End   struct {
		Received struct {
			Bytes         uint64
			BitsPerSecond float64 `json:"bits_per_second"`
		} `json:"sum_received"`
		Sent struct {
			Retransmits uint64
		} `json:"sum_sent"`
		ReverseReceived struct {
			Bytes uint64
		} `json:"sum_received_bidir_reverse"`
	}
}

// main validates every requested scenario and exits unsuccessfully if any transfer failed or made no progress.
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: verify RESULTS_DIRECTORY CASE ...")
		os.Exit(2)
	}
	failed := false
	for _, scenario := range os.Args[2:] {
		directory := filepath.Join(os.Args[1], scenario)
		data, err := os.ReadFile(filepath.Join(directory, "flow.json"))
		var flow flowResult
		if err == nil {
			err = json.Unmarshal(data, &flow)
		}
		if err == nil && (flow.Error != "" || flow.End.Received.Bytes == 0) {
			err = fmt.Errorf("transfer did not complete with data: %s", flow.Error)
		}
		if err == nil && scenario == "tcp-bidir" && flow.End.ReverseReceived.Bytes == 0 {
			err = fmt.Errorf("reverse TCP flow made no progress")
		}
		if err == nil && strings.HasSuffix(scenario, "-rekey") {
			err = verifyRekey(directory)
		}
		if err == nil {
			err = verifyCarrier(directory, scenario)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", scenario, err)
			failed = true
			continue
		}
		fmt.Printf("%s: %.3f Mbit/s, %d TCP retransmissions\n",
			scenario, flow.End.Received.BitsPerSecond/1e6, flow.End.Sent.Retransmits)
	}
	if failed {
		os.Exit(1)
	}
}

// verifyCarrier detects unexpected reconnects in scenarios that should retain one admitted carrier.
func verifyCarrier(directory, scenario string) error {
	if scenario == "tcp-stall" || scenario == "tcp-multipath" {
		return nil
	}
	expected := 3
	if scenario == "native" || strings.HasPrefix(scenario, "forward") {
		expected = 2
	} else if scenario == "tcp-bidir" {
		expected = 4
	} else if scenario == "tcp-idle" {
		expected = 5
	}
	data, err := os.ReadFile(filepath.Join(directory, "server-nstat.txt"))
	if err != nil {
		return err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "TcpPassiveOpens" {
			opened, err := strconv.Atoi(fields[1])
			if err != nil {
				return err
			}
			if opened != expected {
				return fmt.Errorf("accepted %d TCP connections, expected %d including iperf3 control and data", opened, expected)
			}
			return nil
		}
	}
	return fmt.Errorf("missing TCP passive-open counter")
}

// verifyRekey requires a fresh kernel handshake after the initial establishment of a long-lived flow.
func verifyRekey(directory string) error {
	start, err := os.ReadFile(filepath.Join(directory, "flow-start.txt"))
	if err != nil {
		return err
	}
	started, err := strconv.ParseInt(strings.TrimSpace(string(start)), 10, 64)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(directory, "client-handshake.txt"))
	if err != nil {
		return err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return fmt.Errorf("expected one WireGuard peer handshake")
	}
	handshake, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return err
	}
	if handshake < started+110 {
		return fmt.Errorf("no kernel rekey during the long-lived flow")
	}
	return nil
}
