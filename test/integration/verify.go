// Command verify checks completed kernel WireGuard flows and prints goodput, retransmissions, or UDP delivery.
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
	Error     string
	Intervals []struct {
		Sum struct {
			Bytes uint64
		}
	}
	End struct {
		Received struct {
			Bytes         uint64
			BitsPerSecond float64 `json:"bits_per_second"`
		} `json:"sum_received"`
		Sent struct {
			Bytes       uint64
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
		udp := strings.HasSuffix(scenario, "-udp")
		if err == nil && udp && (flow.End.Sent.Bytes == 0 || flow.End.Received.Bytes < flow.End.Sent.Bytes-flow.End.Sent.Bytes/20) {
			err = fmt.Errorf("UDP delivery lost more than five percent of the controlled offered load")
		}
		if err == nil && (strings.HasSuffix(scenario, "-prohibit") || strings.HasSuffix(scenario, "-blackhole")) {
			_, err = os.Stat(filepath.Join(directory, "route-recovered.txt"))
			if err == nil && (len(flow.Intervals) == 0 || flow.Intervals[len(flow.Intervals)-1].Sum.Bytes == 0) {
				err = fmt.Errorf("TCP flow did not resume after route recovery")
			}
		}
		if err == nil {
			err = verifyCarrier(directory, scenario)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", scenario, err)
			failed = true
			continue
		}
		if udp {
			fmt.Printf("%s: %.3f Mbit/s, %d of %d UDP bytes received\n",
				scenario, flow.End.Received.BitsPerSecond/1e6, flow.End.Received.Bytes, flow.End.Sent.Bytes)
		} else {
			fmt.Printf("%s: %.3f Mbit/s, %d TCP retransmissions\n",
				scenario, flow.End.Received.BitsPerSecond/1e6, flow.End.Sent.Retransmits)
		}
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
	if strings.HasPrefix(scenario, "native") || strings.HasPrefix(scenario, "forward") {
		expected = 2
	} else if scenario == "tcp-bidir" {
		expected = 4
	} else if scenario == "tcp-idle" {
		expected = 5
	}
	if strings.HasSuffix(scenario, "-udp") {
		expected--
	}
	data, err := os.ReadFile(filepath.Join(directory, "client-after.txt"))
	if err != nil {
		return err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "TcpActiveOpens" {
			opened, err := strconv.Atoi(fields[1])
			if err != nil {
				return err
			}
			if opened != expected {
				return fmt.Errorf("initiated %d TCP connections, expected %d including iperf3 control and data", opened, expected)
			}
			return nil
		}
	}
	return fmt.Errorf("missing TCP active-open counter")
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
