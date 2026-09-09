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
		if err == nil && strings.HasSuffix(scenario, "-fwmark") {
			err = verifyRouteExclusion(directory)
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
	if scenario == "tcp-stall" {
		return nil
	}
	if scenario == "tcp-multipath" || scenario == "tcp-mixed" {
		return verifyParallelCarriers(directory, scenario)
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
	opened, err := tcpActiveOpens(filepath.Join(directory, "client-after.txt"))
	if err != nil {
		return err
	}
	if opened != expected {
		return fmt.Errorf("initiated %d TCP connections, expected %d including iperf3 control and data", opened, expected)
	}
	return nil
}

// tcpActiveOpens reads the namespace counter for locally initiated TCP connections.
func tcpActiveOpens(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "TcpActiveOpens" {
			return strconv.Atoi(fields[1])
		}
	}
	return 0, fmt.Errorf("missing TCP active-open counter")
}

// verifyParallelCarriers requires both configured lanes to remain available without reconnecting during the flow.
func verifyParallelCarriers(directory, scenario string) error {
	for _, name := range []string{"client-tcp-sockets.txt", "client-tcp-sockets-after.txt"} {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		tcp, wss := 0, 0
		for line := range strings.Lines(string(data)) {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}
			if strings.HasSuffix(fields[3], ":51822") {
				tcp++
			} else if strings.HasSuffix(fields[3], ":51823") {
				wss++
			}
		}
		if scenario == "tcp-mixed" && (tcp != 1 || wss != 1) || scenario == "tcp-multipath" && tcp != 2 {
			return fmt.Errorf("%s has %d TCP and %d WSS carriers, expected two configured lanes", name, tcp, wss)
		}
	}
	before, err := tcpActiveOpens(filepath.Join(directory, "client-before.txt"))
	if err != nil {
		return err
	}
	after, err := tcpActiveOpens(filepath.Join(directory, "client-after.txt"))
	if err != nil {
		return err
	}
	if after-before != 2 {
		return fmt.Errorf("initiated %d TCP connections during the flow, expected only iperf3 control and data", after-before)
	}
	return nil
}

// verifyRouteExclusion confirms that the configured mark bypasses an otherwise capturing WireGuard route.
func verifyRouteExclusion(directory string) error {
	for _, route := range []struct{ name, device string }{
		{name: "unmarked-route.txt", device: "wgtest"},
		{name: "marked-route.txt", device: "eth0"},
	} {
		data, err := os.ReadFile(filepath.Join(directory, route.name))
		if err != nil {
			return err
		}
		fields := strings.Fields(string(data))
		device := ""
		for index := 0; index+1 < len(fields); index++ {
			if fields[index] == "dev" {
				device = fields[index+1]
				break
			}
		}
		if device != route.device {
			return fmt.Errorf("%s uses device %q, expected %q", route.name, device, route.device)
		}
	}
	return nil
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
