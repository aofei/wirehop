// Command verify checks completed kernel WireGuard flows and prints goodput, retransmissions, or UDP delivery.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// flowResult contains the iperf3 fields needed to distinguish completed transfers from nominal test completion.
type flowResult struct {
	Error     string
	Intervals []flowIntervalResult
	Start     struct {
		Timestamp struct {
			Seconds int64 `json:"timesecs"`
		}
	}
	ServerOutput *flowResult `json:"server_output_json"`
	End          struct {
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

// flowIntervalResult contains the directions reported for one measurement period.
type flowIntervalResult struct {
	Sum     flowInterval
	Reverse flowInterval `json:"sum_bidir_reverse"`
}

// flowInterval describes one direction's completed iperf3 reporting interval.
type flowInterval struct {
	Bytes  uint64
	Start  float64
	End    float64
	Sender *bool
}

// rttStepQdiscPattern extracts the configured root netem and its cumulative traffic and drop counters.
var rttStepQdiscPattern = regexp.MustCompile(`(?m)^qdisc netem 10: root ([^\n]*)\n Sent ([0-9]+) bytes [0-9]+ pkt \(dropped ([0-9]+),`)

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
		if err == nil && strings.HasSuffix(scenario, "-bidir") && flow.End.ReverseReceived.Bytes == 0 {
			err = fmt.Errorf("reverse TCP flow made no progress")
		}
		if err == nil && strings.HasSuffix(scenario, "-rekey") {
			err = verifyRekey(directory)
		}
		udp := strings.HasSuffix(scenario, "-udp")
		if err == nil && udp && (flow.End.Sent.Bytes == 0 || flow.End.Received.Bytes < flow.End.Sent.Bytes-flow.End.Sent.Bytes/20) {
			err = fmt.Errorf("UDP delivery lost more than five percent of the controlled offered load")
		}
		if err == nil && (strings.HasSuffix(scenario, "-prohibit") || strings.HasSuffix(scenario, "-blackhole") ||
			strings.HasPrefix(scenario, "tcp-asymmetric-stall") || strings.HasSuffix(scenario, "-stall") ||
			strings.HasSuffix(scenario, "-outage") || strings.HasSuffix(scenario, "-roam")) {
			marker := "path-recovered.txt"
			if strings.HasSuffix(scenario, "-prohibit") || strings.HasSuffix(scenario, "-blackhole") {
				marker = "route-recovered.txt"
			}
			_, err = os.Stat(filepath.Join(directory, marker))
			if err == nil && (len(flow.Intervals) == 0 || flow.Intervals[len(flow.Intervals)-1].Sum.Bytes == 0) {
				err = fmt.Errorf("TCP flow made no progress in the final interval after path recovery")
			}
		}
		capacityChange := strings.HasPrefix(scenario, "native-capacity-change") ||
			strings.HasPrefix(scenario, "tcp-multipath-capacity-change")
		if err == nil && capacityChange {
			for _, name := range []string{"client-rate-dropped.txt", "client-rate-restored.txt",
				"server-rate-dropped.txt", "server-rate-restored.txt"} {
				if _, err = os.Stat(filepath.Join(directory, name)); err != nil {
					break
				}
			}
		}
		if err == nil && (strings.HasPrefix(scenario, "tcp-asymmetric-stall") || capacityChange) {
			duration := 20.0
			if capacityChange {
				duration = 40
			}
			err = verifyThroughputRecovery(flow, strings.HasSuffix(scenario, "-bidir"), duration)
		}
		if err == nil && strings.Contains(scenario, "-rtt-step") {
			err = verifyRTTStep(directory, scenario, flow)
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

// verifyRTTStep checks each exercised direction using intervals measured at its receiving endpoint.
func verifyRTTStep(directory, scenario string, flow flowResult) error {
	if !strings.HasSuffix(scenario, "-reverse") {
		if err := verifyRTTStepDirection(directory, flow.ServerOutput, false); err != nil {
			return fmt.Errorf("forward direction: %w", err)
		}
	}
	bidirectional := strings.HasSuffix(scenario, "-bidir")
	if bidirectional || strings.HasSuffix(scenario, "-reverse") {
		if err := verifyRTTStepDirection(directory, &flow, bidirectional); err != nil {
			return fmt.Errorf("reverse direction: %w", err)
		}
	}
	return nil
}

// verifyRTTStepDirection checks receiver throughput after a persistent RTT increase and its reversal at fixed capacity.
// Reverse intervals are selected only from the client's bidirectional result. Reverse-only flows use their ordinary sum.
func verifyRTTStepDirection(directory string, receiver *flowResult, reverseIntervals bool) error {
	if receiver == nil || receiver.Error != "" || receiver.Start.Timestamp.Seconds == 0 {
		return fmt.Errorf("missing completed receiver interval evidence")
	}
	var raised, restored [2]float64
	for index, role := range []string{"client", "server"} {
		for _, change := range []struct {
			name string
			at   *float64
			want float64
		}{
			{name: "raised", at: &raised[index], want: 10},
			{name: "restored", at: &restored[index], want: 40},
		} {
			name := role + "-delay-" + change.name + ".txt"
			data, err := os.ReadFile(filepath.Join(directory, name))
			if err != nil {
				return err
			}
			at, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			*change.at = float64(at - receiver.Start.Timestamp.Seconds)
			if *change.at < change.want-1 || *change.at > change.want+2 {
				return fmt.Errorf("%s changed at %.0f seconds, expected approximately %.0f", name, *change.at, change.want)
			}
		}
		if err := verifyRTTStepQdiscs(directory, role); err != nil {
			return err
		}
	}
	previousEnd := 0.0
	for _, interval := range receiver.Intervals {
		sample := interval.Sum
		if reverseIntervals {
			sample = interval.Reverse
		}
		if sample.Sender == nil || *sample.Sender || sample.Start < previousEnd || sample.End <= sample.Start {
			return fmt.Errorf("invalid receiver interval evidence")
		}
		previousEnd = sample.End
	}
	var rates [3]float64
	for index, window := range []struct {
		name       string
		start, end float64
	}{
		{name: "baseline", start: 2, end: min(raised[0], raised[1]) - 2},
		{name: "raised RTT", start: max(raised[0], raised[1]) + 18, end: min(restored[0], restored[1]) - 2},
		{name: "restored RTT", start: max(restored[0], restored[1]) + 10, end: 58},
	} {
		var bytes uint64
		var seconds float64
		for _, interval := range receiver.Intervals {
			sample := interval.Sum
			if reverseIntervals {
				sample = interval.Reverse
			}
			if sample.Start >= window.start && sample.End <= window.end {
				bytes += sample.Bytes
				seconds += sample.End - sample.Start
			}
		}
		// Whole intervals can omit at most one second at either edge of a window.
		if seconds < window.end-window.start-2.1 || bytes == 0 {
			return fmt.Errorf("%s lacks complete useful receiver intervals", window.name)
		}
		rates[index] = float64(bytes) / seconds
		if index == 0 && rates[0]*8 < 5e6 {
			return fmt.Errorf("baseline receiver throughput is %.3f Mbit/s, require at least 5 Mbit/s on the 20 Mbit/s link",
				rates[0]*8/1e6)
		}
		if index > 0 && rates[index] < rates[0]/4 {
			return fmt.Errorf("%s receiver throughput is %.1f percent of baseline, require at least 25 percent",
				window.name, rates[index]/rates[0]*100)
		}
	}
	return nil
}

// verifyRTTStepQdiscs verifies successful impairment changes without queue replacement, packet drops, or bypass.
func verifyRTTStepQdiscs(directory, role string) error {
	var previousBytes uint64
	for _, phase := range []struct{ name, delay string }{
		{name: "initial", delay: "5ms"},
		{name: "raised", delay: "600ms"},
		{name: "restored", delay: "5ms"},
		{name: "final", delay: "5ms"},
	} {
		name := role + "-qdisc-" + phase.name + ".txt"
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		match := rttStepQdiscPattern.FindStringSubmatch(string(data))
		if match == nil {
			return fmt.Errorf("%s lacks root netem evidence", name)
		}
		configuration := " " + match[1] + " "
		for _, setting := range []string{"limit 10000", "delay " + phase.delay, "rate 20Mbit"} {
			if !strings.Contains(configuration, " "+setting+" ") {
				return fmt.Errorf("%s lacks expected %s", name, setting)
			}
		}
		bytes, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if match[3] != "0" || phase.name != "initial" && bytes <= previousBytes {
			return fmt.Errorf("%s records packet drops or no additional shaped traffic", name)
		}
		previousBytes = bytes
	}
	return nil
}

// verifyThroughputRecovery requires a complete flow and substantial final-five-second throughput in each direction,
// relative to its own throughput before fault injection at five seconds. duration is the prescribed flow time in seconds.
func verifyThroughputRecovery(flow flowResult, bidirectional bool, duration float64) error {
	directions := 1
	if bidirectional {
		directions = 2
	}
	for direction := range directions {
		var baselineBytes, recoveredBytes uint64
		var baselineSeconds, recoveredSeconds float64
		var finalEnd float64
		for _, interval := range flow.Intervals {
			sample := interval.Sum
			if direction == 1 {
				sample = interval.Reverse
			}
			seconds := sample.End - sample.Start
			if seconds <= 0 {
				continue
			}
			finalEnd = max(finalEnd, sample.End)
			if sample.End <= 4.5 {
				baselineBytes += sample.Bytes
				baselineSeconds += seconds
			}
			if sample.Start >= duration-5.5 {
				recoveredBytes += sample.Bytes
				recoveredSeconds += seconds
			}
		}
		if finalEnd < duration-0.5 || baselineSeconds < 2 || recoveredSeconds < 4 || baselineBytes == 0 {
			return fmt.Errorf("direction %d lacks complete pre-fault or post-fault throughput intervals", direction)
		}
		baseline := float64(baselineBytes) / baselineSeconds
		recovered := float64(recoveredBytes) / recoveredSeconds
		if recovered < baseline/4 {
			return fmt.Errorf("direction %d recovered %.1f percent of pre-fault throughput, require at least 25 percent",
				direction, recovered/baseline*100)
		}
	}
	return nil
}

// verifyCarrier detects unexpected reconnects in scenarios that should retain one admitted carrier.
func verifyCarrier(directory, scenario string) error {
	if strings.HasPrefix(scenario, "tcp-asymmetric-stall") {
		_, err := parallelCarrierSockets(filepath.Join(directory, "client-tcp-sockets.txt"), true)
		return err
	}
	if strings.HasSuffix(scenario, "-stall") || strings.HasSuffix(scenario, "-outage") || strings.HasSuffix(scenario, "-roam") {
		return nil
	}
	if strings.HasPrefix(scenario, "tcp-multipath") || scenario == "tcp-mixed" || scenario == "tcp-asymmetric" {
		return verifyParallelCarriers(directory, scenario)
	}
	expected := 3
	if strings.HasPrefix(scenario, "native") || strings.HasPrefix(scenario, "forward") {
		expected = 2
	}
	if strings.HasSuffix(scenario, "-bidir") {
		expected++
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

// verifyParallelCarriers requires both configured lanes before and after a flow. Only capacity-change faults permit
// reconnection during these parallel-lane checks.
func verifyParallelCarriers(directory, scenario string) error {
	var sockets [2][]string
	for index, name := range []string{"client-tcp-sockets.txt", "client-tcp-sockets-after.txt"} {
		splitPorts := scenario == "tcp-mixed" || scenario == "tcp-asymmetric" ||
			strings.HasPrefix(scenario, "tcp-multipath-distinct-")
		var err error
		sockets[index], err = parallelCarrierSockets(filepath.Join(directory, name), splitPorts)
		if err != nil {
			return err
		}
	}
	if strings.HasPrefix(scenario, "tcp-multipath-capacity-change") {
		return nil
	}
	if !slices.Equal(sockets[0], sockets[1]) {
		return fmt.Errorf("carrier socket identities changed during the flow")
	}
	before, err := tcpActiveOpens(filepath.Join(directory, "client-before.txt"))
	if err != nil {
		return err
	}
	after, err := tcpActiveOpens(filepath.Join(directory, "client-after.txt"))
	if err != nil {
		return err
	}
	expected := 2
	if strings.HasSuffix(scenario, "-bidir") {
		expected = 3
	}
	if after-before != expected {
		return fmt.Errorf("initiated %d TCP connections during the flow, expected %d for iperf3 control and data", after-before, expected)
	}
	return nil
}

// parallelCarrierSockets returns sorted endpoint pairs after validating both configured lanes in one snapshot.
func parallelCarrierSockets(path string, splitPorts bool) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	primary, secondary := 0, 0
	var sockets []string
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		if strings.HasSuffix(fields[3], ":51822") {
			primary++
		} else if strings.HasSuffix(fields[3], ":51823") {
			secondary++
		} else {
			continue
		}
		sockets = append(sockets, fields[2]+" "+fields[3])
	}
	if splitPorts && (primary != 1 || secondary != 1) || !splitPorts && (primary != 2 || secondary != 0) {
		return nil, fmt.Errorf("%s has %d primary and %d secondary carriers, expected two configured lanes",
			filepath.Base(path), primary, secondary)
	}
	slices.Sort(sockets)
	return sockets, nil
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
