// Command verify checks completed kernel WireGuard flows and prints goodput, retransmissions, or UDP delivery.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// flowResult contains the iperf3 fields needed to distinguish completed transfers from nominal test completion.
type flowResult struct {
	Error        string
	Intervals    []flowIntervalResult
	Started      float64        `json:"started_boottime"`
	ServerOutput *flowResult    `json:"-"`
	Receiver     *receiverTrace `json:"-"`
	Start        struct {
		Started float64 `json:"started_boottime"`
	}
	End struct {
		Received struct {
			Bytes         uint64
			BitsPerSecond float64 `json:"bits_per_second"`
			Sender        *bool
		} `json:"sum_received"`
		Sent struct {
			Bytes       uint64
			Retransmits uint64
		} `json:"sum_sent"`
		ReverseReceived struct {
			Bytes  uint64
			Sender *bool
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

// flowDirection selects counter evidence from the endpoint receiving one direction of traffic.
type flowDirection struct {
	name     string
	receiver *flowResult
}

// receiverTrace records bounded application counter updates on the shared boot clock.
type receiverTrace struct {
	Type    string          `json:"type"`
	Stream  int             `json:"stream_id"`
	Started int64           `json:"started_boottime_ns"`
	Ended   int64           `json:"ended_boottime_ns"`
	Total   uint64          `json:"total_bytes"`
	Count   int             `json:"count"`
	Error   *bool           `json:"error"`
	Events  []receiverEvent `json:"-"`
}

// receiverEvent brackets the actual byte-counter update without assuming uniform delivery in a reporting bin.
type receiverEvent struct {
	Began int64  `json:"began_boottime_ns"`
	Ended int64  `json:"ended_boottime_ns"`
	Bytes uint64 `json:"bytes"`
}

// throughputWindow identifies a phase relative to the client's shared measurement origin.
type throughputWindow struct {
	name       string
	start, end float64
}

// throughputMeasurement preserves conservative delivery bounds for one explicitly bounded phase.
type throughputMeasurement struct {
	Direction     string   `json:"direction"`
	Phase         string   `json:"phase"`
	Start         float64  `json:"start_seconds"`
	End           float64  `json:"end_seconds"`
	Covered       float64  `json:"covered_seconds"`
	LowerBytes    uint64   `json:"received_bytes_lower"`
	UpperBytes    uint64   `json:"received_bytes_upper"`
	LowerRate     float64  `json:"bits_per_second_lower"`
	UpperRate     float64  `json:"bits_per_second_upper"`
	LowerFraction *float64 `json:"baseline_fraction_lower,omitempty"`
	UpperFraction *float64 `json:"baseline_fraction_upper,omitempty"`
	Status        string   `json:"status"`
}

// throughputRoundoff permits float64 roundoff at exact throughput-ratio boundaries.
const throughputRoundoff = 1e-12

// rttStepQdiscPattern extracts the configured root netem and its cumulative traffic and drop counters.
var rttStepQdiscPattern = regexp.MustCompile(`(?m)^qdisc netem 10: root ([^\n]*)\n Sent ([0-9]+) bytes [0-9]+ pkt \(dropped ([0-9]+),`)

// capacityChangeQdiscPatterns extract the root TBF and child netem configuration and cumulative counters.
var capacityChangeQdiscPatterns = [2]*regexp.Regexp{
	regexp.MustCompile(`(?m)^qdisc tbf 1: root ([^\n]*)\n Sent ([0-9]+) bytes [0-9]+ pkt \(dropped ([0-9]+),`),
	regexp.MustCompile(`(?m)^qdisc netem 10: parent 1:1 ([^\n]*)\n Sent ([0-9]+) bytes [0-9]+ pkt \(dropped ([0-9]+),`),
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
		flow, measurements, err := verifyResult(directory, scenario)
		udp := strings.HasSuffix(scenario, "-udp")
		for _, measurement := range measurements {
			reference := ""
			if measurement.LowerFraction != nil {
				reference = fmt.Sprintf(" baseline=[%.4f,%.4f]%%", *measurement.LowerFraction*100, *measurement.UpperFraction*100)
			}
			fmt.Printf("%s: %s %s window=[%.6f,%.6f] coverage=%.6fs receiver=[%.6f,%.6f] Mbit/s%s status=%s\n",
				scenario, measurement.Direction, measurement.Phase, measurement.Start, measurement.End,
				measurement.Covered, measurement.LowerRate/1e6, measurement.UpperRate/1e6, reference, measurement.Status)
		}
		report := struct {
			Scenario     string                  `json:"scenario"`
			Measurements []throughputMeasurement `json:"measurements"`
			Error        string                  `json:"error,omitempty"`
		}{Scenario: scenario, Measurements: measurements}
		if err != nil {
			report.Error = err.Error()
		}
		encoded, reportError := json.MarshalIndent(report, "", "  ")
		if reportError == nil {
			reportError = os.WriteFile(filepath.Join(directory, "verification.json"), append(encoded, '\n'), 0644)
		}
		err = errors.Join(err, reportError)
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

// verifyResult loads endpoint evidence and process results without hiding independent carrier failures.
func verifyResult(directory, scenario string) (flowResult, []throughputMeasurement, error) {
	flow, readError := loadFlow(filepath.Join(directory, "flow.json"))
	monitored := strings.Contains(scenario, "-rtt-step") || strings.Contains(scenario, "-capacity-change") ||
		strings.HasPrefix(scenario, "tcp-asymmetric-stall") || hasPathFault(scenario)
	failures := []error{readError}
	if monitored {
		server, err := loadFlow(filepath.Join(directory, "server-flow.json"))
		failures = append(failures, err)
		if err == nil {
			flow.ServerOutput = &server
			if !validTimestamp(server.Started) {
				failures = append(failures, fmt.Errorf("server-flow.json: missing valid data-start timestamp"))
			}
			bytes := server.End.Received.Bytes
			if strings.HasSuffix(scenario, "-reverse") {
				bytes = server.End.Sent.Bytes
			}
			if server.Error != "" || bytes == 0 {
				failures = append(failures, fmt.Errorf("server-flow.json: transfer did not complete with data: %s", server.Error))
			}
		}
		for _, direction := range receiverDirections(scenario, &flow) {
			role := "server"
			if direction.name == "reverse" {
				role = "client"
			}
			if direction.receiver != nil {
				trace, err := loadReceiverTrace(directory, role, direction.receiver)
				failures = append(failures, err)
				direction.receiver.Receiver = trace
			}
		}
	}
	var measurements []throughputMeasurement
	if readError == nil {
		var err error
		measurements, err = verifyScenario(directory, scenario, flow)
		failures = append(failures, err)
	} else {
		failures = append(failures, verifyCarrier(directory, scenario))
		if strings.HasSuffix(scenario, "-fwmark") {
			failures = append(failures, verifyRouteExclusion(directory))
		}
	}
	exitFiles := []string{"client-exit.txt"}
	if monitored {
		exitFiles = append(exitFiles, "server-flow-exit.txt")
	}
	for _, name := range exitFiles {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			failures = append(failures, err)
			continue
		}
		status, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", name, err))
		} else if status != 0 {
			failures = append(failures, fmt.Errorf("%s records process exit %d", name, status))
		}
	}
	data, err := os.ReadFile(filepath.Join(directory, "harness-error.txt"))
	if err == nil {
		failures = append(failures, fmt.Errorf("test harness: %s", strings.TrimSpace(string(data))))
	} else if !errors.Is(err, os.ErrNotExist) {
		failures = append(failures, err)
	}
	return flow, measurements, errors.Join(failures...)
}

// loadFlow decodes a result and requires producer-clock provenance unless the producer reported failure.
func loadFlow(path string) (flowResult, error) {
	var flow flowResult
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &flow)
	}
	if err == nil && flow.Error == "" && (!validTimestamp(flow.Started) || flow.Started != flow.Start.Started) {
		err = fmt.Errorf("missing or inconsistent producer measurement origin")
	}
	if err != nil {
		return flow, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return flow, nil
}

// verifyScenario checks independent assertions even when another assertion has already failed.
func verifyScenario(directory, scenario string, flow flowResult) ([]throughputMeasurement, error) {
	var failures []error
	var measurements []throughputMeasurement
	if !validTimestamp(flow.Started) {
		failures = append(failures, fmt.Errorf("missing valid client data-start timestamp"))
	}
	if flow.Error != "" || flow.End.Received.Bytes == 0 {
		failures = append(failures, fmt.Errorf("transfer did not complete with data: %s", flow.Error))
	}
	if strings.HasSuffix(scenario, "-bidir") && flow.End.ReverseReceived.Bytes == 0 {
		failures = append(failures, fmt.Errorf("reverse TCP flow made no progress"))
	}
	if strings.HasSuffix(scenario, "-rekey") {
		failures = append(failures, verifyRekey(directory))
	}
	if strings.HasSuffix(scenario, "-udp") &&
		(flow.End.Sent.Bytes == 0 || flow.End.Received.Bytes < flow.End.Sent.Bytes-flow.End.Sent.Bytes/20) {
		failures = append(failures, fmt.Errorf("UDP delivery lost more than five percent of the controlled offered load"))
	}
	if strings.HasPrefix(scenario, "tcp-asymmetric-stall") {
		measured, err := verifyRecovery(scenario, flow, 20, 0)
		measurements = append(measurements, measured...)
		failures = append(failures, err)
		failures = append(failures, verifyFaultTimes(directory, flow.Started, "path", 5, 9))
	} else if hasPathFault(scenario) {
		prefix, recovered, duration := "path", 9.0, 20.0
		if strings.HasSuffix(scenario, "-prohibit") || strings.HasSuffix(scenario, "-blackhole") {
			prefix, recovered = "route", 6
		} else if strings.HasSuffix(scenario, "-outage") {
			recovered, duration = 17, 40
		} else if strings.HasSuffix(scenario, "-roam") {
			recovered, duration = 5, 40
		}
		failures = append(failures, verifyFaultTimes(directory, flow.Started, prefix, 5, recovered))
		measured, err := verifyProgress(scenario, flow, duration)
		measurements = append(measurements, measured...)
		failures = append(failures, err)
	}
	if strings.Contains(scenario, "-capacity-change") {
		failures = append(failures, verifyCapacityChangeFault(directory, flow.Started))
		measured, err := verifyRecovery(scenario, flow, 40, 5e6)
		measurements = append(measurements, measured...)
		failures = append(failures, err)
	}
	if strings.Contains(scenario, "-rtt-step") {
		measured, err := verifyRTTStep(directory, scenario, flow)
		measurements = append(measurements, measured...)
		failures = append(failures, err)
	}
	failures = append(failures, verifyCarrier(directory, scenario))
	if strings.HasSuffix(scenario, "-fwmark") {
		failures = append(failures, verifyRouteExclusion(directory))
	}
	return measurements, errors.Join(failures...)
}

// receiverDirections selects the receiving endpoint for every exercised traffic direction.
func receiverDirections(scenario string, flow *flowResult) []flowDirection {
	var directions []flowDirection
	if !strings.HasSuffix(scenario, "-reverse") {
		directions = append(directions, flowDirection{name: "forward", receiver: flow.ServerOutput})
	}
	if strings.HasSuffix(scenario, "-reverse") || strings.HasSuffix(scenario, "-bidir") {
		directions = append(directions, flowDirection{
			name: "reverse", receiver: flow,
		})
	}
	return directions
}

// verifyRTTStep checks each exercised direction using its receiving endpoint's counter updates.
func verifyRTTStep(directory, scenario string, flow flowResult) ([]throughputMeasurement, error) {
	var measurements []throughputMeasurement
	var failures []error
	for _, direction := range receiverDirections(scenario, &flow) {
		measured, err := verifyRTTStepDirection(directory, direction.receiver, flow.Started)
		for index := range measured {
			measured[index].Direction = direction.name
		}
		measurements = append(measurements, measured...)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s direction: %w", direction.name, err))
		}
	}
	return measurements, errors.Join(failures...)
}

// verifyRTTStepDirection checks receiver throughput after a persistent RTT increase and its reversal at fixed capacity.
func verifyRTTStepDirection(directory string, receiver *flowResult, origin float64) ([]throughputMeasurement, error) {
	trace, err := receiverEvidence(receiver, origin)
	if err != nil {
		return nil, err
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
			at, err := readTimestamp(filepath.Join(directory, name))
			if err != nil {
				return nil, err
			}
			*change.at = at - origin
			if math.Abs(*change.at-change.want) > 1 {
				return nil, fmt.Errorf("%s changed at %.6f seconds, expected %.0f +/- 1", name, *change.at, change.want)
			}
		}
		if math.Abs(restored[index]-raised[index]-30) > 1 {
			return nil, fmt.Errorf("%s RTT fault lasted %.6f seconds, expected 30 +/- 1", role, restored[index]-raised[index])
		}
		if err := verifyRTTStepQdiscs(directory, role); err != nil {
			return nil, err
		}
	}
	return measureThroughput(trace, origin, []throughputWindow{
		{name: "baseline", start: 2, end: min(raised[0], raised[1]) - 2},
		{name: "raised RTT", start: max(raised[0], raised[1]) + 18, end: min(restored[0], restored[1]) - 2},
		{name: "restored RTT", start: max(restored[0], restored[1]) + 10, end: 58},
	}, 5e6)
}

// loadReceiverTrace rejects incomplete evidence and checks conservation against the local receiver, including
// bidirectional sums.
func loadReceiverTrace(directory, role string, receiver *flowResult) (*receiverTrace, error) {
	paths, err := filepath.Glob(filepath.Join(directory, role+"-receiver-*.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(paths) != 1 {
		return nil, fmt.Errorf("%s: require exactly one receiver counter trace, found %d", role, len(paths))
	}
	file, err := os.Open(paths[0])
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var trace receiverTrace
	if err := decoder.Decode(&trace); err != nil {
		return nil, fmt.Errorf("%s receiver header: %w", role, err)
	}
	if trace.Type != "receiver_reads" || trace.Stream <= 0 || trace.Count < 0 || trace.Count > 1048576 ||
		trace.Error == nil || *trace.Error || trace.Started <= 0 || trace.Ended <= trace.Started ||
		!validTimestamp(receiver.Started) || math.Abs(float64(trace.Started)/1e9-receiver.Started) > 1e-6 {
		return nil, fmt.Errorf("%s: invalid receiver counter header", role)
	}
	var total uint64
	previous := trace.Started
	for range trace.Count {
		var event receiverEvent
		if err := decoder.Decode(&event); err != nil {
			return nil, fmt.Errorf("%s receiver event: %w", role, err)
		}
		if event.Began < previous || event.Ended < event.Began || event.Ended > trace.Ended ||
			event.Bytes == 0 || event.Bytes > math.MaxUint64-total {
			return nil, fmt.Errorf("%s: invalid receiver counter event", role)
		}
		previous = event.Ended
		total += event.Bytes
		trace.Events = append(trace.Events, event)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%s: receiver counter trace has trailing or malformed records", role)
	}
	var localTotals []uint64
	for _, sum := range []struct {
		bytes  uint64
		sender *bool
	}{
		{receiver.End.Received.Bytes, receiver.End.Received.Sender},
		{receiver.End.ReverseReceived.Bytes, receiver.End.ReverseReceived.Sender},
	} {
		if sum.sender != nil && !*sum.sender {
			localTotals = append(localTotals, sum.bytes)
		}
	}
	var intervalTotal uint64
	for _, interval := range receiver.Intervals {
		found := 0
		for _, sum := range []flowInterval{interval.Sum, interval.Reverse} {
			if sum.Sender != nil && !*sum.Sender {
				if sum.Bytes > math.MaxUint64-intervalTotal {
					return nil, fmt.Errorf("%s: receiver interval counter overflow", role)
				}
				intervalTotal += sum.Bytes
				found++
			}
		}
		if found != 1 {
			return nil, fmt.Errorf("%s: ambiguous local receiver intervals", role)
		}
	}
	if len(localTotals) != 1 || total != trace.Total || total != localTotals[0] || intervalTotal != total {
		return nil, fmt.Errorf("%s: receiver events, intervals, and local final total do not conserve bytes", role)
	}
	return &trace, nil
}

// receiverEvidence requires completed counter evidence. Reporting intervals cannot reconstruct within-bin timing.
func receiverEvidence(receiver *flowResult, origin float64) (*receiverTrace, error) {
	if receiver == nil || receiver.Error != "" || !validTimestamp(receiver.Started) || !validTimestamp(origin) ||
		receiver.Receiver == nil {
		return nil, fmt.Errorf("missing completed receiver counter evidence")
	}
	return receiver.Receiver, nil
}

// measureThroughput passes only when the entire observed ratio range satisfies the unchanged 25 percent requirement.
func measureThroughput(trace *receiverTrace, origin float64, windows []throughputWindow, minimumBaseline float64) ([]throughputMeasurement, error) {
	var measurements []throughputMeasurement
	var failures []error
	var baseline throughputMeasurement
	baselineValid := false
	for index, window := range windows {
		measurement, err := measureWindow(trace, origin, window)
		if err != nil {
			failures = append(failures, err)
		} else {
			if index == 0 {
				baseline = measurement
				baselineValid = baseline.LowerRate > 0 &&
					(minimumBaseline == 0 || baseline.LowerRate/minimumBaseline+throughputRoundoff >= 1)
				if !baselineValid {
					measurement.Status = "invalid"
					failures = append(failures, fmt.Errorf("baseline receiver throughput lower bound is %.6f Mbit/s, require at least %.3f Mbit/s and positive delivery",
						baseline.LowerRate/1e6, minimumBaseline/1e6))
				}
			} else if !baselineValid {
				measurement.Status = "invalid"
			} else {
				lower := measurement.LowerRate / baseline.UpperRate
				upper := measurement.UpperRate / baseline.LowerRate
				measurement.LowerFraction, measurement.UpperFraction = &lower, &upper
				if upper+throughputRoundoff < 0.25 {
					measurement.Status = "below-threshold"
					failures = append(failures, fmt.Errorf("%s receiver throughput is at most %.4f percent of baseline, require at least 25 percent",
						window.name, upper*100))
				} else if lower+throughputRoundoff < 0.25 {
					measurement.Status = "inconclusive"
					failures = append(failures, fmt.Errorf("%s receiver counter bounds straddle the 25 percent threshold: [%.4f,%.4f] percent",
						window.name, lower*100, upper*100))
				}
			}
		}
		measurements = append(measurements, measurement)
	}
	return measurements, errors.Join(failures...)
}

// measureWindow counts definite and possible application deliveries in [start,end), preserving boundary uncertainty.
func measureWindow(trace *receiverTrace, origin float64, window throughputWindow) (throughputMeasurement, error) {
	measurement := throughputMeasurement{Phase: window.name, Start: window.start, End: window.end, Status: "invalid"}
	if trace == nil || !validTimestamp(origin) || window.start < 0 || !(window.end > window.start) ||
		math.IsInf(window.end, 0) || origin+window.end >= float64(math.MaxInt64)/1e9 {
		return measurement, fmt.Errorf("%s has invalid receiver window or missing counter evidence", window.name)
	}
	start := int64(math.Round(origin*1e9)) + int64(math.Round(window.start*1e9))
	end := int64(math.Round(origin*1e9)) + int64(math.Round(window.end*1e9))
	// Before receiver initialization all counters are zero. After its final snapshot no coverage is invented.
	if end > trace.Ended {
		return measurement, fmt.Errorf("%s lacks complete receiver counter coverage", window.name)
	}
	for _, event := range trace.Events {
		if event.Began >= end {
			break
		}
		if event.Ended >= start {
			measurement.UpperBytes += event.Bytes
			if event.Began >= start && event.Ended < end {
				measurement.LowerBytes += event.Bytes
			}
		}
	}
	measurement.Covered = window.end - window.start
	measurement.LowerRate = float64(measurement.LowerBytes) * 8 / measurement.Covered
	measurement.UpperRate = float64(measurement.UpperBytes) * 8 / measurement.Covered
	measurement.Status = "pass"
	return measurement, nil
}

// hasPathFault identifies ordinary interruption, address-change, and route-failure scenarios.
func hasPathFault(scenario string) bool {
	return strings.HasSuffix(scenario, "-prohibit") || strings.HasSuffix(scenario, "-blackhole") ||
		strings.HasSuffix(scenario, "-stall") || strings.HasSuffix(scenario, "-outage") || strings.HasSuffix(scenario, "-roam")
}

// verifyProgress requires receiver delivery in the complete final second of each exercised direction.
func verifyProgress(scenario string, flow flowResult, duration float64) ([]throughputMeasurement, error) {
	var measurements []throughputMeasurement
	var failures []error
	for _, direction := range receiverDirections(scenario, &flow) {
		trace, err := receiverEvidence(direction.receiver, flow.Started)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s direction: %w", direction.name, err))
			continue
		}
		measurement, err := measureWindow(trace, flow.Started, throughputWindow{name: "final progress", start: duration - 1, end: duration})
		measurement.Direction = direction.name
		if err == nil && measurement.LowerBytes == 0 {
			measurement.Status = "no-progress"
			err = fmt.Errorf("no receiver progress in the final second after path recovery")
			if measurement.UpperBytes > 0 {
				measurement.Status = "inconclusive"
				err = fmt.Errorf("receiver counter bounds do not establish progress in the final second")
			}
		}
		measurements = append(measurements, measurement)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s direction: %w", direction.name, err))
		}
	}
	return measurements, errors.Join(failures...)
}

// readTimestamp reads an observed data-flow or fault event on the shared Linux boot clock.
func readTimestamp(path string) (float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	at, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	if !validTimestamp(at) {
		return 0, fmt.Errorf("%s: invalid event timestamp", filepath.Base(path))
	}
	return at, nil
}

// validTimestamp identifies a finite, positive observation on the Linux boot clock.
func validTimestamp(at float64) bool {
	return at > 0 && !math.IsNaN(at) && !math.IsInf(at, 0)
}

// verifyFaultTimes requires both fault events to follow the actual client data-flow start within one second.
func verifyFaultTimes(directory string, started float64, prefix string, dropped, recovered float64) error {
	if !validTimestamp(started) {
		return fmt.Errorf("missing flow timestamp for %s fault", prefix)
	}
	var failures []error
	var observed [2]float64
	for index, event := range []struct {
		name string
		want float64
	}{
		{name: "dropped", want: dropped},
		{name: "recovered", want: recovered},
	} {
		name := prefix + "-" + event.name + ".txt"
		at, err := readTimestamp(filepath.Join(directory, name))
		if err != nil {
			failures = append(failures, err)
		} else if math.Abs(at-started-event.want) > 1 {
			failures = append(failures, fmt.Errorf("%s changed at %.6f seconds, expected %.0f +/- 1",
				name, at-started, event.want))
		}
		observed[index] = at
	}
	if len(failures) == 0 {
		duration := observed[1] - observed[0]
		if duration < 0 || math.Abs(duration-(recovered-dropped)) > 1 {
			failures = append(failures, fmt.Errorf("%s fault lasted %.6f seconds, expected %.0f +/- 1",
				prefix, duration, recovered-dropped))
		}
	}
	return errors.Join(failures...)
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

// verifyCapacityChangeFault requires the prescribed fault timing and retained queues on both endpoints.
func verifyCapacityChangeFault(directory string, started float64) error {
	if !validTimestamp(started) {
		return fmt.Errorf("missing flow timestamp for capacity change")
	}
	for _, role := range []string{"client", "server"} {
		var changed [2]float64
		for index, change := range []struct {
			name string
			at   float64
		}{
			{name: "dropped", at: 5},
			{name: "restored", at: 12},
		} {
			name := role + "-rate-" + change.name + ".txt"
			at, err := readTimestamp(filepath.Join(directory, name))
			if err != nil {
				return err
			}
			changed[index] = at - started
			if math.Abs(changed[index]-change.at) > 1 {
				return fmt.Errorf("%s changed at %.6f seconds, expected %.0f +/- 1", name, changed[index], change.at)
			}
		}
		if changed[1]-changed[0] < 6 || changed[1]-changed[0] > 8 {
			return fmt.Errorf("%s capacity fault lasted %.6f seconds, expected approximately 7", role, changed[1]-changed[0])
		}
		if err := verifyCapacityChangeQdiscs(directory, role); err != nil {
			return err
		}
	}
	return nil
}

// verifyCapacityChangeQdiscs checks shaping, counter continuity, and loss-free queue recovery.
func verifyCapacityChangeQdiscs(directory, role string) error {
	var previousBytes, previousDrops [2]uint64
	for _, phase := range []struct{ name, rate, limit string }{
		{name: "initial", rate: "1Gbit", limit: "10000"},
		{name: "limited", rate: "32Kbit", limit: "8"},
		{name: "restored", rate: "1Gbit", limit: "10000"},
		{name: "final", rate: "1Gbit", limit: "10000"},
	} {
		name := role + "-qdisc-" + phase.name + ".txt"
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		for index, pattern := range capacityChangeQdiscPatterns {
			match := pattern.FindStringSubmatch(string(data))
			if match == nil {
				return fmt.Errorf("%s lacks capacity queue %d evidence", name, index)
			}
			configuration := " " + strings.Join(strings.Fields(match[1]), " ") + " "
			settings := []string{"rate " + phase.rate}
			if index == 0 && phase.name == "limited" {
				settings = append(settings, "burst 4Kb", "lat 1s")
			}
			if index == 1 {
				settings = []string{"limit " + phase.limit}
				if phase.name == "limited" {
					settings = append(settings, "delay 100ms 20ms", "loss 0.2%")
				} else if strings.Contains(configuration, " delay ") || strings.Contains(configuration, " loss ") {
					return fmt.Errorf("%s retains delay or loss outside the fault", name)
				}
			}
			for _, setting := range settings {
				if !strings.Contains(configuration, " "+setting+" ") {
					return fmt.Errorf("%s lacks expected %s", name, setting)
				}
			}
			bytes, err := strconv.ParseUint(match[2], 10, 64)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			drops, err := strconv.ParseUint(match[3], 10, 64)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if bytes < previousBytes[index] || drops < previousDrops[index] {
				return fmt.Errorf("%s resets capacity queue %d counters", name, index)
			}
			if (phase.name == "limited" || phase.name == "final") && bytes == previousBytes[index] {
				return fmt.Errorf("%s records no additional shaped traffic in queue %d", name, index)
			}
			if phase.name == "initial" && drops != 0 || phase.name == "final" && drops != previousDrops[index] {
				return fmt.Errorf("%s records packet drops outside the capacity fault in queue %d", name, index)
			}
			previousBytes[index], previousDrops[index] = bytes, drops
		}
	}
	return nil
}

// verifyRecovery checks every exercised receiver against its own pre-fault delivery and the prescribed flow deadline.
func verifyRecovery(scenario string, flow flowResult, duration, minimumBaseline float64) ([]throughputMeasurement, error) {
	var measurements []throughputMeasurement
	var failures []error
	for _, direction := range receiverDirections(scenario, &flow) {
		trace, err := receiverEvidence(direction.receiver, flow.Started)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s direction: %w", direction.name, err))
			continue
		}
		measured, err := measureThroughput(trace, flow.Started, []throughputWindow{
			{name: "baseline", start: 0, end: 4},
			{name: "recovered", start: duration - 5, end: duration},
		}, minimumBaseline)
		for index := range measured {
			measured[index].Direction = direction.name
		}
		measurements = append(measurements, measured...)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s direction: %w", direction.name, err))
		}
	}
	return measurements, errors.Join(failures...)
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
