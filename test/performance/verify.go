// Command verify compares each routed multipath flow with both standalone paths.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// flowResult contains the measured receiver rate and inner TCP retransmissions.
type flowResult struct {
	Error string
	End   struct {
		Received struct {
			Bytes         uint64
			BitsPerSecond float64 `json:"bits_per_second"`
			Seconds       float64
		} `json:"sum_received"`
		Sent struct {
			Retransmits uint64
		} `json:"sum_sent"`
	}
}

// droppedPattern extracts each qdisc's recorded packet drops.
var droppedPattern = regexp.MustCompile(`dropped ([0-9]+)`)

// shapedBytesPattern identifies actual traffic through each configured path qdisc.
var shapedBytesPattern = regexp.MustCompile(`qdisc netem (10|20):[^\n]*\n Sent ([0-9]+) bytes`)

// main checks topology evidence and requires combined median goodput to retain the best path's throughput.
func main() {
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Fprintln(os.Stderr, "usage: verify RESULTS_DIRECTORY REPETITIONS [BASELINE_DIRECTORY]")
		os.Exit(2)
	}
	repetitions, err := strconv.Atoi(os.Args[2])
	if err != nil || repetitions <= 0 {
		fmt.Fprintln(os.Stderr, "repetitions must be positive")
		os.Exit(2)
	}
	if err := verify(os.Args[1], repetitions); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(os.Args) == 4 {
		if err := compareBaseline(os.Args[1], os.Args[3], repetitions); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

// verify validates fresh flows and compares medians separately for each capacity profile.
func verify(root string, repetitions int) error {
	for _, profile := range []string{"HighCapacity", "LowCapacity"} {
		medians := make(map[string]float64)
		minimumCombined := math.Inf(1)
		for _, path := range []string{"Low", "High", "Both"} {
			var rates []float64
			for repetition := 1; repetition <= repetitions; repetition++ {
				directory := filepath.Join(root, fmt.Sprintf("%s-%d-%s", profile, repetition, path))
				flow, err := readFlow(directory)
				if err != nil {
					return err
				}
				rate := flow.End.Received.BitsPerSecond
				if err := verifyEvidence(directory, path, flow.End.Received.Bytes, false); err != nil {
					return fmt.Errorf("%s: %w", directory, err)
				}
				fmt.Printf("%s repetition=%d path=%s goodput=%.3f Mbit/s retransmits=%d\n",
					profile, repetition, path, rate/1e6, flow.End.Sent.Retransmits)
				rates = append(rates, rate)
				if path == "Both" {
					minimumCombined = min(minimumCombined, rate)
				}
			}
			medians[path] = median(rates)
		}
		best := max(medians["Low"], medians["High"])
		fmt.Printf("%s combined/best=%.3f\n", profile, medians["Both"]/best)
		if medians["Both"] < 0.85*best {
			return fmt.Errorf("%s: combined goodput retains less than 85 percent of the best standalone path", profile)
		}
		if minimumCombined < 0.85*best {
			return fmt.Errorf("%s: a combined flow retains less than 85 percent of the best standalone median", profile)
		}
	}
	return nil
}

// verifyEvidence rejects carrier churn, router bypass, and unexpected drops in a healthy comparison.
func verifyEvidence(directory, path string, minimumBytes uint64, allowDrops bool) error {
	var sockets [2][]string
	for index, name := range []string{"client-tcp-sockets.txt", "client-tcp-sockets-after.txt"} {
		encoded, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		var low, high int
		for _, line := range strings.Split(string(encoded), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}
			if strings.HasSuffix(fields[3], ":51822") {
				low++
				sockets[index] = append(sockets[index], fields[2]+" "+fields[3])
			}
			if strings.HasSuffix(fields[3], ":51823") {
				high++
				sockets[index] = append(sockets[index], fields[2]+" "+fields[3])
			}
		}
		wantLow, wantHigh := 1, 1
		if path == "Low" {
			wantHigh = 0
		}
		if path == "High" {
			wantLow = 0
		}
		if low != wantLow || high != wantHigh {
			return fmt.Errorf("%s: carrier counts %d/%d, want %d/%d", name, low, high, wantLow, wantHigh)
		}
		slices.Sort(sockets[index])
	}
	if !slices.Equal(sockets[0], sockets[1]) {
		return fmt.Errorf("carrier socket identities changed during the flow")
	}
	encoded, err := os.ReadFile(filepath.Join(directory, "router-qdisc.txt"))
	if err != nil {
		return err
	}
	matches := droppedPattern.FindAllSubmatch(encoded, -1)
	if len(matches) < 3 {
		return fmt.Errorf("missing router qdisc counters")
	}
	for _, match := range matches {
		if !allowDrops && string(match[1]) != "0" {
			return fmt.Errorf("router dropped %s packets", match[1])
		}
	}
	shaped := make(map[string]uint64)
	for _, match := range shapedBytesPattern.FindAllSubmatch(encoded, -1) {
		bytes, err := strconv.ParseUint(string(match[2]), 10, 64)
		if err != nil {
			return err
		}
		shaped[string(match[1])] = bytes
	}
	if path != "High" && shaped["10"] == 0 || path != "Low" && shaped["20"] == 0 {
		return fmt.Errorf("missing traffic through a selected path qdisc")
	}
	if shaped["10"]+shaped["20"] < minimumBytes {
		return fmt.Errorf("router shaped fewer bytes than the completed inner flow")
	}
	before, err := tcpActiveOpens(filepath.Join(directory, "client-before.txt"))
	if err != nil {
		return err
	}
	after, err := tcpActiveOpens(filepath.Join(directory, "client-after.txt"))
	if err != nil {
		return err
	}
	if after < before || after-before != 2 {
		return fmt.Errorf("unexpected TCP active opens during flow: before %d, after %d", before, after)
	}
	return nil
}

// readFlow requires a complete iperf3 receiver result before using it in either comparison.
func readFlow(directory string) (flowResult, error) {
	var flow flowResult
	encoded, err := os.ReadFile(filepath.Join(directory, "flow.json"))
	if err != nil {
		return flow, err
	}
	if err := json.Unmarshal(encoded, &flow); err != nil {
		return flow, fmt.Errorf("%s: %w", directory, err)
	}
	rate := flow.End.Received.BitsPerSecond
	if flow.Error != "" || flow.End.Received.Seconds < 19 || flow.End.Received.Bytes == 0 || rate <= 0 {
		return flow, fmt.Errorf("%s: incomplete flow: %s", directory, flow.Error)
	}
	return flow, nil
}

// median sorts a nonempty measured-rate vector and returns its middle value.
func median(rates []float64) float64 {
	slices.Sort(rates)
	middle := rates[len(rates)/2]
	if len(rates)%2 == 0 {
		middle = (middle + rates[len(rates)/2-1]) / 2
	}
	return middle
}

// compareBaseline rejects regressions concealed by comparing only paths within the candidate.
func compareBaseline(candidate, baseline string, repetitions int) error {
	for _, profile := range []string{"HighCapacity", "LowCapacity"} {
		for _, path := range []string{"Low", "High", "Both"} {
			var rates [2][]float64
			for index, root := range []string{candidate, baseline} {
				for repetition := 1; repetition <= repetitions; repetition++ {
					directory := filepath.Join(root, fmt.Sprintf("%s-%d-%s", profile, repetition, path))
					flow, err := readFlow(directory)
					if err != nil {
						return err
					}
					if err := verifyEvidence(directory, path, flow.End.Received.Bytes, index == 1); err != nil {
						return fmt.Errorf("%s: %w", directory, err)
					}
					rates[index] = append(rates[index], flow.End.Received.BitsPerSecond)
				}
			}
			current, previous := median(rates[0]), median(rates[1])
			fmt.Printf("%s path=%s candidate/baseline=%.3f\n", profile, path, current/previous)
			if current < 0.85*previous {
				return fmt.Errorf("%s path=%s: candidate retains less than 85 percent of baseline median", profile, path)
			}
		}
	}
	return nil
}

// tcpActiveOpens reads the namespace counter for locally initiated TCP connections.
func tcpActiveOpens(path string) (uint64, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(encoded)) {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "TcpActiveOpens" {
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("missing TCP active-open counter")
}
