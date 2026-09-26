// Package bench implements the measurement core for RelayForge
// benchmarks: latency recording with percentile reporting and
// environment recording for reproducibility. Numbers without the
// environment block are meaningless; every run must carry both.
package bench

import (
	"encoding/json"
	"os"
	"sort"
	"time"

	"relayforge/internal/version"
)

// Recorder samples producer-side latencies (send → ack, including queue
// wait and fsync when applicable) and reports percentiles over sorted
// samples (accuracy bounded by sample count; documented in BENCHMARKS.md).
// Latency samples are per-record (a batch RPC contributes duration/n);
// record COUNTS are tracked separately and passed to Finalize, because
// throughput is a records/sec figure, not an RPC/sec figure.
type Recorder struct {
	samples []time.Duration
	errors  int
}

// Observe records one operation latency.
func (r *Recorder) Observe(d time.Duration) { r.samples = append(r.samples, d) }

// Fail records one failed operation (excluded from latency, counted).
func (r *Recorder) Fail() { r.errors++ }

// Report summarizes a run.
type Report struct {
	Records    int     `json:"records"` // counted records (warmup excluded)
	RPCSamples int     `json:"rpc_samples"`
	Errors     int     `json:"errors"`
	Throughput float64 `json:"records_per_sec"`
	Duration   float64 `json:"duration_seconds"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	MaxMs      float64 `json:"max_ms"`
	BytesTotal int64   `json:"bytes_total"`
}

// Finalize computes the report. startedAt must be the run's true start
// (before warmup exclusion, if the caller excluded warmup from samples).
func (r *Recorder) Finalize(startedAt time.Time, records int64, bytesTotal int64) Report {
	n := len(r.samples)
	rep := Report{Records: int(records), RPCSamples: n, Errors: r.errors, BytesTotal: bytesTotal}
	rep.Duration = time.Since(startedAt).Seconds()
	if rep.Duration > 0 {
		rep.Throughput = float64(records) / rep.Duration
	}
	if n == 0 {
		return rep
	}
	sorted := make([]time.Duration, n)
	copy(sorted, r.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	rep.P50Ms = ms(sorted[n*50/100])
	rep.P95Ms = ms(sorted[n*95/100])
	rep.P99Ms = ms(sorted[min(n-1, n*99/100)])
	rep.MaxMs = ms(sorted[n-1])
	return rep
}

// Environment records the run context for reproducibility.
type Environment struct {
	Hostname  string `json:"hostname"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	GoVersion string `json:"go_version"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Notes     string `json:"notes"`
}

// RecordEnv captures the run environment.
func RecordEnv(notes string) Environment {
	host, _ := os.Hostname()
	info := version.Get()
	return Environment{
		Hostname:  host,
		OS:        osName(),
		Arch:      runtimeArch(),
		CPUs:      runtimeCPU(),
		GoVersion: info.GoVersion,
		Version:   info.Version,
		Commit:    info.Commit,
		Notes:     notes,
	}
}

// WriteJSON persists a report (+environment) to path.
func WriteJSON(path string, env Environment, extra map[string]any, rep Report) error {
	doc := map[string]any{"environment": env, "report": rep}
	for k, v := range extra {
		doc[k] = v
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
