// Package observability wires Prometheus metrics, the gRPC metrics
// interceptor, and the admin HTTP server (/metrics, /debug/pprof,
// /healthz). Every metric exists to answer a specific question — the
// catalog with those questions lives in docs/OBSERVABILITY.md.
package observability

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Metrics holds the broker's metric collectors. Nil *Metrics is tolerated
// by the broker (no-op instrumentation) so tests can run uninstrumented.
type Metrics struct {
	MessagesIn     *prometheus.CounterVec   // topic, partition
	BytesIn        *prometheus.CounterVec   // topic, partition
	MessagesOut    *prometheus.CounterVec   // topic, partition
	BytesOut       *prometheus.CounterVec   // topic, partition
	AppendLatency  *prometheus.HistogramVec // ack
	FetchLatency   prometheus.Histogram
	QueueDepth     *prometheus.GaugeVec   // topic, partition
	Overloaded     prometheus.Counter     // produce submissions rejected with ErrOverloaded
	PartitionFails prometheus.Counter     // writer panics
	Commits        *prometheus.CounterVec // result=ok|fenced|error
	GroupMembers   *prometheus.GaugeVec   // group
	RecoveryBytes  prometheus.Gauge       // bytes truncated by the last Open
	ReplicationLag *prometheus.GaugeVec   // topic, partition (follower mode)
	ReplicationErr prometheus.Counter     // follower fetch errors
	rpcRequests    *prometheus.CounterVec // method, code
	rpcDuration    *prometheus.HistogramVec
}

// New registers the metric set on the given registerer.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		MessagesIn: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_messages_in_total",
			Help: "Messages produced, by topic and partition.",
		}, []string{"topic", "partition"}),
		BytesIn: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_bytes_in_total",
			Help: "Produced payload bytes (key+value), by topic and partition.",
		}, []string{"topic", "partition"}),
		MessagesOut: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_messages_out_total",
			Help: "Messages fetched, by topic and partition.",
		}, []string{"topic", "partition"}),
		BytesOut: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_bytes_out_total",
			Help: "Fetched payload bytes (key+value), by topic and partition.",
		}, []string{"topic", "partition"}),
		AppendLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "relayforge_append_latency_seconds",
			Help:    "Engine append duration by acknowledgement mode (fsync included for flushed).",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		}, []string{"ack"}),
		FetchLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "relayforge_fetch_latency_seconds",
			Help:    "Fetch RPC duration including the long-poll window.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 5},
		}),
		QueueDepth: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "relayforge_queue_depth",
			Help: "Produce queue occupancy per partition (capacity is the bound).",
		}, []string{"topic", "partition"}),
		Overloaded: f.NewCounter(prometheus.CounterOpts{
			Name: "relayforge_overloaded_total",
			Help: "Produce RPCs rejected because a partition queue stayed full.",
		}),
		PartitionFails: f.NewCounter(prometheus.CounterOpts{
			Name: "relayforge_partition_writer_failures_total",
			Help: "Partition writer panics.",
		}),
		Commits: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_offset_commits_total",
			Help: "Offset commits by result: ok, fenced, error.",
		}, []string{"result"}),
		GroupMembers: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "relayforge_group_members",
			Help: "Members per consumer group.",
		}, []string{"group"}),
		RecoveryBytes: f.NewGauge(prometheus.GaugeOpts{
			Name: "relayforge_recovery_truncated_bytes",
			Help: "Bytes discarded by the log recovery at the last broker start.",
		}),
		ReplicationLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "relayforge_replication_lag",
			Help: "Follower distance from the leader's log end, per topic/partition.",
		}, []string{"topic", "partition"}),
		ReplicationErr: f.NewCounter(prometheus.CounterOpts{
			Name: "relayforge_replication_errors_total",
			Help: "Follower replication fetch failures.",
		}),
		rpcRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "relayforge_rpc_requests_total",
			Help: "gRPC requests by method and status code.",
		}, []string{"method", "code"}),
		rpcDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "relayforge_rpc_duration_seconds",
			Help:    "gRPC request duration by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
	}
	reg.MustRegister(collectors.NewGoCollector())
	return m
}

// RPCInterceptor records per-method request counts and durations.
func (m *Metrics) RPCInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		if m != nil {
			m.rpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
			m.rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		}
		return resp, err
	}
}

// ProduceObs records one produced message.
func (m *Metrics) ProduceObs(topic string, partition int32, msgs, bytes int, ack string, latency time.Duration) {
	if m == nil {
		return
	}
	p := partitionLabel(partition)
	m.MessagesIn.WithLabelValues(topic, p).Add(float64(msgs))
	m.BytesIn.WithLabelValues(topic, p).Add(float64(bytes))
	m.AppendLatency.WithLabelValues(ack).Observe(latency.Seconds())
}

// FetchObs records one fetch batch.
func (m *Metrics) FetchObs(topic string, partition int32, msgs, bytes int, latency time.Duration) {
	if m == nil {
		return
	}
	p := partitionLabel(partition)
	m.MessagesOut.WithLabelValues(topic, p).Add(float64(msgs))
	m.BytesOut.WithLabelValues(topic, p).Add(float64(bytes))
	m.FetchLatency.Observe(latency.Seconds())
}

// QueueObs updates a partition's queue-depth gauge.
func (m *Metrics) QueueObs(topic string, partition int32, depth int) {
	if m == nil {
		return
	}
	m.QueueDepth.WithLabelValues(topic, partitionLabel(partition)).Set(float64(depth))
}

// OverloadInc counts an overload rejection.
func (m *Metrics) OverloadInc() {
	if m != nil {
		m.Overloaded.Inc()
	}
}

// PartitionFailInc counts a writer panic.
func (m *Metrics) PartitionFailInc() {
	if m != nil {
		m.PartitionFails.Inc()
	}
}

// CommitObs counts a commit by result.
func (m *Metrics) CommitObs(result string) {
	if m != nil {
		m.Commits.WithLabelValues(result).Inc()
	}
}

// MembersObs sets the member gauge for a group.
func (m *Metrics) MembersObs(group string, members int) {
	if m != nil {
		m.GroupMembers.WithLabelValues(group).Set(float64(members))
	}
}

// RecoveryObs sets the recovery gauge.
func (m *Metrics) RecoveryObs(bytes int64) {
	if m != nil {
		m.RecoveryBytes.Set(float64(bytes))
	}
}

// ReplicationLagObs sets the follower lag gauge for a partition.
func (m *Metrics) ReplicationLagObs(topic string, partition int32, lag uint64) {
	if m != nil {
		m.ReplicationLag.WithLabelValues(topic, partitionLabel(partition)).Set(float64(lag))
	}
}

func (m *Metrics) ReplicationErrorInc() {
	if m != nil {
		m.ReplicationErr.Inc()
	}
}

func partitionLabel(p int32) string {
	switch {
	case p < 0:
		return "auto"
	default:
		return itoa(int(p))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// NewAdminServer builds the admin HTTP mux: the plain-language Overview
// page at / (when an overview source is provided), /metrics,
// /debug/pprof/*, /healthz. readiness reports whether the broker is
// serving.
func NewAdminServer(reg *prometheus.Registry, readiness func() bool, overview func() OverviewData) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if readiness == nil || readiness() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	if overview != nil {
		mux.Handle("/", OverviewHandler(overview))
	}
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// Collector adapts a snapshot function into a Prometheus collector (used
// for consumer lag, computed fresh on scrape).
type Collector struct {
	desc *prometheus.Desc
	snap func() []LagRow
}

// LagRow is one consumer-lag sample.
type LagRow struct {
	Group, Topic string
	Partition    int32
	Lag          uint64
}

// NewLagCollector builds a collector from a snapshot function.
func NewLagCollector(snap func() []LagRow) *Collector {
	return &Collector{
		desc: prometheus.NewDesc("relayforge_consumer_lag",
			"Log end minus committed offset, per group/topic/partition.", nil, nil),
		snap: snap,
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	for _, r := range c.snap() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(r.Lag),
			r.Group, r.Topic, itoa(int(r.Partition)))
	}
}
