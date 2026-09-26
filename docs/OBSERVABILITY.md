# Observability

Every metric answers a question an operator would actually ask. Endpoint:
admin HTTP server (`-admin-addr`, default `:7001`): `/metrics`,
`/debug/pprof/*`, `/healthz` (readiness: broker recovered and serving).

## Metric catalog

| Metric | Type | Labels | Question it answers |
| --- | --- | --- | --- |
| `relayforge_messages_in_total` | counter | topic, partition | Is the broker absorbing what producers send? |
| `relayforge_bytes_in_total` | counter | topic, partition | What is the ingest data rate (MB/s)? |
| `relayforge_messages_out_total` | counter | topic, partition | Are consumers draining at the same rate? |
| `relayforge_bytes_out_total` | counter | topic, partition | Consume bandwidth |
| `relayforge_append_latency_seconds` | histogram | ack | What does each durability level cost right now? (p50/p95/p99 per ack mode) |
| `relayforge_fetch_latency_seconds` | histogram | — | How long do fetches take, including long-poll waits? |
| `relayforge_queue_depth` | gauge | topic, partition | Is backpressure about to bite (depth near capacity)? |
| `relayforge_overloaded_total` | counter | — | How often is the broker explicitly refusing work? |
| `relayforge_partition_writer_failures_total` | counter | — | Did any partition's writer panic? |
| `relayforge_offset_commits_total` | counter | result | Are commits landing (ok) or being fenced (fenced)? A rising fenced rate means clients are rebalancing badly. |
| `relayforge_consumer_lag` | gauge (collector) | group, topic, partition | Who is behind, by how much, right now (computed fresh at scrape)? |
| `relayforge_group_members` | gauge | group | How many members does each group have? |
| `relayforge_recovery_truncated_bytes` | gauge | — | How much did the last start truncate (crash visibility)? |
| `relayforge_rpc_requests_total` | counter | method, code | Error rates by code per method |
| `relayforge_rpc_duration_seconds` | histogram | method | RPC latency distribution |
| `go_*`, `process_*` | default | — | Goroutines (leaks), heap (boundedness), CPU, fds |

## Label-cardinality policy

Labels are bounded by design: topic names (≤128 chars, ≤ bounded count),
partitions (≤256 per topic), group names, ack modes, and result enums.
Member IDs, offsets, and other unbounded strings are **never** labels.

## Profiling

pprof endpoints on the admin port: CPU (`/debug/pprof/profile`),
heap (`/debug/pprof/heap`), goroutines, mutex/block, trace. The Phase 9
benchmark workflow uses these under load; every optimization is
profile-justified (docs/BENCHMARKS.md).

## Dashboards

`deploy/dashboards/relayforge.json` (provisioned via
`deploy/grafana-datasource.yml` + `docker-compose.yml`) shows throughput,
latency by ack mode, consumer lag, queue depth, overload/failure counters,
and goroutines. NOTE: the Compose stack is authored but not yet verified
live — Docker Desktop WSL integration was disabled at Phase 8 execution
time (docs/ENVIRONMENT.md).
