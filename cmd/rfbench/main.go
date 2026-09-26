// rfbench is the RelayForge load generator and benchmark driver.
//
// Modes:
//
//	produce  - N records of -size bytes through Produce (per ack mode)
//	consume  - drain the topic, measuring catch-up throughput
//
// Latency samples are per-record (a batch RPC contributes duration/n);
// throughput is counted records/sec (warmup excluded). Results go to -out
// (JSON) and stderr (human summary). See docs/BENCHMARKS.md for the
// methodology and the recorded results.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
	"relayforge/bench"
)

func main() {
	mode := flag.String("mode", "produce", "produce | consume")
	addr := flag.String("addr", "127.0.0.1:7000", "broker address")
	topic := flag.String("topic", "default", "topic")
	ack := flag.String("ack", "written", "ack mode: none | written | flushed")
	records := flag.Int("records", 10000, "number of records")
	size := flag.Int("size", 1024, "record value size in bytes")
	batch := flag.Int("batch", 100, "messages per Produce RPC")
	producers := flag.Int("producers", 1, "concurrent producers")
	warmup := flag.Int("warmup", 500, "warmup records (excluded from samples)")
	timeout := flag.Duration("timeout", 30*time.Second, "per-RPC timeout")
	notes := flag.String("notes", "", "environment notes (filesystem, disk, kernel)")
	out := flag.String("out", "", "write JSON report to this path")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	client := relayforgev1.NewRelayForgeClient(conn)

	env := bench.RecordEnv(*notes)
	started := time.Now()
	rec := &bench.Recorder{}
	var counted atomic.Int64 // records included in the report (warmup excluded)

	switch *mode {
	case "produce":
		runProduce(client, rec, &counted, *topic, *ack, *records, *warmup, *size, *batch, *producers, *timeout)
	case "consume":
		runConsume(client, rec, &counted, *topic, *records, *timeout)
	default:
		fatal(fmt.Errorf("unknown mode %q", *mode))
	}

	bytesTotal := counted.Load() * int64(*size)
	rep := rec.Finalize(started, counted.Load(), bytesTotal)
	summary := fmt.Sprintf("%s: records=%d rpcs=%d err=%d throughput=%.0f rec/s (%.1f MB/s) p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
		*mode, rep.Records, rep.RPCSamples, rep.Errors, rep.Throughput,
		float64(bytesTotal)/1e6/rep.Duration, rep.P50Ms, rep.P95Ms, rep.P99Ms, rep.MaxMs)
	fmt.Fprintln(os.Stderr, summary)
	if *out != "" {
		extra := map[string]any{"mode": *mode, "topic": *topic, "ack": *ack, "records": *records,
			"size_bytes": *size, "batch": *batch, "producers": *producers, "warmup": *warmup}
		if err := bench.WriteJSON(*out, env, extra, rep); err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "report written to %s\n", *out)
	}
	if rep.Errors > 0 {
		os.Exit(1)
	}
}

func runProduce(client relayforgev1.RelayForgeClient, r *bench.Recorder, counted *atomic.Int64, topic, ack string, records, warmup, size, batch, producers int, timeout time.Duration) {
	var ackMode relayforgev1.AckMode
	switch ack {
	case "none":
		ackMode = relayforgev1.AckMode_ACK_MODE_NONE
	case "flushed":
		ackMode = relayforgev1.AckMode_ACK_MODE_FLUSHED
	default:
		ackMode = relayforgev1.AckMode_ACK_MODE_WRITTEN
	}
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i % 251)
	}
	var wg sync.WaitGroup
	perProducer := (records - warmup) / producers
	warmEach := warmup / producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			send := func(count, base int, warm bool) {
				for i := 0; i < count; {
					n := batch
					if i+n > count {
						n = count - i
					}
					msgs := make([]*relayforgev1.Message, n)
					for j := 0; j < n; j++ {
						msgs[j] = &relayforgev1.Message{Key: []byte(fmt.Sprintf("k%d", (base+i+j)%16)), Value: value}
					}
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					start := time.Now()
					_, err := client.Produce(ctx, &relayforgev1.ProduceRequest{Topic: topic, AckMode: ackMode, Messages: msgs})
					cancel()
					if err != nil {
						r.Fail()
					} else {
						r.Observe(time.Since(start) / time.Duration(n))
						if !warm {
							counted.Add(int64(n))
						}
					}
					i += n
				}
			}
			send(warmEach, 0, true)
			send(perProducer, warmEach, false)
		}(p)
	}
	wg.Wait()
}

func runConsume(client relayforgev1.RelayForgeClient, r *bench.Recorder, counted *atomic.Int64, topic string, records int, timeout time.Duration) {
	var cursor uint64
	processed := 0
	for processed < records {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		resp, err := client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: topic, Offset: cursor, MaxBytes: 4 << 20, MaxWaitMs: 200})
		cancel()
		if err != nil {
			r.Fail()
			continue
		}
		n := len(resp.GetRecords())
		if n > 0 {
			r.Observe(time.Since(start) / time.Duration(n))
			cursor = resp.GetNextOffset()
			processed += n
			counted.Store(int64(processed))
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "rfbench:", err)
	os.Exit(1)
}
