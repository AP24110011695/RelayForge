// relayctl is the RelayForge operational CLI.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	relayforgev1 "relayforge/api/proto/relayforge/v1"

	"relayforge/internal/version"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "produce":
		err = cmdProduce(os.Args[2:])
	case "fetch":
		err = cmdFetch(os.Args[2:])
	case "topic":
		err = cmdTopic(os.Args[2:])
	case "health":
		err = cmdHealth(os.Args[2:])
	case "version":
		fmt.Println(version.Get())
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		if st, ok := status.FromError(err); ok {
			fmt.Fprintln(os.Stderr, "error:", st.Code(), st.Message())
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: relayctl <command> [flags]

commands:
  produce  append messages (flags or stdin, one message per line)
  fetch    read records from a topic partition
  topic    create/list/describe topics
  health   check broker serving status
  version  print build information`)
}

func cmdTopic(args []string) error {
	if len(args) == 0 {
		return errors.New("topic requires a subcommand: create, list, describe")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("topic create", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:7000", "broker address")
		name := fs.String("name", "", "topic name")
		partitions := fs.Int("partitions", 1, "partition count (fixed at creation)")
		timeout := fs.Duration("timeout", 10*time.Second, "rpc timeout")
		fs.Parse(args[1:])
		if *name == "" {
			return errors.New("topic create: -name is required")
		}
		conn, err := dial(*addr, *timeout)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		_, err = relayforgev1.NewRelayForgeClient(conn).CreateTopic(ctx,
			&relayforgev1.CreateTopicRequest{Name: *name, PartitionCount: int32(*partitions)})
		if err != nil {
			return err
		}
		fmt.Printf("created %s with %d partitions\n", *name, *partitions)
		return nil
	case "list":
		fs := flag.NewFlagSet("topic list", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:7000", "broker address")
		timeout := fs.Duration("timeout", 10*time.Second, "rpc timeout")
		fs.Parse(args[1:])
		conn, err := dial(*addr, *timeout)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		resp, err := relayforgev1.NewRelayForgeClient(conn).ListTopics(ctx, &relayforgev1.ListTopicsRequest{})
		if err != nil {
			return err
		}
		for _, t := range resp.GetTopics() {
			fmt.Printf("%s\tpartitions=%d\n", t.GetName(), t.GetPartitionCount())
		}
		return nil
	case "describe":
		fs := flag.NewFlagSet("topic describe", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:7000", "broker address")
		name := fs.String("name", "", "topic name")
		timeout := fs.Duration("timeout", 10*time.Second, "rpc timeout")
		fs.Parse(args[1:])
		if *name == "" {
			return errors.New("topic describe: -name is required")
		}
		conn, err := dial(*addr, *timeout)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		resp, err := relayforgev1.NewRelayForgeClient(conn).DescribeTopic(ctx, &relayforgev1.DescribeTopicRequest{Name: *name})
		if err != nil {
			return err
		}
		fmt.Printf("%s\tpartitions=%d\n", resp.GetTopic().GetName(), resp.GetTopic().GetPartitionCount())
		return nil
	default:
		return fmt.Errorf("unknown topic subcommand %q", args[0])
	}
}

func dial(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			var d net.Dialer
			dctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return d.DialContext(dctx, "tcp", s)
		}))
}

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7000", "broker address")
	topic := fs.String("topic", "default", "topic")
	partition := fs.Int("partition", -1, "pin all messages to this partition (-1 = route by key, keyless round-robin)")
	key := fs.String("key", "", "key for all messages")
	ack := fs.String("ack", "written", "acknowledgement mode: none, written, flushed")
	msg := fs.String("m", "", "single message value (stdin is read when omitted)")
	timeout := fs.Duration("timeout", 10*time.Second, "rpc timeout")
	fs.Parse(args)

	var ackMode relayforgev1.AckMode
	switch strings.ToLower(*ack) {
	case "none":
		ackMode = relayforgev1.AckMode_ACK_MODE_NONE
	case "flushed":
		ackMode = relayforgev1.AckMode_ACK_MODE_FLUSHED
	case "written", "":
		ackMode = relayforgev1.AckMode_ACK_MODE_WRITTEN
	default:
		return fmt.Errorf("unknown ack mode %q", *ack)
	}

	var values [][]byte
	if *msg != "" {
		values = append(values, []byte(*msg))
	} else {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			values = append(values, append([]byte(nil), line...))
		}
		if err := sc.Err(); err != nil {
			return err
		}
	}

	conn, err := dial(*addr, *timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := relayforgev1.NewRelayForgeClient(conn)
	req := &relayforgev1.ProduceRequest{
		Topic:     *topic,
		Partition: int32(*partition),
		AckMode:   ackMode,
	}
	for _, v := range values {
		req.Messages = append(req.Messages, &relayforgev1.Message{Key: []byte(*key), Value: v})
	}
	resp, err := client.Produce(ctx, req)
	if err != nil {
		return err
	}
	for _, off := range resp.GetOffsets() {
		fmt.Println(off)
	}
	return nil
}

func cmdFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7000", "broker address")
	topic := fs.String("topic", "default", "topic")
	partition := fs.Int("partition", 0, "partition")
	offset := fs.Uint64("offset", 0, "offset to start from")
	maxBytes := fs.Int("max-bytes", 0, "max bytes to return (0 = server default)")
	maxWaitMs := fs.Int("max-wait-ms", 0, "long-poll window in ms")
	timeout := fs.Duration("timeout", 30*time.Second, "rpc timeout")
	fs.Parse(args)

	conn, err := dial(*addr, *timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := relayforgev1.NewRelayForgeClient(conn)
	resp, err := client.Fetch(ctx, &relayforgev1.FetchRequest{
		Topic:     *topic,
		Partition: int32(*partition),
		Offset:    *offset,
		MaxBytes:  int32(*maxBytes),
		MaxWaitMs: int32(*maxWaitMs),
	})
	if err != nil {
		return err
	}
	for _, rec := range resp.GetRecords() {
		line := fmt.Sprintf("offset=%d ts=%d", rec.GetOffset(), rec.GetTimestampMs())
		if len(rec.GetKey()) > 0 {
			line += " key=" + strconv.Quote(string(rec.GetKey()))
		}
		line += " value=" + strconv.Quote(string(rec.GetValue()))
		fmt.Println(line)
	}
	fmt.Printf("next=%d high-watermark=%d\n", resp.GetNextOffset(), resp.GetHighWatermark())
	return nil
}

func cmdHealth(args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7000", "broker address")
	timeout := fs.Duration("timeout", 5*time.Second, "rpc timeout")
	fs.Parse(args)

	conn, err := dial(*addr, *timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	fmt.Println(resp.GetStatus().String())
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		os.Exit(1)
	}
	return nil
}
