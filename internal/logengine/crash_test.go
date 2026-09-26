package logengine

// Crash harness: a child process appends records and is SIGKILLed at
// controlled points by the parent, which then reopens the log and verifies
// the valid-prefix invariant.
//
// Honesty note (see docs/STORAGE.md): SIGKILL tests prove durability
// against PROCESS death (page cache survives). Power-loss durability of
// DurabilityFlushed is proven by construction — the fsync happens inside
// Append before the ack is printed (see TestFsyncBeforeReturnSeam for the
// ordering seam) — plus the documented filesystem assumptions.

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"relayforge/internal/record"
)

func TestMain(m *testing.M) {
	switch os.Getenv("RF_LOGENGINE_CHILD") {
	case "":
		os.Exit(m.Run())
	case "append":
		runCrashChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runCrashChild appends RF_CHILD_TOTAL records, printing "acked <i>" after
// each append returns (after the fsync, for flushed mode), then "done".
func runCrashChild() {
	dir := os.Getenv("RF_CHILD_DIR")
	total, err := strconv.Atoi(os.Getenv("RF_CHILD_TOTAL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "child: bad total:", err)
		os.Exit(1)
	}
	dur := DurabilityWritten
	if os.Getenv("RF_CHILD_SYNC") == "flushed" {
		dur = DurabilityFlushed
	}
	l, err := Open(dir, Options{MaxRecordSize: 4096, MaxSegmentBytes: 8192, IndexIntervalBytes: 1024})
	if err != nil {
		fmt.Fprintln(os.Stderr, "child: open:", err)
		os.Exit(1)
	}
	for i := 0; i < total; i++ {
		rec := record.Record{Value: []byte(fmt.Sprintf("crash-%06d", i))}
		if _, err := l.Append(rec, dur); err != nil {
			fmt.Fprintln(os.Stderr, "child: append:", err)
			os.Exit(1)
		}
		fmt.Printf("acked %d\n", i)
	}
	fmt.Println("done")
	l.Close()
}

type crashScenario struct {
	name            string
	total           int
	synced          bool
	killAfterAcks   int  // kill once this many acked lines arrived; -1 = clean exit
	killImmediately bool // kill without waiting for any ack
}

func TestCrashHarness(t *testing.T) {
	scenarios := []crashScenario{
		{name: "immediate-kill-flushed", total: 500, synced: true, killImmediately: true},
		{name: "immediate-kill-written", total: 500, synced: false, killImmediately: true},
		{name: "after-first-ack-flushed", total: 500, synced: true, killAfterAcks: 1},
		{name: "after-first-ack-written", total: 500, synced: false, killAfterAcks: 1},
		{name: "after-25-acks-flushed", total: 500, synced: true, killAfterAcks: 25},
		{name: "after-100-acks-written", total: 500, synced: false, killAfterAcks: 100},
		{name: "clean-exit-flushed", total: 300, synced: true, killAfterAcks: -1},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			maxAcked, cleanExit := runAndKill(t, dir, sc)

			// Parent-side verification: reopen and check the invariants.
			l, err := Open(dir, Options{MaxRecordSize: 4096, MaxSegmentBytes: 8192, IndexIntervalBytes: 1024})
			if err != nil {
				t.Fatalf("reopen after kill: %v", err)
			}
			defer l.Close()
			next := l.NextOffset()
			if next > uint64(sc.total) {
				t.Fatalf("NextOffset = %d exceeds what the child could have appended (%d)", next, sc.total)
			}
			if maxAcked >= 0 && next < uint64(maxAcked+1) {
				t.Fatalf("acked records lost: child acked up to %d, log holds %d", maxAcked, next)
			}
			if cleanExit && next != uint64(sc.total) {
				t.Fatalf("clean exit but log holds %d of %d records", next, sc.total)
			}
			// Prefix property: records are exactly crash-%06d in order.
			i := uint64(0)
			err = l.Scan(func(rec record.Record, offset uint64) error {
				if offset != i {
					t.Errorf("offset %d out of order (want %d)", offset, i)
				}
				if want := fmt.Sprintf("crash-%06d", i); string(rec.Value) != want {
					t.Errorf("record %d = %q, want %q", i, rec.Value, want)
				}
				i++
				return nil
			})
			if err != nil {
				t.Fatalf("scan after kill: %v", err)
			}
			if i != next {
				t.Errorf("scan saw %d records, NextOffset %d", i, next)
			}
		})
	}
}

// runAndKill spawns the child appender and kills it at the scenario's
// point. It returns the highest acked sequence observed on the child's
// stdout (-1 when none) and whether the child exited cleanly.
func runAndKill(t *testing.T, dir string, sc crashScenario) (maxAcked int, cleanExit bool) {
	t.Helper()
	syncEnv := "RF_CHILD_SYNC=written"
	if sc.synced {
		syncEnv = "RF_CHILD_SYNC=flushed"
	}
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"RF_LOGENGINE_CHILD=append",
		"RF_CHILD_DIR="+dir,
		fmt.Sprintf("RF_CHILD_TOTAL=%d", sc.total),
		syncEnv,
	)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	maxAcked = -1
	acks := 0
	lines := make(chan string, 64)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	readTimeout := time.After(30 * time.Second)
	for line := range lines {
		if strings.HasPrefix(line, "acked ") {
			i, err := strconv.Atoi(strings.TrimPrefix(line, "acked "))
			if err != nil {
				t.Fatalf("bad child line %q: %v", line, err)
			}
			maxAcked = i
			acks++
		}
		if line == "done" {
			cleanExit = true
			break
		}
		if sc.killImmediately {
			_ = cmd.Process.Kill()
			break
		}
		if sc.killAfterAcks >= 0 && acks >= sc.killAfterAcks {
			_ = cmd.Process.Kill()
			break
		}
		select {
		case <-readTimeout:
			_ = cmd.Process.Kill()
			t.Fatal("timed out waiting for child output")
		default:
		}
	}
	if !cleanExit {
		// Drain briefly so the child's exit does not block on a full pipe.
		go func() {
			for range lines {
			}
		}()
		// A kill is expected to produce a non-zero exit; anything else is
		// reported but not fatal (the child may also have just finished).
		_ = cmd.Wait()
	} else {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("clean-exit child failed: %v", err)
		}
	}
	return maxAcked, cleanExit
}
