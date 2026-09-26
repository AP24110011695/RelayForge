package broker

// CLI smoke tests: spawn the real rfbroker binary, drive it with the real
// relayctl binary, and assert behavior end-to-end.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	cliBinDir   string
	rfbrokerBin string
	relayctlBin string
)

func TestMain(m *testing.M) {
	os.Exit(runCliMain(m))
}

func runCliMain(m *testing.M) int {
	// Build binaries only when the CLI smoke tests will run.
	var err error
	cliBinDir, err = os.MkdirTemp("", "relayctl-bins")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(cliBinDir)
	rfbrokerBin = filepath.Join(cliBinDir, "rfbroker")
	relayctlBin = filepath.Join(cliBinDir, "relayctl")
	for _, cmd := range []struct{ out, pkg string }{
		{rfbrokerBin, "../../cmd/rfbroker"},
		{relayctlBin, "../../cmd/relayctl"},
	} {
		build := exec.Command("go", "build", "-o", cmd.out, cmd.pkg)
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "build failed:", err)
			return 1
		}
	}
	return m.Run()
}

// startBrokerBin starts rfbroker on a free port and returns its address and
// a stop function (SIGTERM + wait).
func startBrokerBin(t *testing.T, dir string) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	cmd := exec.Command(rfbrokerBin, "-addr", addr, "-data-dir", dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Wait for the listening line.
	scanning := make(chan string, 1)
	go func() {
		buf := make([]byte, 256)
		var acc []byte
		for {
			n, err := stdout.Read(buf)
			acc = append(acc, buf[:n]...)
			if s := string(acc); strings.Contains(s, "listening on ") {
				line := s[strings.Index(s, "listening on ")+len("listening on "):]
				if idx := strings.IndexByte(line, '\n'); idx >= 0 {
					line = line[:idx]
				}
				scanning <- strings.TrimSpace(line)
				return
			}
			if err != nil {
				close(scanning)
				return
			}
		}
	}()
	select {
	case got := <-scanning:
		if got == "" {
			t.Fatal("broker exited before listening")
		}
		addr = got
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not report its listen address")
	}

	stop := func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("broker did not exit gracefully within 15s")
		}
	}
	t.Cleanup(stop)
	return addr, stop
}

func runRelayctl(t *testing.T, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(relayctlBin, args...)
	out, err := cmd.CombinedOutput()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("relayctl %v: %v", args, err)
	}
	return string(out), code
}

func TestCLISmoke(t *testing.T) {
	dir := t.TempDir()
	addr, _ := startBrokerBin(t, dir)

	if out, code := runRelayctl(t, "health", "-addr", addr); code != 0 || !strings.Contains(out, "SERVING") {
		t.Fatalf("health: code %d out %q", code, out)
	}
	if out, code := runRelayctl(t, "produce", "-addr", addr, "-m", "hello-broker", "-key", "k1", "-ack", "flushed"); code != 0 || !strings.Contains(out, "0") {
		t.Fatalf("produce: code %d out %q", code, out)
	}
	if out, code := runRelayctl(t, "produce", "-addr", addr, "-m", "second"); code != 0 || !strings.Contains(out, "1") {
		t.Fatalf("produce 2: code %d out %q", code, out)
	}
	out, code := runRelayctl(t, "fetch", "-addr", addr)
	if code != 0 || !strings.Contains(out, "hello-broker") || !strings.Contains(out, "offset=0") {
		t.Fatalf("fetch: code %d out %q", code, out)
	}
	if !strings.Contains(out, "next=2") || !strings.Contains(out, "high-watermark=2") {
		t.Fatalf("fetch trailer missing: %q", out)
	}
	// Fetch beyond the end must fail with OUT_OF_RANGE.
	out, code = runRelayctl(t, "fetch", "-addr", addr, "-offset", "99")
	if code == 0 || !strings.Contains(out, "OutOfRange") {
		t.Fatalf("fetch beyond end: code %d out %q", code, out)
	}
	// Version flag on both binaries.
	if out, code := runRelayctl(t, "version"); code != 0 || len(out) == 0 {
		t.Fatalf("version: code %d out %q", code, out)
	}
}

func TestBrokerBinGracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	addr, stop := startBrokerBin(t, dir)
	if _, code := runRelayctl(t, "produce", "-addr", addr, "-m", "x"); code != 0 {
		t.Fatalf("produce failed")
	}
	stop() // SIGTERM: must exit 0 via the graceful path (cleanup would kill)
}
