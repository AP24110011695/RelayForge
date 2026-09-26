package bench

import (
	"testing"
	"time"
)

func TestRecorderPercentiles(t *testing.T) {
	r := &Recorder{}
	// 100 samples: 0..99ms — deterministic, easy to verify by hand.
	for i := 0; i < 100; i++ {
		r.Observe(time.Duration(i) * time.Millisecond)
	}
	start := time.Now().Add(-2 * time.Second)
	rep := r.Finalize(start, 100, 0)
	if rep.Records != 100 || rep.RPCSamples != 100 {
		t.Fatalf("records/samples = %d/%d", rep.Records, rep.RPCSamples)
	}
	if rep.P50Ms != 50 || rep.P95Ms != 95 || rep.MaxMs != 99 {
		t.Errorf("percentiles: p50=%v p95=%v max=%v", rep.P50Ms, rep.P95Ms, rep.MaxMs)
	}
	if rep.P99Ms != 99 {
		t.Errorf("p99 = %v (index min(99, 99) = 99)", rep.P99Ms)
	}
	if rep.Throughput <= 40 || rep.Throughput >= 60 {
		t.Errorf("throughput = %v, want ~50/s", rep.Throughput)
	}
}

func TestRecorderEmptyAndErrors(t *testing.T) {
	r := &Recorder{}
	rep := r.Finalize(time.Now(), 0, 0)
	if rep.Records != 0 || rep.RPCSamples != 0 || rep.P50Ms != 0 {
		t.Fatalf("empty recorder: %+v", rep)
	}
	r.Fail()
	r.Observe(time.Millisecond)
	rep = r.Finalize(time.Now().Add(-time.Second), 1, 0)
	if rep.Errors != 1 || rep.RPCSamples != 1 || rep.Records != 1 {
		t.Fatalf("errors/records/samples: %+v", rep)
	}
}
