package logengine

import (
	"context"
	"fmt"
	"os"
	"time"
)

// ApplyRetention enforces the configured policy once, synchronously and
// bounded: it drops sealed segments from the oldest end while they violate
// the age or size limit, never deleting the active segment. It stops at the
// first I/O error. With no policy configured it is a no-op.
func (l *Log) ApplyRetention(now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.retention.active() {
		return nil
	}
	for len(l.segments) > 1 {
		seg := l.segments[0]
		drop, err := l.shouldDropLocked(seg, now)
		if err != nil {
			return err
		}
		if !drop {
			return nil
		}
		if err := l.dropFrontLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) shouldDropLocked(seg *segment, now time.Time) (bool, error) {
	if l.retention.MaxBytes > 0 {
		var total int64
		for _, s := range l.segments {
			total += s.size
		}
		if total > l.retention.MaxBytes {
			return true, nil
		}
	}
	if l.retention.MaxAge > 0 {
		info, err := seg.logFile.Stat()
		if err != nil {
			return false, fmt.Errorf("logengine: stat segment %d for retention: %w", seg.base, err)
		}
		if now.Sub(info.ModTime()) > l.retention.MaxAge {
			return true, nil
		}
	}
	return false, nil
}

// dropFrontLocked closes and deletes the oldest segment. Only the active
// segment is never dropped: callers keep at least one segment.
func (l *Log) dropFrontLocked() error {
	seg := l.segments[0]
	seg.closeFiles()
	if err := os.Remove(segmentLogPath(l.dir, seg.base)); err != nil {
		return fmt.Errorf("logengine: removing segment %d log: %w", seg.base, err)
	}
	if err := os.Remove(segmentIndexPath(l.dir, seg.base)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logengine: removing segment %d index: %w", seg.base, err)
	}
	l.segments = l.segments[1:]
	return syncDir(l.dir)
}

// StartRetention runs ApplyRetention on a ticker until ctx is cancelled.
// The engine has no logger, so retention errors are dropped here; callers
// that need visibility should call ApplyRetention themselves. One goroutine
// per call; it exits when ctx is done.
func (l *Log) StartRetention(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = l.ApplyRetention(time.Now())
			}
		}
	}()
}
