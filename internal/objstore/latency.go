package objstore

import (
	"context"
	"io"
	"time"
)

// WithLatency wraps store so every operation first sleeps d, simulating a
// network backend for benchmarks and tests. Production code never wraps.
func WithLatency(store Store, d time.Duration) Store {
	return &latencyStore{inner: store, delay: d}
}

type latencyStore struct {
	inner Store
	delay time.Duration
}

func (l *latencyStore) sleep(ctx context.Context) error {
	timer := time.NewTimer(l.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *latencyStore) ReadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	if err := l.sleep(ctx); err != nil {
		return nil, err
	}
	return l.inner.ReadRange(ctx, key, off, length)
}

func (l *latencyStore) ReadAll(ctx context.Context, key string) ([]byte, error) {
	if err := l.sleep(ctx); err != nil {
		return nil, err
	}
	return l.inner.ReadAll(ctx, key)
}

func (l *latencyStore) Size(ctx context.Context, key string) (int64, error) {
	if err := l.sleep(ctx); err != nil {
		return 0, err
	}
	return l.inner.Size(ctx, key)
}

func (l *latencyStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := l.sleep(ctx); err != nil {
		return nil, err
	}
	return l.inner.List(ctx, prefix)
}
