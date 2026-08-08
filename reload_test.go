package cf_vpq

import (
	"testing"
	"time"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

func TestWithConfigSourceDeclaresConfigurationDependency(t *testing.T) {
	q := New(WithConfigSource("vpq", ""), WithQueueName("orders"))
	deps := q.GetDependencies()
	found := false
	for _, d := range deps {
		if d == cf_configuration.ComponentName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("deps = %v, want %q", deps, cf_configuration.ComponentName)
	}
}

func TestOnConfigReloadNoopWithoutSource(t *testing.T) {
	q := New(WithQueueName("orders"))
	q.OnConfigReload("vpq", nil) // must not panic
}

func TestRecoverDefaultsWithHandler(t *testing.T) {
	q := New(WithQueueName("orders"), WithHandler(func(*BGetObject) error { return nil }))
	if q.recoverInterval != defaultRecoverInterval {
		t.Fatalf("recoverInterval = %v, want %v", q.recoverInterval, defaultRecoverInterval)
	}
	if q.recoverMaxAge != defaultRecoverMaxAge {
		t.Fatalf("recoverMaxAge = %v, want %v", q.recoverMaxAge, defaultRecoverMaxAge)
	}
}

func TestRecoverDisabledExplicitly(t *testing.T) {
	q := New(
		WithQueueName("orders"),
		WithHandler(func(*BGetObject) error { return nil }),
		WithRecoverInterval(0),
	)
	if q.recoverInterval != 0 {
		t.Fatalf("recoverInterval = %v, want 0", q.recoverInterval)
	}
}

func TestRecoverWithoutHandlerStaysOff(t *testing.T) {
	q := New(WithQueueName("orders"))
	if q.recoverInterval != 0 {
		t.Fatalf("producer recoverInterval = %v, want 0", q.recoverInterval)
	}
}

func TestWithRecoverIntervalWithoutHandler(t *testing.T) {
	q := New(WithQueueName("orders"), WithRecoverInterval(15*time.Second))
	if q.recoverInterval != 15*time.Second {
		t.Fatalf("recoverInterval = %v, want 15s", q.recoverInterval)
	}
}

func TestWithConfigRecoverAndHealthFields(t *testing.T) {
	q := New(WithQueueName("orders"), WithConfig(PQConfig{
		RecoverInterval: 60,
		RecoverMaxAge:   120,
		MaxDepth:        1000,
		MaxInFlight:     50,
		Workers:         3,
	}))
	if q.recoverInterval != 60*time.Second {
		t.Fatalf("recoverInterval = %v, want 60s", q.recoverInterval)
	}
	if q.recoverMaxAge != 120*time.Second {
		t.Fatalf("recoverMaxAge = %v, want 120s", q.recoverMaxAge)
	}
	if q.maxDepth != 1000 || q.maxInFlight != 50 {
		t.Fatalf("thresholds depth=%d inflight=%d", q.maxDepth, q.maxInFlight)
	}
	if q.workers != 3 {
		t.Fatalf("workers = %d, want 3", q.workers)
	}
}

func TestHealthDefaultsWithHandler(t *testing.T) {
	q := New(WithQueueName("orders"), WithHandler(func(*BGetObject) error { return nil }))
	if q.maxDepth != defaultMaxDepth {
		t.Fatalf("maxDepth = %d, want %d", q.maxDepth, defaultMaxDepth)
	}
	if q.maxInFlight != defaultMaxInFlight(1) {
		t.Fatalf("maxInFlight = %d, want %d", q.maxInFlight, defaultMaxInFlight(1))
	}
	if q.workers != 1 {
		t.Fatalf("workers = %d, want 1", q.workers)
	}
}

func TestHealthDefaultsScaleWithWorkers(t *testing.T) {
	q := New(
		WithQueueName("orders"),
		WithHandler(func(*BGetObject) error { return nil }),
		WithWorkers(8),
	)
	if q.workers != 8 {
		t.Fatalf("workers = %d, want 8", q.workers)
	}
	if q.maxInFlight != defaultMaxInFlight(8) {
		t.Fatalf("maxInFlight = %d, want %d", q.maxInFlight, defaultMaxInFlight(8))
	}
}

func TestHealthDefaultsDisabled(t *testing.T) {
	q := New(
		WithQueueName("orders"),
		WithHandler(func(*BGetObject) error { return nil }),
		WithMaxDepth(0),
		WithMaxInFlight(0),
	)
	if q.maxDepth != 0 || q.maxInFlight != 0 {
		t.Fatalf("thresholds depth=%d inflight=%d, want both 0", q.maxDepth, q.maxInFlight)
	}
}

func TestHealthThresholdsOffWithoutHandler(t *testing.T) {
	q := New(WithQueueName("orders"))
	if q.maxDepth != 0 || q.maxInFlight != 0 {
		t.Fatalf("producer thresholds depth=%d inflight=%d, want 0", q.maxDepth, q.maxInFlight)
	}
}

func TestWithWorkersClampsBelowOne(t *testing.T) {
	q := New(WithQueueName("orders"), WithWorkers(0))
	if q.workers != 1 {
		t.Fatalf("workers = %d, want 1", q.workers)
	}
}
