package cf_vpq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
)

func TestComponentContract(t *testing.T) {
	q := New(WithQueueName("orders"))
	if q.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", q.Name(), ComponentName)
	}
	if q.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", q.GetInitOrderStage(), ComponentStage)
	}
	deps := q.GetDependencies()
	if len(deps) != 2 || deps[0] != "valkey" || deps[1] != "logs" {
		t.Fatalf("GetDependencies() = %v, want [valkey logs]", deps)
	}
	var _ cf.CaerusComponent = q
	var _ cf.Dependencies = q
	var _ cf.Runnable = q

	if _, err := q.Count(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Count before Init should return ErrClosed, got %v", err)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestHealthBeforeInit(t *testing.T) {
	q := New(WithQueueName("q"))
	if err := q.Health(context.Background()); err == nil {
		t.Fatal("Health before Init should fail")
	}
	if ms := q.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
	var _ cf.HealthProvider = q
	var _ cf_observability.MetricsProvider = q
	var _ cf.ConfigReloader = q
}

func TestWithName(t *testing.T) {
	// Default name
	q1 := New(WithQueueName("orders"))
	if q1.Name() != ComponentName {
		t.Fatalf("default Name() = %q, want %q", q1.Name(), ComponentName)
	}

	// Custom name
	q2 := New(WithQueueName("orders"), WithName("email-queue"))
	if q2.Name() != "email-queue" {
		t.Fatalf("custom Name() = %q, want email-queue", q2.Name())
	}

	// Multiple instances with different names
	q3 := New(WithQueueName("billing"), WithName("billing-queue"))
	if q3.Name() != "billing-queue" {
		t.Fatalf("custom Name() = %q, want billing-queue", q3.Name())
	}
}

func TestNewDefaults(t *testing.T) {
	q := New(WithQueueName("q"))
	if q.cfg.BlockDuration != 1 {
		t.Fatalf("BlockDuration = %d, want 1", q.cfg.BlockDuration)
	}
	if q.cfg.PollInterval != 1 {
		t.Fatalf("PollInterval = %d, want 1", q.cfg.PollInterval)
	}
	if q.cfg.PublishWatermarkDelay != 0 {
		t.Fatalf("PublishWatermarkDelay = %d, want 0 (disabled)", q.cfg.PublishWatermarkDelay)
	}
}

func TestWithConfigOverridesOptions(t *testing.T) {
	q := New(
		WithQueueName("from-options"),
		WithBlockDuration(5*time.Second),
		WithConfig(PQConfig{
			QueueName:    "from-config",
			KeyPrefix:    "prod",
			CacheTimeout: 60,
		}),
	)
	if q.cfg.QueueName != "from-config" {
		t.Fatalf("QueueName = %q, want from-config (config wins)", q.cfg.QueueName)
	}
	if q.cfg.KeyPrefix != "prod" {
		t.Fatalf("KeyPrefix = %q, want prod", q.cfg.KeyPrefix)
	}
	// zero config fields keep option-set defaults
	if q.cfg.BlockDuration != 5 {
		t.Fatalf("BlockDuration = %d, want 5 (empty config field keeps default)", q.cfg.BlockDuration)
	}
}

func TestKeyHelpers(t *testing.T) {
	q := New(WithQueueName("orders"))
	if got := q.stringKey("id1"); got != "squeue:orders:id1" {
		t.Fatalf("stringKey = %q", got)
	}
	if got := q.zsetKey(); got != "zqueue:orders" {
		t.Fatalf("zsetKey = %q", got)
	}
	if got := q.deadlockKey(); got != "pqdeadlocks:orders" {
		t.Fatalf("deadlockKey = %q", got)
	}
	if got := q.expiryKey(); got != "zexpiry:orders" {
		t.Fatalf("expiryKey = %q", got)
	}
	if got := q.wakeKey(); got != "pqwake:orders" {
		t.Fatalf("wakeKey = %q", got)
	}
	prefixed := New(WithQueueName("orders"), WithKeyPrefix("myapp:"))
	if got := prefixed.stringKey("id1"); got != "myapp:squeue:orders:id1" {
		t.Fatalf("prefixed stringKey = %q", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	q := New(WithQueueName("q"))
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _, _ = q.Count(context.Background()) }()
		go func() { defer wg.Done(); _ = q.IntCount(context.Background()) }()
		go func() { defer wg.Done(); _, _ = q.BlockingBGet(context.Background()) }()
	}
	wg.Wait()
}
