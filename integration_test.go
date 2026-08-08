package cf_vpq

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
)

// newFramework returns a framework with the logs component registered (a
// required dependency of both valkey and vpq).
func newFramework(t *testing.T) *cf.CaerusFramework {
	t.Helper()
	fw := cf.New()
	addComponent(t, fw, cf_logs.New(cf_logs.WithWriter(io.Discard)))
	return fw
}

func mustAdd(t *testing.T, ctx context.Context, q *PriorityQueue, id, value string) {
	t.Helper()
	if _, err := q.Add(ctx, id, value); err != nil {
		t.Fatalf("Add %s: %v", id, err)
	}
}

func addComponent(t *testing.T, fw *cf.CaerusFramework, c cf.CaerusComponent) {
	t.Helper()
	if err := fw.AddComponent(c); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
}

func mustPop(t *testing.T, ctx context.Context, q *PriorityQueue) *BGetObject {
	t.Helper()
	for i := 0; i < 20; i++ {
		item, err := q.BlockingBGet(ctx)
		if err != nil {
			t.Fatalf("BlockingBGet: %v", err)
		}
		if item != nil {
			return item
		}
	}
	t.Fatal("BlockingBGet returned nil repeatedly")
	return nil
}

func rawClient(t *testing.T, fw *cf.CaerusFramework) valkey.Client {
	t.Helper()
	vk, ok := fw.Component(cf_valkey.ComponentName)
	if !ok {
		t.Fatal("valkey component not found")
	}
	return vk.(*cf_valkey.CFValkey).Client()
}

// flushDB wipes the test server so repeated runs are idempotent (popped-but-
// unacked payloads from a previous run would otherwise leak into assertions).
func flushDB(t *testing.T, fw *cf.CaerusFramework) {
	t.Helper()
	raw := rawClient(t, fw)
	if err := raw.Do(context.Background(), raw.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("Flushdb: %v", err)
	}
}

func TestIntegrationAdded(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	q := New(WithQueueName("at"), WithKeyPrefix("test:"))
	if err := q.Init(context.Background(), cf.New()); err == nil {
		t.Fatal("Init without a valkey component should fail")
	}
	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	addComponent(t, fw, q)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := q.Add(ctx, "id1", "v1")
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !added {
		t.Fatal("first Add should report added=true")
	}
	added, err = q.Add(ctx, "id1", "v2-should-be-ignored")
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if added {
		t.Fatal("second Add should report added=false (already queued)")
	}
	p := mustPop(t, ctx, q)
	if p.ObjectValue != "v1" {
		t.Fatalf("payload = %q, want v1 (duplicate Add must not overwrite)", p.ObjectValue)
	}
}

func TestIntegrationGhostHandling(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	vk := cf_valkey.New(cf_valkey.WithAddress(addr))
	addComponent(t, fw, vk)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Corrupt zqueue member without payload: orphan-dropped (not requeued).
	rq := New(WithQueueName("ghost-r"), WithKeyPrefix("test:"), WithBlockDuration(time.Second))
	if err := rq.Init(ctx, fw); err != nil {
		t.Fatalf("rq Init: %v", err)
	}
	t.Cleanup(func() { _ = rq.Shutdown(context.Background()) })
	raw := rawClient(t, fw)
	_ = raw.Do(ctx, raw.B().Zincrby().Key("test:zqueue:ghost-r").Increment(1).Member("zombie").Build())
	item, err := rq.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("BlockingBGet: %v", err)
	}
	if item != nil {
		t.Fatalf("orphan pop = %+v, want nil", item)
	}
	if n, _ := rq.Count(ctx); n != 0 {
		t.Fatalf("Count after orphan drop = %d, want 0", n)
	}

	// CacheTimeout: coordinated purge removes zqueue + payload together.
	dq := New(WithQueueName("ghost-d"), WithKeyPrefix("test:"), WithCacheTimeout(time.Second), WithBlockDuration(time.Second))
	if err := dq.Init(ctx, fw); err != nil {
		t.Fatalf("dq Init: %v", err)
	}
	t.Cleanup(func() { _ = dq.Shutdown(context.Background()) })
	if _, err := dq.Add(ctx, "volatile", "v"); err != nil {
		t.Fatalf("dq Add: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	n, err := dq.PurgeExpired(ctx)
	if err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
	if n != 1 {
		t.Fatalf("PurgeExpired = %d, want 1", n)
	}
	if cnt, _ := dq.Count(ctx); cnt != 0 {
		t.Fatalf("Count after purge = %d, want 0", cnt)
	}
	// Payload must not outlive the queue member (no Redis EXPIRE ghosts).
	exists, err := raw.Do(ctx, raw.B().Exists().Key("test:squeue:ghost-d:volatile").Build()).AsInt64()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if exists != 0 {
		t.Fatal("payload key should be deleted with purge")
	}
}

// TestIntegrationAtomicClaimKeepsInFlightOnPop verifies claim records deadlock
// tracking in the same script as ZPOPMAX (no pop→track gap).
func TestIntegrationAtomicClaimKeepsInFlightOnPop(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx := context.Background()
	q := New(WithQueueName("atomic"), WithKeyPrefix("test:"))
	if err := q.Init(ctx, fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = q.Shutdown(context.Background()) })

	mustAdd(t, ctx, q, "id1", "v1")
	item, err := q.claim(ctx)
	if err != nil || item == nil {
		t.Fatalf("claim: item=%v err=%v", item, err)
	}
	if cnt, _ := q.Count(ctx); cnt != 0 {
		t.Fatalf("Count after claim = %d, want 0", cnt)
	}
	inflight, err := q.Deadlocked(ctx)
	if err != nil {
		t.Fatalf("Deadlocked: %v", err)
	}
	if len(inflight) != 1 || inflight[0].ObjectID != "id1" {
		t.Fatalf("Deadlocked = %+v, want [id1]", inflight)
	}
	_ = q.Ack(ctx, "id1")
}

func TestIntegrationDeadlockRecovery(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	vk := cf_valkey.New(cf_valkey.WithAddress(addr))
	addComponent(t, fw, vk)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	q := New(WithQueueName("dtest"), WithKeyPrefix("test:"))
	if err := q.Init(ctx, fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = q.Shutdown(context.Background()) })

	mustAdd(t, ctx, q, "stuck", "v")
	p := mustPop(t, ctx, q)
	if p.ObjectID != "stuck" {
		t.Fatalf("popped %q, want stuck", p.ObjectID)
	}
	// simulate a crashed consumer: no Ack/Requeue

	inflight, err := q.Deadlocked(ctx)
	if err != nil {
		t.Fatalf("Deadlocked: %v", err)
	}
	if len(inflight) != 1 || inflight[0].ObjectID != "stuck" {
		t.Fatalf("Deadlocked() = %+v, want [stuck]", inflight)
	}
	if inflight[0].PoppedAt.IsZero() || time.Since(inflight[0].PoppedAt) > time.Minute {
		t.Fatalf("Deadlocked() PoppedAt not recent: %v", inflight[0].PoppedAt)
	}

	n, err := q.RecoverDeadlocked(ctx, 0)
	if err != nil {
		t.Fatalf("RecoverDeadlocked: %v", err)
	}
	if n != 1 {
		t.Fatalf("RecoverDeadlocked returned %d, want 1", n)
	}
	if q.recoveriesTotal.Load() != 1 {
		t.Fatalf("recoveriesTotal = %d, want 1", q.recoveriesTotal.Load())
	}
	if cnt, _ := q.Count(ctx); cnt != 1 {
		t.Fatalf("Count after recovery = %d, want 1", cnt)
	}
	if inflight, _ := q.Deadlocked(ctx); len(inflight) != 0 {
		t.Fatalf("Deadlocked() after recovery = %+v, want empty", inflight)
	}
	p = mustPop(t, ctx, q)
	if p.ObjectID != "stuck" || p.ObjectValue != "v" {
		t.Fatalf("recovered item = %+v, want stuck/v", p)
	}
	if err := q.Ack(ctx, p.ObjectID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestIntegrationRecoverLoop verifies Run's recover ticker requeues abandoned
// in-flight items without a handler (dedicated recoverer).
func TestIntegrationRecoverLoop(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := New(
		WithQueueName("rloop"),
		WithKeyPrefix("test:"),
		WithRecoverInterval(100*time.Millisecond),
		WithRecoverMaxAge(0),
	)
	if err := q.Init(ctx, fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = q.Shutdown(context.Background()) })

	mustAdd(t, ctx, q, "abandoned", "v")
	_ = mustPop(t, ctx, q)

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	done := make(chan error, 1)
	go func() { done <- q.Run(runCtx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := q.Count(ctx); n == 1 && q.recoveriesTotal.Load() >= 1 {
			runCancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not exit after cancel")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	runCancel()
	t.Fatal("recover loop did not requeue abandoned item")
}

// TestIntegrationHealthThresholds verifies MaxDepth / MaxInFlight readiness gates.
func TestIntegrationHealthThresholds(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx := context.Background()
	q := New(WithQueueName("hthr"), WithKeyPrefix("test:"), WithMaxDepth(1))
	if err := q.Init(ctx, fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = q.Shutdown(context.Background()) })

	mustAdd(t, ctx, q, "a", "1")
	if err := q.Health(ctx); err != nil {
		t.Fatalf("Health at depth 1 = %v, want nil", err)
	}
	mustAdd(t, ctx, q, "b", "2")
	if err := q.Health(ctx); err == nil {
		t.Fatal("Health at depth 2 with MaxDepth=1 should fail")
	}

	q2 := New(WithQueueName("hinf"), WithKeyPrefix("test:"), WithMaxInFlight(1))
	if err := q2.Init(ctx, fw); err != nil {
		t.Fatalf("Init q2: %v", err)
	}
	t.Cleanup(func() { _ = q2.Shutdown(context.Background()) })
	mustAdd(t, ctx, q2, "x", "1")
	_ = mustPop(t, ctx, q2)
	if err := q2.Health(ctx); err != nil {
		t.Fatalf("Health with 1 in-flight = %v, want nil", err)
	}
	mustAdd(t, ctx, q2, "y", "2")
	_ = mustPop(t, ctx, q2)
	if err := q2.Health(ctx); err == nil {
		t.Fatal("Health with 2 in-flight and MaxInFlight=1 should fail")
	}
}

// TestIntegration is gated on the VALKEY_ADDR environment variable so the
// regular test run has no external dependency. Point it at a live server:
//
//	VALKEY_ADDR=127.0.0.1:6379 go test ./...
func TestIntegration(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)

	vk := cf_valkey.New(cf_valkey.WithAddress(addr))
	q := New(WithQueueName("itest"), WithKeyPrefix("test:"))
	addComponent(t, fw, vk)
	addComponent(t, fw, q)

	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// empty queue: blocking pop waits and returns nil, nil
	item, err := q.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("BlockingBGet on empty queue: %v", err)
	}
	if item != nil {
		t.Fatalf("BlockingBGet on empty queue = %+v, want nil", item)
	}

	// add three items with different weights
	mustAdd(t, ctx, q, "a", "payload-a")
	mustAdd(t, ctx, q, "b", "payload-b")
	mustAdd(t, ctx, q, "a", "ignored-duplicate")
	mustAdd(t, ctx, q, "c", "payload-c")
	mustAdd(t, ctx, q, "a", "again")

	n, err := q.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Fatalf("Count = %d, want 3 (distinct ids)", n)
	}

	// highest weight first (a was added 3 times)
	popped := mustPop(t, ctx, q)
	if popped.ObjectID != "a" || popped.ObjectValue != "payload-a" {
		t.Fatalf("first pop = %+v, want a/payload-a (weight 3)", popped)
	}
	if popped.ObjectScore != 3 {
		t.Fatalf("first pop score = %v, want 3", popped.ObjectScore)
	}

	// requeue on failure path: item returns to the queue, payload intact
	if err := q.Requeue(ctx, "a"); err != nil {
		t.Fatalf("Requeue: %v", err)
	}

	// consume remaining
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		p := mustPop(t, ctx, q)
		got[p.ObjectID] = true
		if err := q.Ack(ctx, p.ObjectID); err != nil {
			t.Fatalf("Ack %s: %v", p.ObjectID, err)
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		if !got[id] {
			t.Fatalf("missing %s in remaining pops: %v", id, got)
		}
	}
}

func TestIntegrationConsume(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)

	vk := cf_valkey.New(cf_valkey.WithAddress(addr))
	processed := make(chan string, 10)
	q := New(
		WithQueueName("ctest"),
		WithKeyPrefix("test:"),
		WithHandler(func(item *BGetObject) error {
			processed <- item.ObjectID
			return nil
		}),
	)
	addComponent(t, fw, vk)
	addComponent(t, fw, q)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- q.Run(runCtx) }()
	t.Cleanup(stopRun)

	for i := 0; i < 3; i++ {
		if _, err := q.Add(ctx, "id"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		select {
		case id := <-processed:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not process item in time")
		}
	}
	for _, id := range []string{"ida", "idb", "idc"} {
		if !seen[id] {
			t.Fatalf("consumer did not process %s (got %v)", id, seen)
		}
	}

	stopRun()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop on ctx cancel")
	}

	// handler error path: item is requeued
	var mu sync.Mutex
	attempts := 0
	q2 := New(
		WithQueueName("etest"),
		WithKeyPrefix("test:"),
		WithHandler(func(item *BGetObject) error {
			mu.Lock()
			attempts++
			fail := attempts == 1
			mu.Unlock()
			if fail {
				return errors.New("boom")
			}
			return nil
		}),
	)
	if err := q2.Init(ctx, fw); err != nil {
		t.Fatalf("q2 Init: %v", err)
	}
	t.Cleanup(func() { _ = q2.Shutdown(context.Background()) })

	if _, err := q2.Add(ctx, "fail-item", "v"); err != nil {
		t.Fatalf("q2 Add: %v", err)
	}
	runCtx2, stopRun2 := context.WithCancel(context.Background())
	runDone2 := make(chan error, 1)
	go func() { runDone2 <- q2.Run(runCtx2) }()
	t.Cleanup(stopRun2)

	// first attempt fails (requeue), second succeeds
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := attempts
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("requeue cycle did not complete (handler ran %d times)", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	if attempts < 2 {
		t.Fatalf("handler ran %d times, want >= 2 (requeued once)", attempts)
	}
	mu.Unlock()
	stopRun2()
	select {
	case <-runDone2:
	case <-time.After(3 * time.Second):
		t.Fatal("q2 Run did not stop")
	}
	n, _ := q2.Count(ctx)
	if n != 0 {
		t.Fatalf("q2 Count = %d, want 0 after successful processing", n)
	}
}

func TestIntegrationLoggerRedelivery(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	logs, ok := cf.Get[*cf_logs.Logs](fw)
	if !ok {
		t.Fatal("logs component not found")
	}
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	q := New(WithQueueName("lgr"), WithKeyPrefix("test:"))
	addComponent(t, fw, q)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	before := q.logger
	if before == nil || q.logsSub == nil {
		t.Fatal("Init should subscribe to the framework logs component")
	}
	if before == logs.Logger() {
		t.Fatal("component logger must be OnReconfigureFor-scoped, not the process-global Logger()")
	}

	// rebuilding the logs component re-delivers a new scoped logger
	logs.Reconfigure(cf_logs.WithFormat(cf_logs.FormatJSON))
	if q.logger == before {
		t.Fatal("queue should receive a rebuilt logger on Reconfigure")
	}
	if q.logger == logs.Logger() {
		t.Fatal("rebuilt logger must remain OnReconfigureFor-scoped")
	}
}

// TestIntegrationHealthReflectsConnectivity verifies Health reports nil while
// connected and errors after Shutdown.
func TestIntegrationHealthReflectsConnectivity(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))
	q := New(WithQueueName("hlt"), WithKeyPrefix("test:"))
	addComponent(t, fw, q)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := q.Health(context.Background()); err != nil {
		t.Fatalf("Health while connected = %v, want nil", err)
	}
	ms := q.Metrics()
	byName := map[string]cf_observability.Metric{}
	for _, m := range ms {
		byName[m.Name] = m
	}
	for _, name := range []string{"vpq_info", "vpq_depth", "vpq_in_flight", "vpq_recoveries_total"} {
		if _, ok := byName[name]; !ok {
			t.Fatalf("Metrics while connected = %+v, missing %q", ms, name)
		}
	}
	if byName["vpq_info"].Value != 1 {
		t.Fatalf("vpq_info = %v, want 1", byName["vpq_info"].Value)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := q.Health(context.Background()); err == nil {
		t.Fatal("Health after Shutdown should fail")
	}
	if ms := q.Metrics(); ms != nil {
		t.Fatalf("Metrics after Shutdown = %+v, want nil", ms)
	}
}

// TestIntegrationMultipleNamedInstances demonstrates multiple VPQ instances
// in the same process using WithName and GetByName.
func TestIntegrationMultipleNamedInstances(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	fw := newFramework(t)
	addComponent(t, fw, cf_valkey.New(cf_valkey.WithAddress(addr)))

	email := New(WithQueueName("email"), WithName("email-queue"), WithKeyPrefix("test:"))
	billing := New(WithQueueName("billing"), WithName("billing-queue"), WithKeyPrefix("test:"))

	addComponent(t, fw, email)
	addComponent(t, fw, billing)

	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	// Get by name retrieves the correct instance
	emailGot, ok := cf.GetByName[*PriorityQueue](fw, "email-queue")
	if !ok || emailGot != email {
		t.Fatalf("GetByName(email-queue) returned wrong component: %v, %v", emailGot, ok)
	}
	billingGot, ok := cf.GetByName[*PriorityQueue](fw, "billing-queue")
	if !ok || billingGot != billing {
		t.Fatalf("GetByName(billing-queue) returned wrong component: %v, %v", billingGot, ok)
	}

	// Get returns false when multiple instances exist
	if _, ok := cf.Get[*PriorityQueue](fw); ok {
		t.Fatal("Get should return false when multiple VPQ instances exist")
	}

	// Both instances work independently
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Add to email queue
	if _, err := email.Add(ctx, "email-1", "welcome"); err != nil {
		t.Fatalf("email Add: %v", err)
	}

	// Add to billing queue
	if _, err := billing.Add(ctx, "billing-1", "invoice"); err != nil {
		t.Fatalf("billing Add: %v", err)
	}

	// Pop from email queue
	emailItem, err := email.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("email BlockingBGet: %v", err)
	}
	if emailItem == nil || emailItem.ObjectID != "email-1" || emailItem.ObjectValue != "welcome" {
		t.Fatalf("email item = %+v, want email-1/welcome", emailItem)
	}
	if err := email.Ack(ctx, emailItem.ObjectID); err != nil {
		t.Fatalf("email Ack: %v", err)
	}

	// Pop from billing queue
	billingItem, err := billing.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("billing BlockingBGet: %v", err)
	}
	if billingItem == nil || billingItem.ObjectID != "billing-1" || billingItem.ObjectValue != "invoice" {
		t.Fatalf("billing item = %+v, want billing-1/invoice", billingItem)
	}
	if err := billing.Ack(ctx, billingItem.ObjectID); err != nil {
		t.Fatalf("billing Ack: %v", err)
	}
}

// TestIntegrationConfigReload verifies that VPQ picks up the new valkey client
// after a config reload. The valkey component swaps its client on reload; VPQ
// must use the fresh client via getClient() rather than caching it at Init.
func TestIntegrationConfigReload(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	// Set up framework with configuration, valkey (with config source), and VPQ
	fw := newFramework(t)
	conf := cf_configuration.New()
	addComponent(t, fw, conf)
	vk := cf_valkey.New(cf_valkey.WithConfigSource("valkey", ""))
	addComponent(t, fw, vk)
	q := New(WithQueueName("reload-test"), WithKeyPrefix("test:"))
	addComponent(t, fw, q)

	// Initialize - configuration will load valkey config from env/source
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })
	flushDB(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify VPQ works before reload
	if _, err := q.Add(ctx, "before-reload", "value1"); err != nil {
		t.Fatalf("Add before reload: %v", err)
	}
	item, err := q.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("BlockingBGet before reload: %v", err)
	}
	if item == nil || item.ObjectID != "before-reload" {
		t.Fatalf("item before reload = %+v, want before-reload", item)
	}
	if err := q.Ack(ctx, item.ObjectID); err != nil {
		t.Fatalf("Ack before reload: %v", err)
	}

	// Trigger valkey config reload - this swaps the client
	vk.OnConfigReload("valkey", nil)

	// Give a moment for the reload to complete
	time.Sleep(100 * time.Millisecond)

	// Verify VPQ still works after reload (proves it uses the new client)
	if _, err := q.Add(ctx, "after-reload", "value2"); err != nil {
		t.Fatalf("Add after reload: %v", err)
	}
	item, err = q.BlockingBGet(ctx)
	if err != nil {
		t.Fatalf("BlockingBGet after reload: %v", err)
	}
	if item == nil || item.ObjectID != "after-reload" {
		t.Fatalf("item after reload = %+v, want after-reload", item)
	}
	if err := q.Ack(ctx, item.ObjectID); err != nil {
		t.Fatalf("Ack after reload: %v", err)
	}
}
