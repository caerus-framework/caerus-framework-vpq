# caerus-framework-vpq

[![CI](https://github.com/caerus-framework/caerus-framework-vpq/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-vpq/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-vpq/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-vpq)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)


Caerus Framework Valkey Priority Queue Component. A weighted priority queue
backed by [valkey-go](https://github.com/valkey-io/valkey-go), sharing the
connection of the `caerus-framework-valkey` component. The weight of an item is
the number of times it was added since it last left the queue; consumers always
pop the highest-weighted item first.

**Not a general job queue** (no DLQ/cron/dashboard out of the box). For retries,
DLQ, scheduling, or UIs use River/asynq/NATS (or similar), not this component.

## Wiring

Chassis `valkey` is usually declared in `main`. Product queues (interest heat,
orders, …) are typically **app-owned**: construct them in the app’s `New`, wire
`WithHandler` there, and expose them via `cf.Subcomponents` so the framework
registers them (see
[`caerus-framework-demoapp`](https://github.com/caerus-framework/caerus-framework-demoapp)).
Sibling `fw.AddComponent(queue)` in `main` still works for simple binaries:

```go
fw := cf.New()

logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
fw.AddComponent(logs) // "logs" is a required dependency

valkey := cf_valkey.New(cf_valkey.WithAddress("127.0.0.1:6379"))
queue := cf_vpq.New(
	cf_vpq.WithQueueName("orders"),
	cf_vpq.WithHandler(func(item *cf_vpq.BGetObject) error {
		return processOrder(item.ObjectID, item.ObjectValue)
	}),
)
fw.AddComponent(valkey)
fw.AddComponent(queue) // GetDependencies() -> [valkey logs]

if err := fw.Run(context.Background()); err != nil {
	log.Fatal(err)
}
```

The queue is a `cf.Runnable`: when a handler is set, `Run` consumes items until
the framework shuts down. A failed handler requeues the item (weight +1).
With a handler, `Run` also recovers abandoned in-flight items every 30s by
default (`WithRecoverInterval(0)` to disable).

## Usage

Manual producer/consumer:

```go
queue := cf.MustGet[*cf_vpq.PriorityQueue](fw)

queue.Add(ctx, "order-1", `{"amount": 42}`) // returns (true, nil); weight 1
queue.Add(ctx, "order-1", `{"amount": 42}`) // returns (false, nil); weight 2, payload kept

n, _ := queue.Count(ctx) // 1 (distinct ids)

item, err := queue.BlockingBGet(ctx) // pops highest weight, blocks up to 1s
if item == nil { /* timeout, nothing to pop */ }
if err := process(item); err != nil {
	queue.Requeue(ctx, item.ObjectID) // back to the queue, weight +1
} else {
	queue.Ack(ctx, item.ObjectID) // drop deadlock tracking + payload
}
```

`Add` returns whether the payload was newly stored: `false` means the id is
already queued and the existing payload is kept (this is the "add more weight"
semantic — it does not overwrite).

## Options

| Option | Description |
| --- | --- |
| `WithConfig(PQConfig)` | static queue config snapshot; non-zero fields override option-set defaults |
| `WithConfigSource(name, path, …)` | bind a configuration source for Init + `OnConfigReload` tunables; the module registers the `Source[PQConfig]` itself (declares `configuration` dep) |
| `WithQueueName(name)` | queue name (required); part of the pub/sub channel and key namespace |
| `WithKeyPrefix(prefix)` | key namespace prefix (default `""` → `squeue:<queue>:<id>`, `zqueue:<queue>`, `pqdeadlocks:<queue>`; the pub/sub channel is `prefix + queue`) |
| `WithBlockDuration(d)` | blocking pop wait (default `1s`) |
| `WithPublishWatermarkDelay(d)` | min interval between pub/sub notifications on Add (default `0` = off; consumers then poll) |
| `WithCacheTimeout(d)` | max queue residence time (default `0` = unlimited); purges zqueue+payload together (no payload-only EXPIRE) |
| `WithPollInterval(d)` | auto-consumer fallback poll interval (default `1s`) |
| `WithHandler(Handler)` | auto-consumer callback used by `Run`; enables default 30s recover ticker and default Health thresholds |
| `WithWorkers(n)` | concurrent auto-consumer goroutines (default `1`); handlers must be concurrency-safe when `n > 1` |
| `WithRecoverInterval(d)` | how often `Run` calls `RecoverDeadlocked` (default `30s` with handler; `0` = off) |
| `WithRecoverMaxAge(d)` | minimum in-flight age before recovery (default `5m`; `0` = recover all in-flight) |
| `WithMaxDepth(n)` | fail `Health` when queued ids exceed `n`; with handler default `10000`; explicit `0` = off |
| `WithMaxInFlight(n)` | fail `Health` when unacked pops exceed `n`; with handler default `max(64, workers*16)`; explicit `0` = off |
| `WithName(name)` | custom component name for multiple instances (default `"vpq"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |

## Multiple instances

Use `WithName` to run multiple VPQ queues in the same process (e.g., email and billing):

```go
email := cf_vpq.New(
    cf_vpq.WithName("email-queue"),
    cf_vpq.WithQueueName("email"),
    cf_vpq.WithHandler(func(item *cf_vpq.BGetObject) error {
        return sendEmail(item.ObjectID, item.ObjectValue)
    }),
)
billing := cf_vpq.New(
    cf_vpq.WithName("billing-queue"),
    cf_vpq.WithQueueName("billing"),
    cf_vpq.WithHandler(func(item *cf_vpq.BGetObject) error {
        return processInvoice(item.ObjectID, item.ObjectValue)
    }),
)

fw.AddComponent(email)
fw.AddComponent(billing)

// Retrieve by name
emailQueue := cf.MustGetByName[*cf_vpq.PriorityQueue](fw, "email-queue")
billingQueue := cf.MustGetByName[*cf_vpq.PriorityQueue](fw, "billing-queue")
```

When multiple instances exist, `cf.Get[*cf_vpq.PriorityQueue](fw)` returns `false` to prevent ambiguous lookups. Always use `GetByName` for named instances.

## Configuration

Same approach as `caerus-framework-valkey`: load `PQConfig` through the
configuration component and pass it via `WithConfig`. Durations are in seconds:

```yaml
# config.yaml
queue_name: orders
key_prefix: prod:
block_duration_sec: 5
publish_watermark_delay_sec: 1
poll_interval_sec: 1
```

## Data model

Every multi-key mutating path is an atomic Lua script (EVALSHA with EVAL
fallback).

- Payload: `SET NX` on `squeue:<queue>:<id>` (kept on duplicate Add; **never**
  Redis-`EXPIRE`'d — see CacheTimeout below).
- Priority: `ZINCRBY` on `zqueue:<queue>` per Add.
- Residence index: optional `zexpiry:<queue>` (`member → expire-at`) when
  `CacheTimeout > 0`. Purge deletes zqueue member + payload together.
- Wake list: `pqwake:<queue>` (`LPUSH`/`LTRIM` on Add/Requeue/Recover;
  `BRPOP` in `BlockingBGet`) so waiting is non-destructive.
- Claim: one Lua script does `ZPOPMAX` + expiry clear + payload `GET` +
  `ZADD pqdeadlocks` — no pop→track crash window.
- Deadlock tracking: `pqdeadlocks:<queue>` stores `member → pop timestamp`.
  Cleared by `Ack` or `Requeue` (back to `zqueue`, weight +1, payload kept).

### CacheTimeout (queue residence)

When set, items may sit in the queue for at most that duration. Expiry is
enforced by `PurgeExpired` (also from recover ticks / before claim), which
removes the zset member and payload in one script. This replaces the old
payload-`EXPIRE` model that could leave ghost queue members.

### Corrupt / orphan members

A zqueue member with a missing payload (manual corruption) is dropped on claim
and logged — not requeued (avoids an infinite loop). Normal CacheTimeout expiry
never takes this path.

### Crashed consumers

A consumer that dies between claim and Ack/Requeue leaves its item in the
deadlock set (already tracked atomically at claim). With a handler, `Run`
recovers those automatically every 30s (items older than `WithRecoverMaxAge`,
default 5m). Pure producers leave recovery off unless you set
`WithRecoverInterval` or call `RecoverDeadlocked` yourself.

Ops checklist:

1. Prefer `WithHandler` consumers (recover-by-default) or an explicit recoverer.
2. Set `CacheTimeout` only when queued items should expire as a whole.
3. Multiple queues: `WithName` + distinct `WithQueueName`; look up via `GetByName`.
4. Handlers should be idempotent (at-least-once after recover).

## Configuration reload

The module is **self-sufficient**: `WithConfigSource(name, path)` registers its
own `Source[PQConfig]` with the configuration component (via
`cf.ConfigSourceRegistrar`, run by the framework during argv absorption). The
default `EnvPrefix` is the uppercase source name (`"vpq"` → `"VPQ_"`); override
with `WithSourceEnvPrefix`. `main` only points the instance at where config
lives:

```go
queue := cf_vpq.New(
	cf_vpq.WithConfigSource("vpq", "vpq.yaml"),
	cf_vpq.WithHandler(handler),
)
```

For low-level control, register the source manually instead:

```go
_ = cf_configuration.AddSource(conf, cf_configuration.Source[cf_vpq.PQConfig]{
	Name:   "vpq",
	Path:   "vpq.yaml",
	Format: cf_configuration.FormatYAML,
	Owner:  queue.Name(),
})
queue := cf_vpq.New(
	cf_vpq.WithConfigSource("vpq", ""), // bind by name only
	cf_vpq.WithHandler(handler),
)
```

`OnConfigReload` re-applies tunables (poll/block/cache/recover/health thresholds).
Queue name and key prefix stay fixed after Init. Valkey credential rotation is
handled by the valkey component’s own `WithConfigSource`.

## Observability

`PriorityQueue` implements `cf.HealthProvider`: `Health(ctx)` pings the backing
valkey server and enforces `WithMaxDepth` / `WithMaxInFlight` when non-zero so
`/readyz` can fail on backlog. With `WithHandler`, those thresholds default on
(see options table); producers without a handler leave them off unless set.
`WithWorkers(n)` runs `n` concurrent consumers (claim is atomic; make handlers
safe for concurrent use). Before `Init` or after `Shutdown` Health is unhealthy.

`cf.MetricsProvider` samples (lazy pickup when uninitialized). All samples carry
`queue` and `component` (= Name()) labels; counters are always emitted while
initialized (zero until first fire):

| Metric | Meaning |
| --- | --- |
| `vpq_info` | queue present (`queue`, `component` labels) |
| `vpq_depth` | distinct ids in the zset |
| `vpq_in_flight` | popped but unacked |
| `vpq_recoveries_total` | cumulative recoveries |

## Tests

Unit tests cover the contract without a server. Integration tests (queue
mechanics, auto-consumer, requeue-on-failure, ghost handling, deadlock
recovery) are gated on `VALKEY_ADDR`:

```
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
