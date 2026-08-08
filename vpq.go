package cf_vpq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
)

const (
	// ComponentName is the framework component name for the valkey priority
	// queue component.
	ComponentName = "vpq"

	// ComponentStage is the stage the queue initializes in. It is not a
	// built-in bootstrap stage; AddComponent registers it automatically the
	// first time a component declares it.
	ComponentStage = cf.Stage("data")

	defaultRecoverInterval = 30 * time.Second
	defaultRecoverMaxAge   = 5 * time.Minute
	defaultMaxDepth        = int64(10000)
)

// defaultMaxInFlight returns the Health in-flight ceiling when WithHandler is
// set and WithMaxInFlight was not configured: max(64, workers*16).
func defaultMaxInFlight(workers int) int64 {
	v := int64(workers) * 16
	if v < 64 {
		return 64
	}
	return v
}

// ErrClosed is returned by queue operations after Shutdown or before Init.
var ErrClosed = errors.New("cf_vpq: queue is not initialized or is shut down")

// Lua scripts keep the multi-key queue operations atomic, so a partial failure
// can never leave an orphan payload or a stranded queue member. Claim
// (ZPOPMAX + deadlock track + payload read) is a single script so a crash
// cannot lose an item between pop and track. CacheTimeout uses an expiry
// zset (not EXPIRE on the payload) so queue member and payload are removed
// together.
var (
	// scriptAdd: store payload (SET NX, no Redis EXPIRE), increment priority,
	// optionally refresh queue-residence expiry, and poke the wake list.
	// KEYS: payload, zqueue, zexpiry, wake
	// ARGV: id, value, ttlSec, nowUnix
	// Returns 1 if the payload was newly stored, 0 if already queued.
	scriptAdd = valkey.NewLuaScript(`
local added = redis.call('SET', KEYS[1], ARGV[2], 'NX')
redis.call('ZINCRBY', KEYS[2], 1, ARGV[1])
local ttl = tonumber(ARGV[3])
if ttl > 0 then
	redis.call('ZADD', KEYS[3], tonumber(ARGV[4]) + ttl, ARGV[1])
end
redis.call('LPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], 0, 0)
if added then return 1 else return 0 end`)

	// scriptAck: drop deadlock tracking, delete payload, clear queue/expiry.
	// KEYS: deadlock, payload, zqueue, zexpiry
	scriptAck = valkey.NewLuaScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1`)

	// scriptRequeue: return an item to the queue with weight +1, drop deadlock
	// tracking, refresh residence expiry when ttl > 0, poke wake.
	// KEYS: zqueue, deadlock, zexpiry, wake
	// ARGV: id, ttlSec, nowUnix
	scriptRequeue = valkey.NewLuaScript(`
redis.call('ZINCRBY', KEYS[1], 1, ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
local ttl = tonumber(ARGV[2])
if ttl > 0 then
	redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + ttl, ARGV[1])
else
	redis.call('ZREM', KEYS[3], ARGV[1])
end
redis.call('LPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], 0, 0)
return 1`)

	// scriptClaim: atomic non-blocking pop. ZPOPMAX + remove expiry + GET
	// payload + ZADD deadlock. Missing payload (corrupt) is not requeued.
	// KEYS: zqueue, deadlock, zexpiry
	// ARGV: nowUnix, payloadKeyPrefix
	// Returns nil if empty; {1, id, score, value} on success;
	// {0, id, score} when payload missing (orphan dropped).
	scriptClaim = valkey.NewLuaScript(`
local popped = redis.call('ZPOPMAX', KEYS[1], 1)
if #popped == 0 then
	return nil
end
local id = popped[1]
local score = tonumber(popped[2]) + 0.0
-- +0.0 forces RESP3 double; integer weights otherwise decode as int64 and
-- ValkeyMessage.ToFloat64 fails on the Go side.
redis.call('ZREM', KEYS[3], id)
local v = redis.call('GET', ARGV[2] .. id)
if not v then
	return {0, id, score}
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), id)
return {1, id, score, v}`)

	// scriptPurgeExpired: remove queued items past CacheTimeout residence.
	// Deletes zqueue member + payload + expiry entry together (never leaves
	// a ghost). In-flight items are not touched (already absent from zqueue).
	// KEYS: zexpiry, zqueue
	// ARGV: nowUnix, payloadKeyPrefix
	scriptPurgeExpired = valkey.NewLuaScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local n = 0
for _, id in ipairs(ids) do
	if redis.call('ZSCORE', KEYS[2], id) then
		redis.call('ZREM', KEYS[2], id)
		redis.call('DEL', ARGV[2] .. id)
		n = n + 1
	end
	redis.call('ZREM', KEYS[1], id)
end
return n`)

	// scriptRecover: requeue deadlock entries older than the cutoff.
	// KEYS: deadlock, zqueue, zexpiry, wake
	// ARGV: cutoffUnix, ttlSec, nowUnix
	scriptRecover = valkey.NewLuaScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
for _, id in ipairs(ids) do
	redis.call('ZINCRBY', KEYS[2], 1, id)
	redis.call('ZREM', KEYS[1], id)
	if ttl > 0 then
		redis.call('ZADD', KEYS[3], now + ttl, id)
	end
end
if #ids > 0 then
	redis.call('LPUSH', KEYS[4], '1')
	redis.call('LTRIM', KEYS[4], 0, 0)
end
return #ids`)
)

// BGetObject is an item popped from the queue. ObjectScore is the item weight
// (the number of times it was added since it last left the queue).
type BGetObject struct {
	ObjectID    string
	ObjectScore float64
	ObjectValue string
}

// Handler processes one item in the auto-consumer loop. Returning an error
// requeues the item (weight +1) and drops its deadlock tracking.
type Handler func(*BGetObject) error

// PQConfig is the file/env-drivable queue configuration. Load it through the
// configuration component via WithConfigSource (preferred) or WithConfig.
// Durations are in seconds; zero means "unset" (keep option defaults).
type PQConfig struct {
	QueueName             string `json:"queue_name" yaml:"queue_name" env:"QUEUE_NAME"`
	KeyPrefix             string `json:"key_prefix,omitempty" yaml:"key_prefix,omitempty" env:"KEY_PREFIX"`
	BlockDuration         int    `json:"block_duration_sec,omitempty" yaml:"block_duration_sec,omitempty" env:"BLOCK_DURATION_SEC"`
	PublishWatermarkDelay int    `json:"publish_watermark_delay_sec,omitempty" yaml:"publish_watermark_delay_sec,omitempty" env:"PUBLISH_WATERMARK_DELAY_SEC"`
	CacheTimeout          int    `json:"cache_timeout_sec,omitempty" yaml:"cache_timeout_sec,omitempty" env:"CACHE_TIMEOUT_SEC"`
	PollInterval          int    `json:"poll_interval_sec,omitempty" yaml:"poll_interval_sec,omitempty" env:"POLL_INTERVAL_SEC"`
	RecoverInterval       int    `json:"recover_interval_sec,omitempty" yaml:"recover_interval_sec,omitempty" env:"RECOVER_INTERVAL_SEC"`
	RecoverMaxAge         int    `json:"recover_max_age_sec,omitempty" yaml:"recover_max_age_sec,omitempty" env:"RECOVER_MAX_AGE_SEC"`
	MaxDepth              int    `json:"max_depth,omitempty" yaml:"max_depth,omitempty" env:"MAX_DEPTH"`
	MaxInFlight           int    `json:"max_in_flight,omitempty" yaml:"max_in_flight,omitempty" env:"MAX_IN_FLIGHT"`
	Workers               int    `json:"workers,omitempty" yaml:"workers,omitempty" env:"WORKERS"`
}

// Option configures the queue at construction time.
type Option func(*options)

type options struct {
	cfg                PQConfig
	loaded             *PQConfig // set by WithConfig; overrides option-set defaults
	configSource       string
	configPath         string // source file path (module self-registration)
	srcEnvPrefix       string // source env overlay prefix (default: NAME_)
	srcFormat          cf_configuration.Format
	srcFormatSet       bool
	logger             *slog.Logger
	loggerSet          bool // true when WithLogger was called explicitly
	handler            Handler
	name               string // custom component name; empty means use ComponentName
	recoverInterval    time.Duration
	recoverIntervalSet bool
	recoverMaxAge      time.Duration
	recoverMaxAgeSet   bool
	maxDepth           int64
	maxDepthSet        bool
	maxInFlight        int64
	maxInFlightSet     bool
	workers            int
}

// SourceOption configures the self-registered configuration source created by
// WithConfigSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix sets the environment overlay prefix for the source
// (default: the uppercase source name with "-" replaced by "_", plus "_").
// An empty prefix disables env overlay.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the file format instead of inferring it from the
// path extension (".yaml"/".yml" → YAML; anything else JSON).
func WithSourceFormat(f cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) { o.format = f; o.formatSet = true }
}

// defaultSourceEnvPrefix derives an environment prefix from a source name.
func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// WithConfig sets a static queue configuration snapshot. Non-zero fields of
// cfg override the values set by the convenience options. Prefer
// WithConfigSource when using caerus-framework-configuration with hot-reload.
func WithConfig(cfg PQConfig) Option {
	return func(o *options) { o.loaded = &cfg }
}

// WithConfigSource binds this component to a named configuration source and
// registers that source with the configuration component (via the framework's
// ConfigSourceRegistrar pass during argv absorption). The module owns the
// Source: the config type, default EnvPrefix and its Owner (Name(), so named
// instances reload correctly). main only points the instance at where the
// config lives.
//
//	cf_vpq.New(cf_vpq.WithConfigSource("vpq", "config/vpq.json"), ...)
//
// A path of "" registers an env-only (fileless) source when the EnvPrefix is
// non-empty. The path CLI override stays --<source-name> (ParseFlags).
// Declares a dependency on "configuration". Queue identity (name/prefix) is
// applied at Init; reload updates tunables only (durations, recover, health
// thresholds).
func WithConfigSource(name, path string, opts ...SourceOption) Option {
	return func(o *options) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithQueueName sets the queue name. It is the pub/sub channel and the key
// namespace segment; it is required.
func WithQueueName(name string) Option {
	return func(o *options) { o.cfg.QueueName = name }
}

// WithKeyPrefix sets a key namespace prefix (default ""). With the default the
// keys are "squeue:<queue>:<id>", "zqueue:<queue>" and "pqdeadlocks:<queue>".
func WithKeyPrefix(prefix string) Option {
	return func(o *options) { o.cfg.KeyPrefix = prefix }
}

// WithBlockDuration sets how long a blocking pop waits for an item (default
// 1s).
func WithBlockDuration(d time.Duration) Option {
	return func(o *options) { o.cfg.BlockDuration = int(d.Seconds()) }
}

// WithPublishWatermarkDelay sets the minimum interval between pub/sub
// notifications on Add (default 0 = no pub/sub; consumers then rely on the
// poll interval).
func WithPublishWatermarkDelay(d time.Duration) Option {
	return func(o *options) { o.cfg.PublishWatermarkDelay = int(d.Seconds()) }
}

// WithCacheTimeout sets how long an item may remain queued before it is
// discarded (default 0 = no residence limit). Expiry removes the zqueue member
// and payload together; the payload key itself is never Redis-EXPIRE'd, so
// partial "ghost" expiry cannot occur. In-flight items are unaffected.
func WithCacheTimeout(d time.Duration) Option {
	return func(o *options) { o.cfg.CacheTimeout = int(d.Seconds()) }
}

// WithPollInterval sets the fallback consumer poll interval (default 1s).
func WithPollInterval(d time.Duration) Option {
	return func(o *options) { o.cfg.PollInterval = int(d.Seconds()) }
}

// WithHandler sets the auto-consumer callback used by Run. When set,
// deadlock recovery runs every 30s by default (see WithRecoverInterval), and
// Health gets default MaxDepth / MaxInFlight thresholds (see WithMaxDepth /
// WithMaxInFlight). Handlers must be safe for concurrent use when
// WithWorkers(n) has n > 1.
func WithHandler(h Handler) Option {
	return func(o *options) { o.handler = h }
}

// WithWorkers sets how many concurrent auto-consumer goroutines Run/Consume
// use (default 1). Values below 1 are treated as 1. Claim is atomic; each
// worker runs the handler independently — handlers must be concurrency-safe
// when n > 1.
func WithWorkers(n int) Option {
	return func(o *options) { o.workers = n }
}

// WithRecoverInterval sets how often Run calls RecoverDeadlocked.
// With a handler, the default is 30s. Pass 0 to disable. Without a handler,
// recovery stays off unless this sets a positive interval (dedicated recoverer).
func WithRecoverInterval(d time.Duration) Option {
	return func(o *options) {
		o.recoverInterval = d
		o.recoverIntervalSet = true
	}
}

// WithRecoverMaxAge sets the minimum age of in-flight items before
// RecoverDeadlocked requeues them (default 5m). Pass 0 to recover all
// currently in-flight items on each tick.
func WithRecoverMaxAge(d time.Duration) Option {
	return func(o *options) {
		o.recoverMaxAge = d
		o.recoverMaxAgeSet = true
	}
}

// WithMaxDepth fails Health when the queued item count exceeds n.
// With a handler, the default is 10000 when this option is omitted; pass 0 to
// disable the check explicitly.
func WithMaxDepth(n int64) Option {
	return func(o *options) { o.maxDepth = n; o.maxDepthSet = true }
}

// WithMaxInFlight fails Health when popped-but-unacked items exceed n.
// With a handler, the default is max(64, workers*16) when this option is
// omitted; pass 0 to disable the check explicitly.
func WithMaxInFlight(n int64) Option {
	return func(o *options) { o.maxInFlight = n; o.maxInFlightSet = true }
}

// WithLogger overrides the logger used for component diagnostics. By default
// the component logs through the framework logs component (declared in
// GetDependencies); WithLogger is an explicit override for tests and embedded
// use and wins over the framework logger. slog.Default() remains the fallback
// only when neither is available.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger; o.loggerSet = true }
}

// WithName sets a custom component name, allowing multiple VPQ instances
// in the same process. The default name is "vpq" (ComponentName). Use
// this when you need multiple queues (e.g., email and billing) in one binary.
// Retrieve named instances with GetByName[*PriorityQueue](fw, "email").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// PriorityQueue is the caerus-framework-vpq component: a weighted priority
// queue backed by valkey. Add increments the item weight; consumers pop the
// highest-weighted item. It depends on the valkey component for its client.
type PriorityQueue struct {
	mu              sync.RWMutex
	cfg             PQConfig
	baseCfg         PQConfig // option defaults; config source overlays onto this
	configSource    string
	configPath      string // source file path (module self-registration)
	srcEnvPrefix    string
	srcFormat       cf_configuration.Format
	srcFormatSet    bool
	logger          *slog.Logger
	loggerSet       bool
	logsSub         *cf_logs.Subscription
	handler         Handler
	valkey          *cf_valkey.CFValkey
	lastPublish     time.Time
	name            string // custom name; empty means use ComponentName
	fw              *cf.CaerusFramework
	recoverInterval time.Duration
	recoverMaxAge   time.Duration
	maxDepth        int64
	maxDepthSet     bool
	maxInFlight     int64
	maxInFlightSet  bool
	workers         int
	recoveriesTotal atomic.Int64
}

// New creates a priority queue component. It requires a valkey component and a
// queue name at Init.
func New(opts ...Option) *PriorityQueue {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	baseCfg := o.cfg
	if o.loaded != nil {
		overlayConfig(&o.cfg, *o.loaded)
		applyPQConfigOps(&o, *o.loaded)
	}
	if o.cfg.BlockDuration <= 0 {
		o.cfg.BlockDuration = 1
	}
	if o.cfg.PollInterval <= 0 {
		o.cfg.PollInterval = 1
	}
	workers := o.workers
	if workers < 1 {
		workers = 1
	}
	if o.handler != nil && !o.recoverIntervalSet {
		o.recoverInterval = defaultRecoverInterval
	}
	if !o.recoverMaxAgeSet {
		o.recoverMaxAge = defaultRecoverMaxAge
	}
	if o.handler != nil {
		if !o.maxDepthSet {
			o.maxDepth = defaultMaxDepth
		}
		if !o.maxInFlightSet {
			o.maxInFlight = defaultMaxInFlight(workers)
		}
	}
	return &PriorityQueue{
		cfg:             o.cfg,
		baseCfg:         baseCfg,
		configSource:    o.configSource,
		configPath:      o.configPath,
		srcEnvPrefix:    o.srcEnvPrefix,
		srcFormat:       o.srcFormat,
		srcFormatSet:    o.srcFormatSet,
		logger:          o.logger,
		loggerSet:       o.loggerSet,
		handler:         o.handler,
		name:            o.name,
		recoverInterval: o.recoverInterval,
		recoverMaxAge:   o.recoverMaxAge,
		maxDepth:        o.maxDepth,
		maxDepthSet:     o.maxDepthSet,
		maxInFlight:     o.maxInFlight,
		maxInFlightSet:  o.maxInFlightSet,
		workers:         workers,
	}
}

// applyPQConfigOps maps non-zero PQConfig recover/health/worker fields onto options.
func applyPQConfigOps(o *options, loaded PQConfig) {
	if loaded.RecoverInterval > 0 {
		o.recoverInterval = time.Duration(loaded.RecoverInterval) * time.Second
		o.recoverIntervalSet = true
	}
	if loaded.RecoverMaxAge > 0 {
		o.recoverMaxAge = time.Duration(loaded.RecoverMaxAge) * time.Second
		o.recoverMaxAgeSet = true
	}
	if loaded.MaxDepth > 0 {
		o.maxDepth = int64(loaded.MaxDepth)
		o.maxDepthSet = true
	}
	if loaded.MaxInFlight > 0 {
		o.maxInFlight = int64(loaded.MaxInFlight)
		o.maxInFlightSet = true
	}
	if loaded.Workers > 0 {
		o.workers = loaded.Workers
	}
}

// overlayConfig overlays non-zero fields of loaded onto cfg. It runs last, so
// a loaded config always wins over option-set defaults.
func overlayConfig(cfg *PQConfig, loaded PQConfig) {
	if loaded.QueueName != "" {
		cfg.QueueName = loaded.QueueName
	}
	if loaded.KeyPrefix != "" {
		cfg.KeyPrefix = loaded.KeyPrefix
	}
	overlayTunables(cfg, loaded)
}

// overlayTunables overlays non-identity fields (not queue name / key prefix).
func overlayTunables(cfg *PQConfig, loaded PQConfig) {
	if loaded.BlockDuration != 0 {
		cfg.BlockDuration = loaded.BlockDuration
	}
	if loaded.PublishWatermarkDelay != 0 {
		cfg.PublishWatermarkDelay = loaded.PublishWatermarkDelay
	}
	if loaded.CacheTimeout != 0 {
		cfg.CacheTimeout = loaded.CacheTimeout
	}
	if loaded.PollInterval != 0 {
		cfg.PollInterval = loaded.PollInterval
	}
	if loaded.RecoverInterval != 0 {
		cfg.RecoverInterval = loaded.RecoverInterval
	}
	if loaded.RecoverMaxAge != 0 {
		cfg.RecoverMaxAge = loaded.RecoverMaxAge
	}
	if loaded.MaxDepth != 0 {
		cfg.MaxDepth = loaded.MaxDepth
	}
	if loaded.MaxInFlight != 0 {
		cfg.MaxInFlight = loaded.MaxInFlight
	}
	if loaded.Workers != 0 {
		cfg.Workers = loaded.Workers
	}
}

// Name implements cf.CaerusComponent.
// Name implements cf.CaerusComponent. Returns the custom name set via WithName,
// or the default ComponentName ("vpq") if no custom name was set.
func (q *PriorityQueue) Name() string {
	if q.name != "" {
		return q.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (q *PriorityQueue) GetInitOrderStage() cf.Stage { return ComponentStage }

// GetDependencies implements cf.Dependencies. Depends on configuration when
// WithConfigSource is set.
func (q *PriorityQueue) GetDependencies() []string {
	deps := []string{cf_valkey.ComponentName, cf_logs.ComponentName}
	if q.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar. The framework
// calls it during argv absorption; it registers this component's configuration
// source (name, path, env prefix, format and Owner) with the configuration
// component. No-op when no source is bound.
func (q *PriorityQueue) RegisterConfigSources(conf any) error {
	cfg, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_vpq: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if q.configSource == "" {
		return nil
	}
	format := q.srcFormat
	if !q.srcFormatSet {
		if p := strings.ToLower(q.configPath); strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml") {
			format = cf_configuration.FormatYAML
		} else {
			format = cf_configuration.FormatJSON
		}
	}
	return cf_configuration.AddSource(cfg, cf_configuration.Source[PQConfig]{
		Name:      q.configSource,
		Path:      q.configPath,
		Format:    format,
		Owner:     q.Name(),
		EnvPrefix: q.srcEnvPrefix,
	})
}

// Init implements cf.CaerusComponent. It resolves the valkey client and
// validates the queue name, failing fast before the framework starts runners.
func (q *PriorityQueue) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.fw = fw
	if !q.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			q.logsSub = logs.OnReconfigureFor(q.Name(), func(l *slog.Logger) { q.logger = l })
		}
	}
	if q.configSource != "" {
		if err := q.applyConfigSourceLocked(true); err != nil {
			return err
		}
	}
	comp, ok := fw.Component(cf_valkey.ComponentName)
	if !ok {
		return fmt.Errorf("cf_vpq: dependency %q is not registered", cf_valkey.ComponentName)
	}
	vk, ok := comp.(*cf_valkey.CFValkey)
	if !ok {
		return fmt.Errorf("cf_vpq: dependency %q is %T, want *cf_valkey.CFValkey", cf_valkey.ComponentName, comp)
	}
	if vk.Client() == nil {
		return fmt.Errorf("cf_vpq: valkey component is not initialized")
	}
	if q.cfg.QueueName == "" {
		return fmt.Errorf("cf_vpq: queue name is required (WithQueueName or config)")
	}
	q.valkey = vk
	q.logger.Info("cf_vpq: queue ready",
		"queue", q.cfg.QueueName,
		"key_prefix", q.cfg.KeyPrefix,
		"recover_interval", q.recoverInterval.String(),
		"recover_max_age", q.recoverMaxAge.String(),
	)
	return nil
}

// OnConfigReload implements cf.ConfigReloader. It re-reads queue tunables from
// the bound configuration source. Queue name and key prefix are not changed.
// Credential rotation for the shared client is handled by the valkey component.
func (q *PriorityQueue) OnConfigReload(source string, cfg any) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if source != q.configSource || q.fw == nil {
		return
	}
	if err := q.applyConfigSourceLocked(false); err != nil {
		q.logger.Error("cf_vpq: config reload rejected", "err", err)
		return
	}
	q.logger.Info("cf_vpq: tunables reloaded",
		"queue", q.cfg.QueueName,
		"recover_interval", q.recoverInterval.String(),
		"poll_interval_sec", q.cfg.PollInterval,
	)
}

// applyConfigSourceLocked loads the named source onto baseCfg. When identity
// is true, queue name/prefix from the source are applied; otherwise only
// tunables. Caller must hold q.mu.
func (q *PriorityQueue) applyConfigSourceLocked(identity bool) error {
	conf, ok := cf.Get[*cf_configuration.Configuration](q.fw)
	if !ok {
		return errors.New("cf_vpq: configuration component not registered")
	}
	loaded, ok := cf_configuration.Get[PQConfig](conf, q.configSource)
	if !ok {
		return fmt.Errorf("cf_vpq: configuration source %q not found", q.configSource)
	}
	cfg := q.baseCfg
	if cfg.BlockDuration <= 0 {
		cfg.BlockDuration = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1
	}
	if identity {
		overlayConfig(&cfg, *loaded)
	} else {
		queueName, keyPrefix := q.cfg.QueueName, q.cfg.KeyPrefix
		overlayConfig(&cfg, *loaded)
		cfg.QueueName = queueName
		cfg.KeyPrefix = keyPrefix
	}
	q.cfg = cfg
	q.applyRecoverHealthFromConfig(*loaded)
	return nil
}

func (q *PriorityQueue) applyRecoverHealthFromConfig(loaded PQConfig) {
	if loaded.RecoverInterval > 0 {
		q.recoverInterval = time.Duration(loaded.RecoverInterval) * time.Second
	}
	if loaded.RecoverMaxAge > 0 {
		q.recoverMaxAge = time.Duration(loaded.RecoverMaxAge) * time.Second
	}
	if loaded.MaxDepth > 0 {
		q.maxDepth = int64(loaded.MaxDepth)
		q.maxDepthSet = true
	}
	if loaded.MaxInFlight > 0 {
		q.maxInFlight = int64(loaded.MaxInFlight)
		q.maxInFlightSet = true
	}
	if loaded.Workers > 0 {
		q.workers = loaded.Workers
		if q.workers < 1 {
			q.workers = 1
		}
		if q.handler != nil && !q.maxInFlightSet {
			q.maxInFlight = defaultMaxInFlight(q.workers)
		}
	}
}

// Shutdown implements cf.CaerusComponent. It stops serving; the shared valkey
// client is owned and closed by the valkey component.
func (q *PriorityQueue) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	if q.logsSub != nil {
		q.logsSub.Unsubscribe()
		q.logsSub = nil
	}
	q.valkey = nil
	q.mu.Unlock()
	return nil
}

// Run implements cf.Runnable. It runs the auto-consumer (when WithHandler is
// set) and/or the deadlock-recovery ticker until ctx is canceled.
func (q *PriorityQueue) Run(ctx context.Context) error {
	interval, maxAge := q.getRecoverSettings()
	if interval <= 0 && q.handler == nil {
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	if interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.runRecoverLoop(runCtx, interval, maxAge)
		}()
	}
	if q.handler != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Consume(runCtx, q.handler); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
}

func (q *PriorityQueue) getRecoverSettings() (interval, maxAge time.Duration) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.recoverInterval, q.recoverMaxAge
}

func (q *PriorityQueue) runRecoverLoop(ctx context.Context, interval, maxAge time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	q.recoverOnce(ctx, maxAge)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			interval, maxAge = q.getRecoverSettings()
			if interval <= 0 {
				return
			}
			q.recoverOnce(ctx, maxAge)
		}
	}
}

func (q *PriorityQueue) recoverOnce(ctx context.Context, maxAge time.Duration) {
	if _, err := q.PurgeExpired(ctx); err != nil {
		if !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
			q.logger.Error("cf_vpq: purge expired", "err", err)
		}
	}
	n, err := q.RecoverDeadlocked(ctx, maxAge)
	if err != nil {
		if !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
			q.logger.Error("cf_vpq: recover deadlocked", "err", err)
		}
		return
	}
	if n > 0 {
		q.mu.RLock()
		queue := q.cfg.QueueName
		q.mu.RUnlock()
		q.logger.Info("cf_vpq: recovered deadlocked items", "count", n, "queue", queue)
	}
}

// getClient returns the current valkey client from the valkey component, or
// nil after Shutdown. This always returns the live client, so config reloads
// that swap the connection are picked up automatically.
func (q *PriorityQueue) getClient() valkey.Client {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.valkey == nil {
		return nil
	}
	return q.valkey.Client()
}

// Health implements cf.HealthProvider. It pings the backing valkey server and,
// when thresholds are non-zero, fails if queue depth or in-flight count exceed
// them (WithMaxDepth / WithMaxInFlight; defaults apply when WithHandler is set).
// A nil client is unhealthy.
func (q *PriorityQueue) Health(ctx context.Context) error {
	client := q.getClient()
	if client == nil {
		return errors.New("cf_vpq: client is not initialized")
	}
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		return err
	}
	q.mu.RLock()
	maxDepth, maxInFlight := q.maxDepth, q.maxInFlight
	q.mu.RUnlock()
	if maxDepth > 0 {
		n, err := q.Count(ctx)
		if err != nil {
			return fmt.Errorf("cf_vpq: health depth: %w", err)
		}
		if n > maxDepth {
			return fmt.Errorf("cf_vpq: queue depth %d exceeds max %d", n, maxDepth)
		}
	}
	if maxInFlight > 0 {
		n, err := q.InFlightCount(ctx)
		if err != nil {
			return fmt.Errorf("cf_vpq: health in-flight: %w", err)
		}
		if n > maxInFlight {
			return fmt.Errorf("cf_vpq: in-flight %d exceeds max %d", n, maxInFlight)
		}
	}
	return nil
}

// Metrics implements cf_observability.MetricsProvider. While initialized it
// reports info, depth, in-flight, and cumulative recoveries; before Init or
// after Shutdown it returns nil (observability lazy pickup).
func (q *PriorityQueue) Metrics() []cf_observability.Metric {
	if q.getClient() == nil {
		return nil
	}
	q.mu.RLock()
	queue := q.cfg.QueueName
	q.mu.RUnlock()
	labels := map[string]string{"queue": queue, "component": q.Name()}
	ms := []cf_observability.Metric{{
		Name:   "vpq_info",
		Help:   "Valkey priority queue state.",
		Value:  1,
		Labels: labels,
	}}
	mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if depth, err := q.Count(mctx); err == nil {
		ms = append(ms, cf_observability.Metric{
			Name:   "vpq_depth",
			Help:   "Number of distinct ids in the priority queue.",
			Value:  float64(depth),
			Labels: labels,
		})
	}
	if inflight, err := q.InFlightCount(mctx); err == nil {
		ms = append(ms, cf_observability.Metric{
			Name:   "vpq_in_flight",
			Help:   "Number of popped but unacked items.",
			Value:  float64(inflight),
			Labels: labels,
		})
	}
	ms = append(ms, cf_observability.Metric{
		Name:   "vpq_recoveries_total",
		Help:   "Total items requeued by deadlock recovery.",
		Value:  float64(q.recoveriesTotal.Load()),
		Labels: labels,
		Type:   cf_observability.MetricTypeCounter,
	})
	return ms
}

func (q *PriorityQueue) stringKey(id string) string {
	return q.payloadKeyPrefix() + id
}

func (q *PriorityQueue) payloadKeyPrefix() string {
	return q.cfg.KeyPrefix + "squeue:" + q.cfg.QueueName + ":"
}

func (q *PriorityQueue) zsetKey() string {
	return q.cfg.KeyPrefix + "zqueue:" + q.cfg.QueueName
}

func (q *PriorityQueue) deadlockKey() string {
	return q.cfg.KeyPrefix + "pqdeadlocks:" + q.cfg.QueueName
}

func (q *PriorityQueue) expiryKey() string {
	return q.cfg.KeyPrefix + "zexpiry:" + q.cfg.QueueName
}

func (q *PriorityQueue) wakeKey() string {
	return q.cfg.KeyPrefix + "pqwake:" + q.cfg.QueueName
}

func (q *PriorityQueue) channel() string {
	return q.cfg.KeyPrefix + q.cfg.QueueName
}

func (q *PriorityQueue) cacheTimeoutSec() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.cfg.CacheTimeout
}

func (q *PriorityQueue) blockDuration() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	d := time.Duration(q.cfg.BlockDuration) * time.Second
	if d <= 0 {
		return time.Second
	}
	return d
}

// Add enqueues an item and returns whether its payload was newly stored. A
// false result means the id is already queued: the existing payload is kept
// and the item weight is incremented (this is the "add more weight" semantic;
// it does not overwrite). When a watermark delay is configured, a pub/sub
// notification is throttled to that interval; notification failures are logged
// but do not fail the add.
func (q *PriorityQueue) Add(ctx context.Context, id, value string) (bool, error) {
	cl := q.getClient()
	if cl == nil {
		return false, ErrClosed
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	ttl := strconv.Itoa(q.cacheTimeoutSec())
	res := scriptAdd.Exec(ctx, cl,
		[]string{q.stringKey(id), q.zsetKey(), q.expiryKey(), q.wakeKey()},
		[]string{id, value, ttl, now},
	)
	added, err := res.AsInt64()
	if err != nil {
		return false, fmt.Errorf("cf_vpq: add: %w", err)
	}
	if err := q.publishToPubSub(ctx); err != nil {
		q.logger.Error("cf_vpq: publish", "err", err)
	}
	return added == 1, nil
}

// PurgeExpired removes queued items whose CacheTimeout residence has elapsed,
// deleting the zqueue member and payload together. It is a no-op when
// CacheTimeout is 0. Safe to call concurrently; also run from recover ticks
// and before each claim.
func (q *PriorityQueue) PurgeExpired(ctx context.Context) (int64, error) {
	if q.cacheTimeoutSec() <= 0 {
		return 0, nil
	}
	cl := q.getClient()
	if cl == nil {
		return 0, ErrClosed
	}
	n, err := scriptPurgeExpired.Exec(ctx, cl,
		[]string{q.expiryKey(), q.zsetKey()},
		[]string{strconv.FormatInt(time.Now().Unix(), 10), q.payloadKeyPrefix()},
	).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("cf_vpq: purge expired: %w", err)
	}
	if n > 0 {
		q.logger.Info("cf_vpq: purged expired queued items",
			"count", n, "queue", q.cfg.QueueName)
	}
	return n, nil
}

// Count returns the number of queued items (distinct ids).
func (q *PriorityQueue) Count(ctx context.Context) (int64, error) {
	cl := q.getClient()
	if cl == nil {
		return 0, ErrClosed
	}
	return cl.Do(ctx, cl.B().Zcard().Key(q.zsetKey()).Build()).AsInt64()
}

// InFlightCount returns the number of popped but unacked items (deadlock set).
func (q *PriorityQueue) InFlightCount(ctx context.Context) (int64, error) {
	cl := q.getClient()
	if cl == nil {
		return 0, ErrClosed
	}
	return cl.Do(ctx, cl.B().Zcard().Key(q.deadlockKey()).Build()).AsInt64()
}

// IntCount returns the number of queued items, or 0 on error.
func (q *PriorityQueue) IntCount(ctx context.Context) int64 {
	n, err := q.Count(ctx)
	if err != nil {
		return 0
	}
	return n
}

// BlockingBGet pops and returns the highest-weighted item, waiting up to the
// block duration. It returns (nil, nil) when the wait expires with no item.
// Pop and deadlock tracking are a single atomic Lua claim (no crash window).
// Waiting uses a wake list (BRPOP), not BZPOPMAX, so the wait cannot orphan
// an item.
func (q *PriorityQueue) BlockingBGet(ctx context.Context) (*BGetObject, error) {
	cl := q.getClient()
	if cl == nil {
		return nil, ErrClosed
	}
	deadline := time.Now().Add(q.blockDuration())
	for {
		if _, err := q.PurgeExpired(ctx); err != nil {
			return nil, err
		}
		item, err := q.claim(ctx)
		if err != nil {
			return nil, err
		}
		if item != nil {
			return item, nil
		}
		// Orphan drops / races may leave more members; retry without sleeping.
		if q.IntCount(ctx) > 0 {
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		// Non-destructive wait: Add/Requeue/Recover LPUSH the wake key.
		timeout := remaining.Seconds()
		_, err = cl.Do(ctx, cl.B().Brpop().Key(q.wakeKey()).Timeout(timeout).Build()).ToArray()
		if errors.Is(err, valkey.Nil) {
			return nil, nil
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, nil
			}
			return nil, fmt.Errorf("cf_vpq: wake wait: %w", err)
		}
		// Spurious/contended wake: loop and claim again.
	}
}

// asFloatScore accepts RESP3 doubles or ints (Lua often returns whole scores
// as integers). Prefer this over ToFloat64 alone for script results.
func asFloatScore(m valkey.ValkeyMessage) (float64, error) {
	if f, err := m.ToFloat64(); err == nil {
		return f, nil
	}
	if i, err := m.AsInt64(); err == nil {
		return float64(i), nil
	}
	if s, err := m.ToString(); err == nil {
		return strconv.ParseFloat(s, 64)
	}
	return 0, fmt.Errorf("cf_vpq: score is neither float, int, nor numeric string")
}

// claim atomically pops the highest-weighted item and records it in the
// deadlock set. Returns (nil, nil) when the queue is empty. Corrupt entries
// (queue member without payload) are dropped and logged, not requeued.
func (q *PriorityQueue) claim(ctx context.Context) (*BGetObject, error) {
	cl := q.getClient()
	if cl == nil {
		return nil, ErrClosed
	}
	res, err := scriptClaim.Exec(ctx, cl,
		[]string{q.zsetKey(), q.deadlockKey(), q.expiryKey()},
		[]string{strconv.FormatInt(time.Now().Unix(), 10), q.payloadKeyPrefix()},
	).ToArray()
	if errors.Is(err, valkey.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: claim: %w", err)
	}
	if len(res) < 3 {
		return nil, fmt.Errorf("cf_vpq: unexpected claim result with %d elements", len(res))
	}
	ok, err := res[0].AsInt64()
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: decode claim status: %w", err)
	}
	id, err := res[1].ToString()
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: decode claim id: %w", err)
	}
	score, err := asFloatScore(res[2])
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: decode claim score: %w", err)
	}
	if ok == 0 {
		q.logger.Error("cf_vpq: dropped orphan item (payload missing)",
			"queue", q.cfg.QueueName, "id", id, "score", score)
		return nil, nil
	}
	if len(res) < 4 {
		return nil, fmt.Errorf("cf_vpq: claim success missing payload")
	}
	val, err := res[3].ToString()
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: decode claim payload: %w", err)
	}
	return &BGetObject{ObjectID: id, ObjectScore: score, ObjectValue: val}, nil
}

// Ack completes a popped item: it removes the deadlock tracking, deletes the
// payload and clears any lingering queue/expiry member. Idempotent; safe to
// call for already-acked ids.
func (q *PriorityQueue) Ack(ctx context.Context, id string) error {
	cl := q.getClient()
	if cl == nil {
		return ErrClosed
	}
	err := scriptAck.Exec(ctx, cl,
		[]string{q.deadlockKey(), q.stringKey(id), q.zsetKey(), q.expiryKey()},
		[]string{id},
	).Error()
	if err != nil {
		return fmt.Errorf("cf_vpq: ack: %w", err)
	}
	return nil
}

// Requeue returns a popped item to the queue with weight +1 and drops its
// deadlock tracking. Call it after a manual consumer failed to process the
// item; the payload is preserved. CacheTimeout residence is refreshed.
func (q *PriorityQueue) Requeue(ctx context.Context, id string) error {
	cl := q.getClient()
	if cl == nil {
		return ErrClosed
	}
	err := scriptRequeue.Exec(ctx, cl,
		[]string{q.zsetKey(), q.deadlockKey(), q.expiryKey(), q.wakeKey()},
		[]string{id, strconv.Itoa(q.cacheTimeoutSec()), strconv.FormatInt(time.Now().Unix(), 10)},
	).Error()
	if err != nil {
		return fmt.Errorf("cf_vpq: requeue: %w", err)
	}
	return nil
}

// InFlightItem is an item that was popped (removed from the queue) but not yet
// acked or requeued.
type InFlightItem struct {
	ObjectID string
	PoppedAt time.Time
}

// Deadlocked returns the items currently in flight (popped but not acked or
// requeued), with the time each was popped. A consumer that crashed between a
// successful pop and its Ack/Requeue leaves its item listed here until
// RecoverDeadlocked returns it to the queue.
func (q *PriorityQueue) Deadlocked(ctx context.Context) ([]InFlightItem, error) {
	cl := q.getClient()
	if cl == nil {
		return nil, ErrClosed
	}
	zs, err := cl.Do(ctx, cl.B().Zrangebyscore().Key(q.deadlockKey()).Min("-inf").Max("+inf").Withscores().Build()).AsZScores()
	if err != nil {
		return nil, fmt.Errorf("cf_vpq: deadlocked: %w", err)
	}
	items := make([]InFlightItem, 0, len(zs))
	for _, z := range zs {
		items = append(items, InFlightItem{ObjectID: z.Member, PoppedAt: time.Unix(int64(z.Score), 0)})
	}
	return items, nil
}

// RecoverDeadlocked requeues every in-flight item that has been unacked for
// longer than maxAge, restoring it to the queue with weight +1. It returns the
// number of items recovered. Run invokes this on WithRecoverInterval (default
// 30s when a handler is set); it may also be called manually.
func (q *PriorityQueue) RecoverDeadlocked(ctx context.Context, maxAge time.Duration) (int64, error) {
	cl := q.getClient()
	if cl == nil {
		return 0, ErrClosed
	}
	res := scriptRecover.Exec(ctx, cl,
		[]string{q.deadlockKey(), q.zsetKey(), q.expiryKey(), q.wakeKey()},
		[]string{
			strconv.FormatInt(time.Now().Add(-maxAge).Unix(), 10),
			strconv.Itoa(q.cacheTimeoutSec()),
			strconv.FormatInt(time.Now().Unix(), 10),
		},
	)
	n, err := res.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("cf_vpq: recover deadlocked: %w", err)
	}
	if n > 0 {
		q.recoveriesTotal.Add(n)
	}
	return n, nil
}

// publishToPubSub notifies consumers, throttled to the watermark delay.
func (q *PriorityQueue) publishToPubSub(ctx context.Context) error {
	delay := time.Duration(q.cfg.PublishWatermarkDelay) * time.Second
	if delay <= 0 {
		return nil
	}
	q.mu.Lock()
	if !q.lastPublish.IsZero() && time.Since(q.lastPublish) < delay {
		q.mu.Unlock()
		return nil
	}
	q.lastPublish = time.Now()
	q.mu.Unlock()

	cl := q.getClient()
	if cl == nil {
		return ErrClosed
	}
	if err := cl.Do(ctx, cl.B().Publish().Channel(q.channel()).Message(fmt.Sprintf("%d", q.IntCount(ctx))).Build()).Error(); err != nil {
		return fmt.Errorf("cf_vpq: publish: %w", err)
	}
	return nil
}

// Consume runs the auto-consumer until ctx is canceled: a pub/sub wakeup
// triggers an immediate pass, otherwise the queue is polled at the poll
// interval. Uses WithWorkers concurrent loops sharing one subscription.
// A failed handler is logged and the item requeued. Handlers must be safe for
// concurrent use when workers > 1.
func (q *PriorityQueue) Consume(ctx context.Context, handler Handler) error {
	cl := q.getClient()
	if cl == nil {
		return ErrClosed
	}
	if handler == nil {
		return errors.New("cf_vpq: nil handler")
	}

	workers := q.getWorkers()
	wake := make(chan struct{}, workers)
	subErr := make(chan error, 1)
	go func() {
		err := cl.Receive(ctx, cl.B().Subscribe().Channel(q.channel()).Build(), func(msg valkey.PubSubMessage) {
			select {
			case wake <- struct{}{}:
			default:
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, valkey.ErrClosing) {
			subErr <- err
		}
	}()

	if workers == 1 {
		return q.consumeLoop(ctx, handler, wake, subErr)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.consumeLoop(runCtx, handler, wake, subErr); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
}

func (q *PriorityQueue) getWorkers() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.workers < 1 {
		return 1
	}
	return q.workers
}

// consumeLoop is one auto-consumer worker.
func (q *PriorityQueue) consumeLoop(ctx context.Context, handler Handler, wake <-chan struct{}, subErr <-chan error) error {
	for {
		q.mu.RLock()
		poll := time.Duration(q.cfg.PollInterval) * time.Second
		q.mu.RUnlock()
		if poll <= 0 {
			poll = time.Second
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-subErr:
			return err
		default:
		}
		if q.IntCount(ctx) > 0 {
			if err := q.processOne(ctx, handler); err != nil {
				q.logger.Error("cf_vpq: process item", "err", err)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-time.After(poll):
		}
	}
}

// processOne pops one item and runs the handler, requeueing on failure.
// Concurrent callers are safe: claim is atomic in Valkey.
func (q *PriorityQueue) processOne(ctx context.Context, handler Handler) error {
	item, err := q.BlockingBGet(ctx)
	if err != nil {
		return err
	}
	if item == nil {
		return nil // wait expired or orphan/empty
	}
	if err := handler(item); err != nil {
		if reqErr := q.Requeue(ctx, item.ObjectID); reqErr != nil {
			return fmt.Errorf("cf_vpq: handler failed for %q: %v (requeue also failed: %w)", item.ObjectID, err, reqErr)
		}
		return fmt.Errorf("cf_vpq: handler failed for %q: %w", item.ObjectID, err)
	}
	return q.Ack(ctx, item.ObjectID)
}

var _ cf.CaerusComponent = (*PriorityQueue)(nil)
var _ cf.Dependencies = (*PriorityQueue)(nil)
var _ cf.Runnable = (*PriorityQueue)(nil)
var _ cf.HealthProvider = (*PriorityQueue)(nil)
var _ cf_observability.MetricsProvider = (*PriorityQueue)(nil)
var _ cf.ConfigReloader = (*PriorityQueue)(nil)
var _ cf.ConfigSourceRegistrar = (*PriorityQueue)(nil)
