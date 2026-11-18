// Package config loads, defaults, and validates the ModelMesh gateway
// configuration. Configuration is JSON (standard library only); secrets are
// never stored in the file — only the names of environment variables that
// hold them.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// Duration is a time.Duration that marshals to and from JSON strings such as
// "250ms" or "30s". Bare numbers are rejected to avoid unit ambiguity.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"500ms\" or \"30s\", got %s", string(b))
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// Config is the root gateway configuration.
type Config struct {
	Listen      string     `json:"listen"`
	AdminListen string     `json:"admin_listen"`
	LogLevel    string     `json:"log_level"`
	TLS         TLS        `json:"tls"`
	Auth        Auth       `json:"auth"`
	Limits      Limits     `json:"limits"`
	Timeouts    Timeouts   `json:"timeouts"`
	Retry       Retry      `json:"retry"`
	Breaker     Breaker    `json:"breaker"`
	Providers   []Provider `json:"providers"`
	Models      []Model    `json:"models"`
}

// TLS optionally enables HTTPS on the public listener.
type TLS struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// Enabled reports whether TLS is configured.
func (t TLS) Enabled() bool { return t.CertFile != "" || t.KeyFile != "" }

// Auth configures client authentication.
type Auth struct {
	// Mode is "api_keys" (default) or "none". "none" must be chosen
	// explicitly; the gateway never silently runs unauthenticated.
	Mode string `json:"mode"`
	// KeysEnv names an environment variable holding a comma-separated list
	// of keys, each optionally prefixed by a name: "ci:sk-abc,web:sk-def".
	KeysEnv string `json:"keys_env"`
	// KeysFile is an alternative to KeysEnv: one key (or name:key) per line.
	KeysFile string `json:"keys_file"`

	// Keys holds the resolved keys. It is never read from or written to JSON.
	Keys []APIKey `json:"-"`
}

// APIKey is a resolved client key.
type APIKey struct {
	Name  string
	Value string
}

// Limits bounds resource usage.
type Limits struct {
	MaxBodyBytes          int64    `json:"max_body_bytes"`
	MaxResponseBytes      int64    `json:"max_response_bytes"`
	MaxEventBytes         int      `json:"max_event_bytes"`
	MaxConcurrentRequests int      `json:"max_concurrent_requests"`
	AdmissionWait         Duration `json:"admission_wait"`
	PerKeyRPS             float64  `json:"per_key_rps"`
	PerKeyBurst           int      `json:"per_key_burst"`
}

// Timeouts groups every deadline the gateway enforces.
type Timeouts struct {
	// Request is the overall deadline for one client request, all attempts
	// and the full stream included.
	Request Duration `json:"request"`
	// FirstToken bounds the time from sending an upstream streaming request
	// to receiving its first SSE event (headers included).
	FirstToken Duration `json:"first_token"`
	// Idle bounds the gap between two upstream SSE events.
	Idle Duration `json:"idle"`
	// Attempt bounds one non-streaming upstream attempt.
	Attempt Duration `json:"attempt"`
	// ClientWrite bounds a single write to the client (slow consumers).
	ClientWrite Duration `json:"client_write"`
	// BodyRead bounds reading the client request body.
	BodyRead Duration `json:"body_read"`
	// ReadHeader bounds reading client request headers.
	ReadHeader Duration `json:"read_header"`
	// IdleConn is the server keep-alive idle timeout.
	IdleConn Duration `json:"idle_conn"`
	// ShutdownGrace is how long in-flight requests may drain on shutdown.
	ShutdownGrace Duration `json:"shutdown_grace"`
}

// Retry configures retries and failover.
type Retry struct {
	MaxAttempts int      `json:"max_attempts"`
	BaseBackoff Duration `json:"base_backoff"`
	MaxBackoff  Duration `json:"max_backoff"`
	// BudgetRatio is the fraction of requests that may be retried over time.
	BudgetRatio float64 `json:"budget_ratio"`
	// BudgetMinPerSecond guarantees a floor of retries per second so that
	// low-traffic gateways can still retry.
	BudgetMinPerSecond float64 `json:"budget_min_per_second"`
	// DefaultCooldown applies after a 429 without a usable Retry-After.
	DefaultCooldown Duration `json:"default_cooldown"`
	// MaxCooldown caps any upstream Retry-After value.
	MaxCooldown Duration `json:"max_cooldown"`
}

// Breaker configures the per-provider circuit breaker.
type Breaker struct {
	FailureThreshold int      `json:"failure_threshold"`
	OpenDuration     Duration `json:"open_duration"`
	HalfOpenProbes   int      `json:"half_open_probes"`
}

// Provider is an upstream OpenAI-compatible endpoint.
type Provider struct {
	Name string `json:"name"`
	// BaseURL includes the API prefix, e.g. "http://127.0.0.1:11434/v1".
	BaseURL string `json:"base_url"`
	// APIKeyEnv names the environment variable holding the provider key.
	// Empty means no Authorization header is sent (mock, Ollama).
	APIKeyEnv     string `json:"api_key_env"`
	MaxConcurrent int    `json:"max_concurrent"`
	// Prices are in USD per 1,000 tokens and only feed the cost estimate
	// metric. Leave zero for local providers.
	PricePer1KPrompt     float64 `json:"price_per_1k_prompt"`
	PricePer1KCompletion float64 `json:"price_per_1k_completion"`

	// APIKey is resolved from APIKeyEnv at load time.
	APIKey string `json:"-"`
}

// Model maps a public model alias to an ordered list of upstream targets.
type Model struct {
	Name    string   `json:"name"`
	Targets []Target `json:"targets"`
}

// Target is one upstream (provider, model) pair.
type Target struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// Defaults returns a configuration with every default applied and no
// providers or models.
func Defaults() Config {
	return Config{
		Listen:      ":8080",
		AdminListen: "127.0.0.1:9090",
		LogLevel:    "info",
		Auth:        Auth{Mode: "api_keys", KeysEnv: "MODELMESH_API_KEYS"},
		Limits: Limits{
			MaxBodyBytes:          1 << 20,
			MaxResponseBytes:      8 << 20,
			MaxEventBytes:         1 << 20,
			MaxConcurrentRequests: 2048,
			PerKeyBurst:           0,
		},
		Timeouts: Timeouts{
			Request:       Duration(5 * time.Minute),
			FirstToken:    Duration(30 * time.Second),
			Idle:          Duration(30 * time.Second),
			Attempt:       Duration(2 * time.Minute),
			ClientWrite:   Duration(10 * time.Second),
			BodyRead:      Duration(10 * time.Second),
			ReadHeader:    Duration(10 * time.Second),
			IdleConn:      Duration(2 * time.Minute),
			ShutdownGrace: Duration(30 * time.Second),
		},
		Retry: Retry{
			MaxAttempts:        3,
			BaseBackoff:        Duration(100 * time.Millisecond),
			MaxBackoff:         Duration(2 * time.Second),
			BudgetRatio:        0.2,
			BudgetMinPerSecond: 5,
			DefaultCooldown:    Duration(time.Second),
			MaxCooldown:        Duration(time.Minute),
		},
		Breaker: Breaker{
			FailureThreshold: 5,
			OpenDuration:     Duration(10 * time.Second),
			HalfOpenProbes:   1,
		},
	}
}

// Load reads, defaults, resolves secrets, and validates the file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw, os.Getenv)
}

// Parse decodes raw JSON on top of Defaults, resolves secrets through
// getenv, and validates the result. Unknown fields are rejected so typos
// fail at startup instead of being silently ignored.
func Parse(raw []byte, getenv func(string) string) (*Config, error) {
	cfg := Defaults()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if dec.More() {
		return nil, errors.New("parse config: trailing data after JSON object")
	}
	applyProviderDefaults(&cfg)
	// Report secret and semantic problems together so one startup shows
	// every mistake instead of one per restart.
	if err := errors.Join(cfg.resolveSecrets(getenv), cfg.Validate()); err != nil {
		return nil, fmt.Errorf("invalid config:\n%w", err)
	}
	return &cfg, nil
}

func applyProviderDefaults(cfg *Config) {
	for i := range cfg.Providers {
		if cfg.Providers[i].MaxConcurrent == 0 {
			cfg.Providers[i].MaxConcurrent = 256
		}
	}
	if cfg.Limits.PerKeyRPS > 0 && cfg.Limits.PerKeyBurst == 0 {
		cfg.Limits.PerKeyBurst = int(cfg.Limits.PerKeyRPS)
		if cfg.Limits.PerKeyBurst < 1 {
			cfg.Limits.PerKeyBurst = 1
		}
	}
}

func (c *Config) resolveSecrets(getenv func(string) string) error {
	var errs []error
	if c.Auth.Mode == "api_keys" {
		var src string
		switch {
		case c.Auth.KeysFile != "":
			b, err := os.ReadFile(c.Auth.KeysFile)
			if err != nil {
				errs = append(errs, fmt.Errorf("auth.keys_file: %w", err))
			}
			src = strings.ReplaceAll(string(b), "\n", ",")
		case c.Auth.KeysEnv != "":
			src = getenv(c.Auth.KeysEnv)
		}
		keys, err := ParseKeys(src)
		if err != nil {
			errs = append(errs, err)
		}
		c.Auth.Keys = keys
	}
	for i := range c.Providers {
		p := &c.Providers[i]
		if p.APIKeyEnv == "" {
			continue
		}
		key := getenv(p.APIKeyEnv)
		p.APIKey = key
		if p.APIKey == "" {
			errs = append(errs, fmt.Errorf("provider %q: environment variable %s is empty or unset", p.Name, p.APIKeyEnv))
		}
	}
	return errors.Join(errs...)
}

// ParseKeys parses "name:key" or "key" entries separated by commas.
// Blank entries and lines starting with '#' are ignored.
func ParseKeys(src string) ([]APIKey, error) {
	var keys []APIKey
	seen := map[string]bool{}
	for i, entry := range strings.Split(src, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		name, value, found := strings.Cut(entry, ":")
		if !found {
			name, value = fmt.Sprintf("key-%d", i+1), entry
		}
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("auth: entry %d has an empty key", i+1)
		}
		if len(value) < 16 {
			return nil, fmt.Errorf("auth: key %q is shorter than 16 characters", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("auth: duplicate key name %q", name)
		}
		seen[name] = true
		keys = append(keys, APIKey{Name: name, Value: value})
	}
	return keys, nil
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

// Validate checks semantic constraints and returns every problem found.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		add("listen: %v", err)
	}
	if c.AdminListen != "" {
		if _, _, err := net.SplitHostPort(c.AdminListen); err != nil {
			add("admin_listen: %v", err)
		}
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("log_level: must be debug, info, warn, or error (got %q)", c.LogLevel)
	}
	if c.TLS.Enabled() && (c.TLS.CertFile == "" || c.TLS.KeyFile == "") {
		add("tls: both cert_file and key_file are required")
	}
	switch c.Auth.Mode {
	case "none":
	case "api_keys":
		if len(c.Auth.Keys) == 0 {
			add("auth: mode is api_keys but no keys were found (set %s or auth.keys_file, or set auth.mode to \"none\" explicitly)", c.Auth.KeysEnv)
		}
	default:
		add("auth.mode: must be \"api_keys\" or \"none\" (got %q)", c.Auth.Mode)
	}

	l := c.Limits
	if l.MaxBodyBytes <= 0 || l.MaxResponseBytes <= 0 || l.MaxEventBytes <= 0 {
		add("limits: max_body_bytes, max_response_bytes, and max_event_bytes must be positive")
	}
	if l.MaxConcurrentRequests <= 0 {
		add("limits.max_concurrent_requests: must be positive")
	}
	if l.AdmissionWait < 0 || l.PerKeyRPS < 0 || l.PerKeyBurst < 0 {
		add("limits: admission_wait, per_key_rps, and per_key_burst must not be negative")
	}

	t := c.Timeouts
	for name, d := range map[string]Duration{
		"request": t.Request, "first_token": t.FirstToken, "idle": t.Idle,
		"attempt": t.Attempt, "client_write": t.ClientWrite, "body_read": t.BodyRead,
		"read_header": t.ReadHeader, "idle_conn": t.IdleConn, "shutdown_grace": t.ShutdownGrace,
	} {
		if d <= 0 {
			add("timeouts.%s: must be positive", name)
		}
	}

	r := c.Retry
	if r.MaxAttempts < 1 || r.MaxAttempts > 10 {
		add("retry.max_attempts: must be between 1 and 10")
	}
	if r.BaseBackoff <= 0 || r.MaxBackoff < r.BaseBackoff {
		add("retry: base_backoff must be positive and max_backoff >= base_backoff")
	}
	if r.BudgetRatio < 0 || r.BudgetRatio > 1 || r.BudgetMinPerSecond < 0 {
		add("retry: budget_ratio must be in [0,1] and budget_min_per_second >= 0")
	}
	if r.DefaultCooldown <= 0 || r.MaxCooldown < r.DefaultCooldown {
		add("retry: default_cooldown must be positive and max_cooldown >= default_cooldown")
	}

	b := c.Breaker
	if b.FailureThreshold < 1 || b.OpenDuration <= 0 || b.HalfOpenProbes < 1 {
		add("breaker: failure_threshold, open_duration, and half_open_probes must be positive")
	}

	providers := map[string]bool{}
	if len(c.Providers) == 0 {
		add("providers: at least one provider is required")
	}
	for i, p := range c.Providers {
		where := fmt.Sprintf("providers[%d]", i)
		if !nameRE.MatchString(p.Name) {
			add("%s.name: %q is not a valid name", where, p.Name)
		}
		if providers[p.Name] {
			add("%s.name: duplicate provider %q", where, p.Name)
		}
		providers[p.Name] = true
		u, err := url.Parse(p.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			add("%s.base_url: must be an absolute http(s) URL (got %q)", where, p.BaseURL)
		} else if u.User != nil {
			add("%s.base_url: must not embed credentials; use api_key_env", where)
		}
		if p.MaxConcurrent < 1 {
			add("%s.max_concurrent: must be positive", where)
		}
		if p.PricePer1KPrompt < 0 || p.PricePer1KCompletion < 0 {
			add("%s: prices must not be negative", where)
		}
	}

	models := map[string]bool{}
	if len(c.Models) == 0 {
		add("models: at least one model alias is required")
	}
	for i, m := range c.Models {
		where := fmt.Sprintf("models[%d]", i)
		if !nameRE.MatchString(m.Name) {
			add("%s.name: %q is not a valid model name", where, m.Name)
		}
		if models[m.Name] {
			add("%s.name: duplicate model %q", where, m.Name)
		}
		models[m.Name] = true
		if len(m.Targets) == 0 {
			add("%s.targets: at least one target is required", where)
		}
		for j, tg := range m.Targets {
			if !providers[tg.Provider] {
				add("%s.targets[%d].provider: unknown provider %q", where, j, tg.Provider)
			}
			if strings.TrimSpace(tg.Model) == "" {
				add("%s.targets[%d].model: must not be empty", where, j)
			}
		}
	}
	return errors.Join(errs...)
}

// IsLoopback reports whether addr ("host:port") binds only to loopback.
func IsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
