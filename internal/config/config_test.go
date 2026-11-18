package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

const minimal = `{
  "providers": [{"name": "mock-a", "base_url": "http://127.0.0.1:9001/v1"}],
  "models": [{"name": "fast", "targets": [{"provider": "mock-a", "model": "mock-small"}]}]
}`

func TestParseAppliesDefaultsAndResolvesKeys(t *testing.T) {
	cfg, err := Parse([]byte(minimal), env(map[string]string{
		"MODELMESH_API_KEYS": "ci:sk-test-local-key, sk-anon-local-key",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Listen != ":8080" || cfg.Retry.MaxAttempts != 3 || cfg.Timeouts.Idle.D() != 30*time.Second {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if cfg.Providers[0].MaxConcurrent != 256 {
		t.Fatalf("provider default max_concurrent = %d", cfg.Providers[0].MaxConcurrent)
	}
	if len(cfg.Auth.Keys) != 2 || cfg.Auth.Keys[0].Name != "ci" || cfg.Auth.Keys[1].Name != "key-2" {
		t.Fatalf("keys = %+v", cfg.Auth.Keys)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	_, err := Parse([]byte(`{"listne": ":1"}`), env(nil))
	if err == nil || !strings.Contains(err.Error(), "listne") {
		t.Fatalf("expected unknown-field error, got %v", err)
	}
}

func TestParseRejectsNumericDuration(t *testing.T) {
	_, err := Parse([]byte(`{"timeouts": {"idle": 30}}`), env(nil))
	if err == nil || !strings.Contains(err.Error(), "duration must be a string") {
		t.Fatalf("expected duration error, got %v", err)
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	raw := `{
	  "auth": {"mode": "api_keys"},
	  "retry": {"max_attempts": 0},
	  "providers": [
	    {"name": "a", "base_url": "ftp://x"},
	    {"name": "a", "base_url": "http://user:pw@host/v1", "api_key_env": "MISSING_KEY"}
	  ],
	  "models": [{"name": "m", "targets": [{"provider": "nope", "model": ""}]}]
	}`
	_, err := Parse([]byte(raw), env(nil))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		"MISSING_KEY", "no keys were found", "max_attempts", "ftp://x",
		"duplicate provider", "must not embed credentials", "unknown provider", "must not be empty",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestAuthNoneMustBeExplicit(t *testing.T) {
	raw := strings.Replace(minimal, "{", `{"auth": {"mode": "none"},`, 1)
	if _, err := Parse([]byte(raw), env(nil)); err != nil {
		t.Fatalf("explicit none should be valid: %v", err)
	}
	if _, err := Parse([]byte(minimal), env(nil)); err == nil {
		t.Fatal("missing keys without explicit mode none must fail")
	}
}

func TestParseKeys(t *testing.T) {
	cases := []struct {
		in      string
		wantErr string
		want    int
	}{
		{in: "", want: 0},
		{in: "# comment, a:0123456789abcdef0", want: 1},
		{in: "a:short", wantErr: "shorter than 16"},
		{in: "a:0123456789abcdef0,a:0123456789abcdef1", wantErr: "duplicate"},
		{in: "a:", wantErr: "empty key"},
	}
	for _, c := range cases {
		keys, err := ParseKeys(c.in)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("ParseKeys(%q) err = %v, want %q", c.in, err, c.wantErr)
			}
			continue
		}
		if err != nil || len(keys) != c.want {
			t.Errorf("ParseKeys(%q) = %v, %v; want %d keys", c.in, keys, err, c.want)
		}
	}
}

func TestKeysFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys")
	if err := os.WriteFile(path, []byte("# team keys\nweb:sk-web-local-key\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := strings.Replace(minimal, "{", `{"auth": {"keys_file": "`+path+`"},`, 1)
	cfg, err := Parse([]byte(raw), env(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Auth.Keys) != 1 || cfg.Auth.Keys[0].Name != "web" {
		t.Fatalf("keys = %+v", cfg.Auth.Keys)
	}
}

func TestExampleConfigsAreValid(t *testing.T) {
	matches, err := filepath.Glob("../../configs/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Skip("no example configs found")
	}
	e := env(map[string]string{
		"MODELMESH_API_KEYS": "dev:sk-dev-local-key",
		"OPENAI_API_KEY":     "sk-not-used",
	})
	for _, m := range matches {
		raw, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(raw, e); err != nil {
			t.Errorf("%s: %v", m, err)
		}
	}
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9090": true, "localhost:1": true, "[::1]:9": true,
		":9090": false, "0.0.0.0:9090": false, "10.0.0.1:9": false,
	} {
		if got := IsLoopback(addr); got != want {
			t.Errorf("IsLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}
