package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseChatRequestValid(t *testing.T) {
	req, err := ParseChatRequest([]byte(`{
		"model": "fast",
		"messages": [{"role": "user", "content": "hi"}],
		"stream": true,
		"stream_options": {"include_usage": true},
		"temperature": 0.2,
		"x_vendor_field": {"keep": "me"}
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Model != "fast" || !req.Stream || !req.IncludeUsage {
		t.Fatalf("parsed = %+v", req)
	}
}

func TestParseChatRequestInvalid(t *testing.T) {
	cases := map[string]string{
		`[]`:                                       "",
		`{"messages":[{"role":"user"}]}`:           "model",
		`{"model":"","messages":[{"role":"u"}]}`:   "model",
		`{"model":"m"}`:                            "messages",
		`{"model":"m","messages":[]}`:              "messages",
		`{"model":"m","messages":[{"content":1}]}`: "messages[0].role",
		`{"model":"m","messages":[{"role":"u"}],"stream":"yes"}`:                          "stream",
		`{"model":"m","messages":[{"role":"u"}],"stream_options":{"include_usage":true}}`: "stream_options",
		`not json`: "",
	}
	for body, param := range cases {
		_, err := ParseChatRequest([]byte(body))
		if err == nil {
			t.Errorf("%s: expected error", body)
			continue
		}
		if err.Param != param {
			t.Errorf("%s: param = %q, want %q", body, err.Param, param)
		}
	}
}

func TestUpstreamBodyRewritesModelAndPreservesUnknownFields(t *testing.T) {
	req, rerr := ParseChatRequest([]byte(`{"model":"fast","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"foo":1},"x_vendor_field":{"keep":"me"}}`))
	if rerr != nil {
		t.Fatal(rerr)
	}
	body, err := req.UpstreamBody("llama3.2:1b")
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out["model"] != "llama3.2:1b" {
		t.Errorf("model = %v", out["model"])
	}
	so := out["stream_options"].(map[string]any)
	if so["include_usage"] != true || so["foo"] != float64(1) {
		t.Errorf("stream_options = %v", so)
	}
	if out["x_vendor_field"].(map[string]any)["keep"] != "me" {
		t.Errorf("unknown field not preserved: %v", out)
	}
	if req.IncludeUsage {
		t.Error("client did not ask for usage")
	}
}

func TestUpstreamBodyNonStreamHasNoStreamOptions(t *testing.T) {
	req, _ := ParseChatRequest([]byte(`{"model":"fast","messages":[{"role":"user","content":"hi"}]}`))
	body, err := req.UpstreamBody("up")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "stream_options") {
		t.Fatalf("unexpected stream_options in %s", body)
	}
}

func TestInspectChunk(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want ChunkInfo
	}{
		{"content", `{"choices":[{"index":0,"delta":{"content":" hi"},"finish_reason":null}],"usage":null}`,
			ChunkInfo{Valid: true, HasContent: true}},
		{"role only", `{"choices":[{"delta":{"role":"assistant","content":""}}]}`,
			ChunkInfo{Valid: true}},
		{"usage only", `{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`,
			ChunkInfo{Valid: true, UsageOnly: true, Usage: &Usage{3, 5, 8}}},
		{"usage with choices", `{"choices":[{"delta":{}}],"usage": {"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			ChunkInfo{Valid: true, Usage: &Usage{1, 1, 2}}},
		{"error", `{"error":{"message":"overloaded","type":"server_error"}}`,
			ChunkInfo{Valid: true, ErrorBody: true}},
		{"content mentions usage", `{"choices":[{"delta":{"content":"\"usage\": {x}"}}]}`,
			ChunkInfo{Valid: true, HasContent: true}},
		{"error string", `{"error":"overloaded"}`, ChunkInfo{Valid: true, ErrorBody: true}},
		{"error null", `{"error":null,"choices":[{"delta":{"content":"x"}}]}`,
			ChunkInfo{Valid: true, HasContent: true}},
		{"null", `null`, ChunkInfo{}},
		{"string", `"hi"`, ChunkInfo{}},
		{"array", `[{"choices":[]}]`, ChunkInfo{}},
		{"empty object", `{}`, ChunkInfo{}},
		{"empty object with spaces", ` { } `, ChunkInfo{}},
		{"empty choices with provider fields", `{"choices":[],"prompt_filter_results":[{"prompt_index":0}]}`,
			ChunkInfo{Valid: true}},
		{"choice without delta", `{"choices":[{"index":0,"finish_reason":null,"content_filter_results":{}}]}`,
			ChunkInfo{Valid: true}},
		{"usage without choices", `{"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`,
			ChunkInfo{Valid: true, UsageOnly: true, Usage: &Usage{1, 2, 3}}},
		{"finish only", `{"choices":[{"finish_reason":"stop"}]}`, ChunkInfo{Valid: true}},
		{"tool call", `{"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}`, ChunkInfo{Valid: true}},
		{"malformed", `{"choices":[{"delta":`, ChunkInfo{}},
	}
	for _, c := range cases {
		got := InspectChunk([]byte(c.in))
		if got.Valid != c.want.Valid || got.HasContent != c.want.HasContent || got.UsageOnly != c.want.UsageOnly ||
			got.ErrorBody != c.want.ErrorBody || (got.Usage == nil) != (c.want.Usage == nil) ||
			(got.Usage != nil && *got.Usage != *c.want.Usage) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestResponseUsage(t *testing.T) {
	u := ResponseUsage([]byte(`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6}}`))
	if u == nil || u.TotalTokens != 6 {
		t.Fatalf("usage = %+v", u)
	}
	if ResponseUsage([]byte(`{}`)) != nil {
		t.Fatal("expected nil usage")
	}
}

const benchChunk = `{"id":"chatcmpl-123","object":"chat.completion.chunk","created":1700000000,"model":"mock-small","choices":[{"index":0,"delta":{"content":" lorem"},"finish_reason":null}]}`

func BenchmarkInspectChunk(b *testing.B) {
	p := []byte(benchChunk)
	b.ReportAllocs()
	for b.Loop() {
		_ = InspectChunk(p)
	}
}
