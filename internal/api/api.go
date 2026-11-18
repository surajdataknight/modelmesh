// Package api holds the OpenAI Chat Completions wire types that ModelMesh
// understands. The gateway treats this dialect as its lingua franca (ADR
// 0002): it validates and rewrites only the fields it needs and forwards
// every other field verbatim.
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Error types used in error bodies (OpenAI-style "type" field).
const (
	TypeInvalidRequest = "invalid_request_error"
	TypeAuthentication = "authentication_error"
	TypeRateLimit      = "rate_limit_error"
	TypeNotFound       = "not_found_error"
	TypeOverloaded     = "overloaded_error"
	TypeUpstream       = "upstream_error"
	TypeTimeout        = "timeout_error"
	TypeServer         = "server_error"
)

// ErrorBody is the OpenAI-compatible error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes one error.
type ErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code"`
}

// Usage is the OpenAI token usage object.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatRequest is a validated chat completion request. Only the fields the
// gateway acts on are decoded; the rest stay as raw JSON and are forwarded
// unchanged.
type ChatRequest struct {
	Model        string
	Stream       bool
	IncludeUsage bool // the client asked for stream_options.include_usage
	raw          map[string]json.RawMessage
}

// RequestError is a client-side validation failure.
type RequestError struct {
	Param   string
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(param, format string, args ...any) *RequestError {
	return &RequestError{Param: param, Message: fmt.Sprintf(format, args...)}
}

// ParseChatRequest validates the parts of the request the gateway relies on.
// It deliberately does not validate message content: that is the provider's
// job, and the gateway must not drift from providers' evolving schemas.
func ParseChatRequest(body []byte) (*ChatRequest, *RequestError) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil || raw == nil {
		return nil, badRequest("", "request body must be a JSON object")
	}
	req := &ChatRequest{raw: raw}

	m, ok := raw["model"]
	if !ok || json.Unmarshal(m, &req.Model) != nil || req.Model == "" {
		return nil, badRequest("model", "model is required and must be a non-empty string")
	}

	msgs, ok := raw["messages"]
	if !ok {
		return nil, badRequest("messages", "messages is required")
	}
	var list []map[string]json.RawMessage
	if err := json.Unmarshal(msgs, &list); err != nil || len(list) == 0 {
		return nil, badRequest("messages", "messages must be a non-empty array of objects")
	}
	for i, msg := range list {
		var role string
		if r, ok := msg["role"]; !ok || json.Unmarshal(r, &role) != nil || role == "" {
			return nil, badRequest(fmt.Sprintf("messages[%d].role", i), "messages[%d].role must be a non-empty string", i)
		}
	}

	if s, ok := raw["stream"]; ok && string(s) != "null" {
		if err := json.Unmarshal(s, &req.Stream); err != nil {
			return nil, badRequest("stream", "stream must be a boolean")
		}
	}
	if so, ok := raw["stream_options"]; ok && string(so) != "null" {
		var opts struct {
			IncludeUsage *bool `json:"include_usage"`
		}
		if err := json.Unmarshal(so, &opts); err != nil {
			return nil, badRequest("stream_options", "stream_options must be an object")
		}
		if !req.Stream {
			return nil, badRequest("stream_options", "stream_options is only allowed when stream is true")
		}
		req.IncludeUsage = opts.IncludeUsage != nil && *opts.IncludeUsage
	}
	return req, nil
}

// UpstreamBody renders the request for one upstream target: the model alias
// is replaced with the upstream model name and, for streams, usage reporting
// is always requested so the gateway can account tokens even when the client
// did not ask for them (the extra chunk is filtered out before the client
// sees it).
func (r *ChatRequest) UpstreamBody(upstreamModel string) ([]byte, error) {
	out := make(map[string]json.RawMessage, len(r.raw)+1)
	for k, v := range r.raw {
		out[k] = v
	}
	out["model"] = json.RawMessage(strconv.Quote(upstreamModel))
	if r.Stream {
		opts := map[string]json.RawMessage{}
		if so, ok := r.raw["stream_options"]; ok && string(so) != "null" {
			if err := json.Unmarshal(so, &opts); err != nil {
				return nil, err
			}
		}
		opts["include_usage"] = json.RawMessage("true")
		b, err := json.Marshal(opts)
		if err != nil {
			return nil, err
		}
		out["stream_options"] = b
	}
	return json.Marshal(out)
}

// ChunkInfo is what the gateway learns from one streamed chunk.
type ChunkInfo struct {
	Valid      bool   // payload is syntactically valid JSON
	HasContent bool   // carries a non-empty delta content (used to estimate tokens)
	UsageOnly  bool   // the final usage chunk with no choices
	Usage      *Usage // non-nil when the chunk reports usage
	ErrorBody  bool   // the provider sent an error object in-band
}

var (
	keyUsage   = []byte(`"usage"`)
	keyError   = []byte(`"error"`)
	keyContent = []byte(`"content"`)
)

// InspectChunk extracts accounting and error information from one OpenAI
// stream chunk. The fast path (ordinary content chunk) performs a validity
// scan plus a few byte searches and does not decode the JSON.
func InspectChunk(p []byte) ChunkInfo {
	info := ChunkInfo{Valid: json.Valid(p)}
	if !info.Valid {
		return info
	}
	info.HasContent = valueIsNonEmptyString(p, keyContent)
	usageObj := valueIsObject(p, keyUsage)
	errObj := valueIsObject(p, keyError)
	if !usageObj && !errObj {
		return info
	}
	var c struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   *Usage            `json:"usage"`
		Error   json.RawMessage   `json:"error"`
	}
	if json.Unmarshal(p, &c) != nil {
		info.Valid = false
		return info
	}
	info.Usage = c.Usage
	info.UsageOnly = c.Usage != nil && len(c.Choices) == 0
	info.ErrorBody = len(c.Error) > 0 && string(c.Error) != "null"
	return info
}

// valueIsObject reports whether some occurrence of key is followed by ':'
// and an object. It is a heuristic pre-filter; a positive result is always
// confirmed by a real decode.
func valueIsObject(p, key []byte) bool {
	for {
		i := bytes.Index(p, key)
		if i < 0 {
			return false
		}
		p = p[i+len(key):]
		j := skipWS(p, 0)
		if j < len(p) && p[j] == ':' {
			j = skipWS(p, j+1)
			if j < len(p) && p[j] == '{' {
				return true
			}
		}
	}
}

// valueIsNonEmptyString reports whether key is followed by a non-empty JSON
// string value.
func valueIsNonEmptyString(p, key []byte) bool {
	for {
		i := bytes.Index(p, key)
		if i < 0 {
			return false
		}
		p = p[i+len(key):]
		j := skipWS(p, 0)
		if j < len(p) && p[j] == ':' {
			j = skipWS(p, j+1)
			if j+1 < len(p) && p[j] == '"' && p[j+1] != '"' {
				return true
			}
		}
	}
}

func skipWS(p []byte, i int) int {
	for i < len(p) && (p[i] == ' ' || p[i] == '\t' || p[i] == '\n' || p[i] == '\r') {
		i++
	}
	return i
}

// ResponseUsage extracts usage from a non-streaming completion body.
func ResponseUsage(body []byte) *Usage {
	var r struct {
		Usage *Usage `json:"usage"`
	}
	if json.Unmarshal(body, &r) != nil {
		return nil
	}
	return r.Usage
}

// EstimateTokens is a deliberately rough fallback (about 4 bytes per token)
// used only when the provider reports no usage. Metrics label it
// "estimated" so it is never confused with provider-reported numbers.
func EstimateTokens(nBytes int) int {
	if nBytes <= 0 {
		return 0
	}
	return (nBytes + 3) / 4
}
