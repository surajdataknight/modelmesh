package sse

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, input string, max int) ([]string, error) {
	t.Helper()
	r := NewReader(strings.NewReader(input), max)
	var out []string
	for {
		ev, err := r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		out = append(out, string(ev))
	}
}

func TestReaderParsesEvents(t *testing.T) {
	input := ": keep-alive\n\n" +
		"data: {\"a\":1}\n\n" +
		"event: ignored\r\ndata:{\"b\":2}\r\n\r\n" +
		"data: line1\ndata: line2\n\n" +
		"id: 7\nretry: 10\n\n" +
		"data: [DONE]\n\n"
	got, err := readAll(t, input, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"a":1}`, `{"b":2}`, "line1\nline2", "[DONE]"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReaderTruncatedEvent(t *testing.T) {
	_, err := readAll(t, "data: {\"a\":1}\n\ndata: {\"partial\"", 1<<10)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
	_, err = readAll(t, "data: {\"a\":1}\n", 1<<10)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("missing blank line: err = %v, want ErrUnexpectedEOF", err)
	}
}

func TestReaderEventTooLarge(t *testing.T) {
	big := "data: " + strings.Repeat("x", 100<<10) + "\n\n"
	_, err := readAll(t, big, 1<<10)
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("err = %v, want ErrEventTooLarge", err)
	}
	// Many small data lines that add up past the limit.
	many := strings.Repeat("data: 0123456789\n", 200) + "\n"
	_, err = readAll(t, many, 1<<10)
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("err = %v, want ErrEventTooLarge", err)
	}
}

func TestReaderLongLineWithinLimit(t *testing.T) {
	payload := strings.Repeat("y", 40<<10) // longer than the 4 KiB bufio buffer
	got, err := readAll(t, "data: "+payload+"\n\n", 1<<20)
	if err != nil || len(got) != 1 || got[0] != payload {
		t.Fatalf("long line: err=%v len=%d", err, len(got))
	}
}

func TestEventBuffered(t *testing.T) {
	r := NewReader(strings.NewReader("data: a\n\ndata: b\n\ndata: partial"), 1<<10)
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if !r.EventBuffered() {
		t.Fatal("second event is fully buffered")
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if r.EventBuffered() {
		t.Fatal("only a partial event remains")
	}
}

func TestWriteEventRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	for _, p := range []string{`{"x":1}`, "multi\nline"} {
		if err := WriteEvent(&buf, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if got := buf.String(); got != "data: {\"x\":1}\n\ndata: multi\ndata: line\n\n" {
		t.Fatalf("wire = %q", got)
	}
	got, err := readAll(t, buf.String(), 1<<10)
	if err != nil || len(got) != 2 || got[1] != "multi\nline" {
		t.Fatalf("round trip = %q, %v", got, err)
	}
}

const sampleChunk = `{"id":"chatcmpl-123","object":"chat.completion.chunk","created":1700000000,"model":"mock-small","choices":[{"index":0,"delta":{"content":" lorem"},"finish_reason":null}]}`

func BenchmarkReaderNext(b *testing.B) {
	var stream bytes.Buffer
	const events = 1000
	for i := 0; i < events; i++ {
		stream.WriteString("data: " + sampleChunk + "\n\n")
	}
	raw := stream.Bytes()
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	src := bytes.NewReader(raw)
	r := NewReader(src, 1<<20)
	iters := 0
	for b.Loop() {
		iters++
		src.Reset(raw)
		r.br.Reset(src)
		for {
			if _, err := r.Next(); err != nil {
				break
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(iters*events), "ns/event")
}
