// Package sse implements the subset of the Server-Sent Events wire format
// that LLM providers use: "data:" fields, comments, and blank-line event
// terminators. The reader reuses its buffers so steady-state parsing does not
// allocate.
package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// ErrEventTooLarge is returned when an event exceeds the configured limit.
var ErrEventTooLarge = errors.New("sse: event exceeds size limit")

// Done is the OpenAI end-of-stream sentinel payload.
var Done = []byte("[DONE]")

// readBufferSize is the per-stream read buffer. LLM stream events are a few
// hundred bytes, so 4 KiB holds many of them; longer lines are still handled
// by accumulating into a scratch slice. (At high concurrency, memory per
// stream is dominated by goroutine stacks, not this buffer; see
// docs/benchmarks.md.)
const readBufferSize = 4 << 10

// Reader parses SSE events from an upstream body.
type Reader struct {
	br       *bufio.Reader
	maxEvent int
	data     []byte // current event payload, reused between events
	line     []byte // scratch for lines longer than the bufio buffer
	hasData  bool
}

// NewReader returns a Reader that rejects events larger than maxEvent bytes.
func NewReader(r io.Reader, maxEvent int) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readBufferSize), maxEvent: maxEvent}
}

// Reset makes the Reader read from src, reusing its buffers. It lets a
// sync.Pool recycle readers so a stream does not allocate a fresh read
// buffer. Unusually large scratch buffers are dropped rather than retained.
func (r *Reader) Reset(src io.Reader, maxEvent int) {
	r.br.Reset(src)
	r.maxEvent = maxEvent
	r.hasData = false
	if cap(r.data) > 64<<10 {
		r.data = nil
	}
	if cap(r.line) > 64<<10 {
		r.line = nil
	}
	r.data, r.line = r.data[:0], r.line[:0]
}

// EventBuffered reports whether a complete event is already buffered, so
// the next call to Next will not block on the network. The gateway uses it
// to coalesce flushes without ever holding back an event while it waits for
// a partial one to finish arriving.
func (r *Reader) EventBuffered() bool {
	n := r.br.Buffered()
	if n == 0 {
		return false
	}
	b, _ := r.br.Peek(n) // never blocks: n bytes are already buffered
	return bytes.Contains(b, []byte("\n\n")) || bytes.Contains(b, []byte("\n\r\n"))
}

// Next returns the data payload of the next event. Multi-line data fields
// are joined with "\n" as the SSE specification requires. The returned slice
// is only valid until the next call. A clean end of input returns io.EOF;
// input that ends inside an event returns io.ErrUnexpectedEOF.
func (r *Reader) Next() ([]byte, error) {
	r.data = r.data[:0]
	r.hasData = false
	for {
		line, err := r.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if r.hasData {
					return nil, io.ErrUnexpectedEOF
				}
				return nil, io.EOF
			}
			return nil, err
		}
		if len(line) == 0 {
			if r.hasData {
				return r.data, nil
			}
			continue // empty event or keep-alive blank line
		}
		if line[0] == ':' {
			continue // comment / heartbeat
		}
		field, value := line, []byte(nil)
		if i := bytes.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], line[i+1:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
		}
		if string(field) != "data" {
			continue // event:, id:, retry: are not used by OpenAI-dialect streams
		}
		if r.hasData {
			r.data = append(r.data, '\n')
		}
		if len(r.data)+len(value) > r.maxEvent {
			return nil, ErrEventTooLarge
		}
		r.data = append(r.data, value...)
		r.hasData = true
	}
}

// readLine returns one line without its terminator ("\n" or "\r\n").
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err == nil {
		return trimEOL(line), nil
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		if len(line) > 0 && errors.Is(err, io.EOF) {
			// A final line without a terminator is incomplete input.
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	// Long line: accumulate into scratch, bounded by maxEvent.
	r.line = append(r.line[:0], line...)
	for {
		line, err = r.br.ReadSlice('\n')
		if len(r.line)+len(line) > r.maxEvent+8 {
			return nil, ErrEventTooLarge
		}
		r.line = append(r.line, line...)
		if err == nil {
			return trimEOL(r.line), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
}

func trimEOL(b []byte) []byte {
	b = b[:len(b)-1]
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}

var (
	dataPrefix = []byte("data: ")
	eventEnd   = []byte("\n\n")
	newline    = []byte("\n")
)

// WriteEvent writes payload as one SSE event, splitting embedded newlines
// into multiple data fields. It performs no allocation.
func WriteEvent(w io.Writer, payload []byte) error {
	for {
		i := bytes.IndexByte(payload, '\n')
		if i < 0 {
			break
		}
		if _, err := w.Write(dataPrefix); err != nil {
			return err
		}
		if _, err := w.Write(payload[:i]); err != nil {
			return err
		}
		if _, err := w.Write(newline); err != nil {
			return err
		}
		payload = payload[i+1:]
	}
	if _, err := w.Write(dataPrefix); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.Write(eventEnd)
	return err
}
