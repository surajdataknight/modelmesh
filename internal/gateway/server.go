package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server runs the public and admin listeners and owns graceful shutdown.
type Server struct {
	g          *Gateway
	public     *http.Server
	admin      *http.Server
	baseCancel context.CancelCauseFunc
}

// NewServer wires the gateway's handlers into HTTP servers configured for
// long-lived streams:
//   - WriteTimeout stays zero: it sets one deadline for the whole response
//     and cuts off any stream that outlives it.
//     The handler arms rolling per-write deadlines instead, which bound slow
//     clients without bounding stream length;
//   - ReadTimeout stays zero; the handler applies timeouts.body_read to the
//     body itself so a slow upload gets a clear 408 JSON error;
//   - every request context derives from a base context that shutdown can
//     cancel with a cause, which lets streams end with a clear error event.
func NewServer(g *Gateway) *Server {
	base, cancel := context.WithCancelCause(context.Background())
	t := g.cfg.Timeouts
	errLog := slog.NewLogLogger(g.log.Handler(), slog.LevelWarn)
	s := &Server{g: g, baseCancel: cancel}
	s.public = &http.Server{
		Addr:              g.cfg.Listen,
		Handler:           g.Handler(),
		ReadHeaderTimeout: t.ReadHeader.D(),
		IdleTimeout:       t.IdleConn.D(),
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          errLog,
		BaseContext:       func(net.Listener) context.Context { return base },
	}
	if g.cfg.AdminListen != "" {
		s.admin = &http.Server{
			Addr:              g.cfg.AdminListen,
			Handler:           g.AdminHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			ErrorLog:          errLog,
		}
	}
	return s
}

// Serve serves on the given listeners (admin may be nil). It returns when
// the public listener stops; http.ErrServerClosed is reported as nil.
func (s *Server) Serve(public, admin net.Listener) error {
	if admin != nil && s.admin != nil {
		go func() {
			if err := s.admin.Serve(admin); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.g.log.Error("admin listener failed", "err", err)
			}
		}()
	}
	var err error
	if s.g.cfg.TLS.Enabled() {
		err = s.public.ServeTLS(public, s.g.cfg.TLS.CertFile, s.g.cfg.TLS.KeyFile)
	} else {
		err = s.public.Serve(public)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Listen opens the configured listeners.
func (s *Server) Listen() (public, admin net.Listener, err error) {
	public, err = net.Listen("tcp", s.public.Addr)
	if err != nil {
		return nil, nil, err
	}
	if s.admin != nil {
		admin, err = net.Listen("tcp", s.admin.Addr)
		if err != nil {
			public.Close()
			return nil, nil, err
		}
	}
	return public, admin, nil
}

// Shutdown drains the gateway:
//  1. readiness turns false;
//  2. listeners close and idle keep-alive connections are dropped;
//  3. in-flight requests, including streams, get the grace period to finish;
//  4. whatever is still running is cancelled with errShutdown, which makes
//     streams send a final "gateway_shutdown" error event, and is given a
//     moment to write it before connections are closed.
func (s *Server) Shutdown(ctx context.Context) error {
	s.g.SetReady(false)
	grace := s.g.cfg.Timeouts.ShutdownGrace.D()
	graceCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	err := s.public.Shutdown(graceCtx)
	if err != nil {
		s.g.log.Warn("shutdown grace period expired; cancelling in-flight requests")
		s.baseCancel(errShutdown)
		done := make(chan struct{})
		go func() { s.g.active.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		_ = s.public.Close()
		err = nil
	}
	s.baseCancel(errShutdown)
	if s.admin != nil {
		adminCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.admin.Shutdown(adminCtx)
	}
	s.g.CloseIdleConnections()
	return err
}
