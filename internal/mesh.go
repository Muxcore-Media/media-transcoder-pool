package internal

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/client"
)

func dialAddrForPeer(moduleID, httpAddr string) string {
	httpAddr = strings.TrimSpace(httpAddr)
	if httpAddr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil || port == "" {
		return httpAddr
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return net.JoinHostPort(host, port)
	}
	if os.Getenv("MUXCORE_MESH_DIAL_LOCAL") == "true" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	if moduleID != "" {
		return net.JoinHostPort(moduleID, port)
	}
	return httpAddr
}

func (m *Module) meshClient() *client.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mc
}

func (m *Module) dialCoreLoop(ctx context.Context) {
	meshAddr := strings.TrimSpace(os.Getenv("MUXCORE_GRPC_ADDR"))
	if meshAddr == "" {
		return
	}
	delay := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if m.tryDialCore(meshAddr) {
			slog.Info("transcoder-pool: connected to core mesh", "addr", meshAddr)
			m.refreshTranscoderWorkers(ctx)
			go m.workerRefreshLoop(ctx)
			go m.staleSweepLoop(ctx)
			return
		}
		if err := sleepContext(ctx, delay); err != nil {
			return
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
}

func (m *Module) tryDialCore(meshAddr string) bool {
	var opts []client.Option
	if meshInsecureAllowed() {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.Dial(meshAddr, opts...)
	if err != nil {
		slog.Warn("transcoder-pool: dial core failed", "error", err)
		return false
	}
	m.mu.Lock()
	if m.mc != nil {
		_ = m.mc.Close()
	}
	m.mc = c
	m.mu.Unlock()
	return true
}

func (m *Module) workerRefreshLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.refreshTranscoderWorkers(ctx)
		}
	}
}

func (m *Module) staleSweepLoop(ctx context.Context) {
	interval := time.Duration(m.staleAfterSec) * time.Second
	if interval <= 0 {
		interval = 60 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := m.store.SweepStaleWorkers(ctx); err != nil {
				slog.Warn("transcoder-pool: stale worker sweep failed", "error", err)
			} else if n > 0 {
				slog.Info("transcoder-pool: requeued jobs from stale workers", "workers", n)
			}
		}
	}
}

func (m *Module) refreshTranscoderWorkers(ctx context.Context) {
	mc := m.meshClient()
	if mc == nil || m.store == nil {
		return
	}
	seen := make(map[string]struct{})
	for _, cap := range []string{"media.transcoder", "executor.transcode"} {
		modules, err := mc.Discovery.FindByCapability(ctx, cap)
		if err != nil {
			slog.Debug("transcoder-pool: FindByCapability failed", "capability", cap, "error", err)
			continue
		}
		for _, info := range modules {
			id := info.GetId()
			if id == "" || id == m.id {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			addr := dialAddrForPeer(id, info.GetHttpAddr())
			if addr == "" {
				continue
			}
			_, err := m.store.RegisterWorker(ctx, Worker{
				ID: id, NodeID: id, GRPCAddr: addr, Capacity: 1, Labels: []string{cap},
			})
			if err != nil {
				slog.Warn("transcoder-pool: register mesh worker failed", "module", id, "error", err)
				continue
			}
			slog.Debug("transcoder-pool: registered mesh worker", "module", id, "addr", addr)
		}
	}
	if len(seen) > 0 {
		slog.Info("transcoder-pool: mesh transcoder workers refreshed", "count", len(seen))
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
