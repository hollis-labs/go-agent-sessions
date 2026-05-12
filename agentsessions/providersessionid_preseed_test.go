//go:build !windows

package agentsessions

import (
	"context"
	"path/filepath"
	"testing"
)

// TestProviderSessionID_PresetCarriedBeforeTurn — regression guard for the
// latent bug across the three long-lived runtime kinds (pty / streaming-
// stdio / jsonrpc-stdio): each returned the empty string from
// ProviderSessionID() between Start and the first observed session-id
// event, because lastSessionID was never seeded from StartOptions.
// SessionIDPreset at Start. The compliance harness's
// CapsProviderSessionID/PresetCarriedBeforeTurn assertion requires the
// preset to be visible *before any turn fires*; this test pins the
// invariant at the lib level so callers don't depend on running compliance
// against their own integration to catch it.
//
// adapterSession (subprocess-per-turn) was already correct
// (from_adapter.go: s.sessionID.Store(opts.SessionIDPreset) at Start);
// the three long-lived kinds now match.
func TestProviderSessionID_PresetCarriedBeforeTurn(t *testing.T) {
	const preset = "ses_v091_preset"

	t.Run("streamingStdio", func(t *testing.T) {
		dir := t.TempDir()
		script := writeNoopStdinScript(t, dir)
		rt, err := NewFromAdapter(AdapterRuntimeConfig{
			ID:      "preseed-streaming",
			Kind:    "cli",
			Adapter: &minimalAdapter{binary: script},
			Caps:    Capabilities{StreamingStdio: true, ProviderSessionID: true, BinaryRequired: true},
		})
		if err != nil {
			t.Fatalf("NewFromAdapter: %v", err)
		}
		sess, err := rt.Start(context.Background(), StartOptions{
			Workdir:         dir,
			LogPath:         filepath.Join(dir, "session.log"),
			SessionIDPreset: preset,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = sess.Stop(context.Background()) }()
		sider, ok := sess.(SessionIDer)
		if !ok {
			t.Fatal("session does not implement SessionIDer")
		}
		if got := sider.ProviderSessionID(); got != preset {
			t.Errorf("ProviderSessionID() = %q before any turn; want %q", got, preset)
		}
	})

	t.Run("jsonRpcStdio", func(t *testing.T) {
		dir := t.TempDir()
		script := writeJsonRpcSilentScript(t, dir)
		rt, err := NewFromAdapter(AdapterRuntimeConfig{
			ID:      "preseed-jsonrpc",
			Kind:    "cli",
			Adapter: &minimalAdapter{binary: script},
			Caps:    Capabilities{JsonRpcStdio: true, ProviderSessionID: true, BinaryRequired: true},
		})
		if err != nil {
			t.Fatalf("NewFromAdapter: %v", err)
		}
		sess, err := rt.Start(context.Background(), StartOptions{
			Workdir:         dir,
			LogPath:         filepath.Join(dir, "session.log"),
			SessionIDPreset: preset,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = sess.Stop(context.Background()) }()
		sider, ok := sess.(SessionIDer)
		if !ok {
			t.Fatal("session does not implement SessionIDer")
		}
		if got := sider.ProviderSessionID(); got != preset {
			t.Errorf("ProviderSessionID() = %q before any turn; want %q", got, preset)
		}
	})

	t.Run("pty", func(t *testing.T) {
		dir := t.TempDir()
		script := writeNoopStdinScript(t, dir)
		rt, err := NewFromAdapter(AdapterRuntimeConfig{
			ID:      "preseed-pty",
			Kind:    "cli",
			Adapter: &minimalAdapter{binary: script},
			Caps:    Capabilities{PTY: true, ProviderSessionID: true, BinaryRequired: true},
		})
		if err != nil {
			t.Fatalf("NewFromAdapter: %v", err)
		}
		sess, err := rt.Start(context.Background(), StartOptions{
			Workdir:         dir,
			LogPath:         filepath.Join(dir, "session.log"),
			SessionIDPreset: preset,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = sess.Stop(context.Background()) }()
		sider, ok := sess.(SessionIDer)
		if !ok {
			t.Fatal("session does not implement SessionIDer")
		}
		if got := sider.ProviderSessionID(); got != preset {
			t.Errorf("ProviderSessionID() = %q before any turn; want %q", got, preset)
		}
	})

	t.Run("adapter", func(t *testing.T) {
		// adapterSession already does the right thing — this subtest is the
		// parity check that the lib-level invariant is symmetric across all
		// four runtime kinds.
		rt, err := NewFromAdapter(AdapterRuntimeConfig{
			ID:      "preseed-adapter",
			Kind:    "cli",
			Adapter: &minimalAdapter{binary: "/bin/true"},
			Caps:    Capabilities{ProviderSessionID: true},
		})
		if err != nil {
			t.Fatalf("NewFromAdapter: %v", err)
		}
		sess, err := rt.Start(context.Background(), StartOptions{
			Workdir:         t.TempDir(),
			SessionIDPreset: preset,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = sess.Stop(context.Background()) }()
		sider, ok := sess.(SessionIDer)
		if !ok {
			t.Fatal("session does not implement SessionIDer")
		}
		if got := sider.ProviderSessionID(); got != preset {
			t.Errorf("ProviderSessionID() = %q before any turn; want %q", got, preset)
		}
	})
}
