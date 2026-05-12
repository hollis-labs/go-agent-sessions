//go:build !windows

package agentsessions

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
)

// scriptedBootDirAdapter satisfies CLIAdapter + BootDirProvider, with the
// binary path under the caller's control so integration tests can spin up
// a real long-lived child against the planted layout.
type scriptedBootDirAdapter struct {
	binary string
	spec   provider.BootDirSpec
}

func (a *scriptedBootDirAdapter) Name() string { return "scripted-bootdir" }
func (a *scriptedBootDirAdapter) BuildArgs(prompt, systemPrompt, cliSessionID string) []string {
	return nil
}
func (a *scriptedBootDirAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	return nil, nil
}
func (a *scriptedBootDirAdapter) Detect() (string, bool)            { return a.binary, a.binary != "" }
func (a *scriptedBootDirAdapter) BootDirSpec() provider.BootDirSpec { return a.spec }

func newPlantThroughStreamingSession(t *testing.T, root, workspace string, onPlanted func(string)) (Session, *scriptedBootDirAdapter) {
	t.Helper()
	script := writeNoopStdinScript(t, root)
	adapter := &scriptedBootDirAdapter{
		binary: script,
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: staticContent("hello")},
				{RelPath: ".mcp.json", Render: staticContent("{}")},
			},
			EnvAmendments: []string{"PLANTED_BOOT={{.BootDir}}"},
		},
	}
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "scripted-stream",
		Kind:    "cli",
		Adapter: adapter,
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:          workspace,
		LogPath:          filepath.Join(workspace, "session.log"),
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		OnBootDirPlanted: onPlanted,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return sess, adapter
}

func TestAutoPlantBootDir_Runtime_PlantedFilesExistDuringSession(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	var planted string
	sess, _ := newPlantThroughStreamingSession(t, root, workspace, func(p string) { planted = p })
	defer func() { _ = sess.Stop(context.Background()) }()

	if planted == "" {
		t.Fatal("OnBootDirPlanted never fired")
	}
	if _, err := os.Stat(filepath.Join(planted, "CLAUDE.md")); err != nil {
		t.Errorf("planted CLAUDE.md missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(planted, ".mcp.json")); err != nil {
		t.Errorf("planted .mcp.json missing: %v", err)
	}
}

func TestAutoPlantBootDir_Runtime_CleanupOnStop(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	var planted string
	sess, _ := newPlantThroughStreamingSession(t, root, workspace, func(p string) { planted = p })

	if planted == "" {
		t.Fatal("OnBootDirPlanted never fired")
	}
	if _, err := os.Stat(planted); err != nil {
		t.Fatalf("bootDir should exist mid-session: %v", err)
	}

	if err := sess.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := sess.Wait(); err != nil {
		t.Logf("Wait err (expected after Stop): %v", err)
	}

	if !waitFor(2*time.Second, func() bool {
		_, err := os.Stat(planted)
		return os.IsNotExist(err)
	}) {
		t.Errorf("bootDir %q still exists after Stop+Wait", planted)
	}
}

func TestAutoPlantBootDir_Runtime_CleanupOnCleanExit(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	// Script that emits one line then exits cleanly — drives the
	// session's reader to EOF and the waiter goroutine to finalize
	// without explicit Stop.
	script := filepath.Join(root, "quick-exit.sh")
	body := `#!/bin/sh
printf '%s\n' '{"type":"hello"}'
exit 0
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	adapter := &scriptedBootDirAdapter{
		binary: script,
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "marker", Render: staticContent("m")},
			},
		},
	}
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "scripted-clean-exit",
		Kind:    "cli",
		Adapter: adapter,
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	var planted string
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:          workspace,
		LogPath:          filepath.Join(workspace, "session.log"),
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		OnBootDirPlanted: func(p string) { planted = p },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()
	if _, err := sess.Wait(); err != nil {
		t.Logf("Wait err: %v", err)
	}

	if planted == "" {
		t.Fatal("OnBootDirPlanted never fired")
	}
	if !waitFor(2*time.Second, func() bool {
		_, err := os.Stat(planted)
		return os.IsNotExist(err)
	}) {
		t.Errorf("bootDir %q still exists after clean exit", planted)
	}
}

func TestAutoPlantBootDir_Runtime_ChildSeesPlantedEnv(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	// Script that writes its PLANTED_BOOT env to a sidecar file then
	// reads stdin until EOF — exercises EnvAmendments threading.
	witnessPath := filepath.Join(workspace, "witness")
	script := filepath.Join(root, "env-witness.sh")
	body := `#!/bin/sh
printf '%s' "$PLANTED_BOOT" > "` + witnessPath + `"
while IFS= read -r _; do :; done
exit 0
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	adapter := &scriptedBootDirAdapter{
		binary: script,
		spec: provider.BootDirSpec{
			PlantedFiles:  []provider.PlantedFile{{RelPath: "m", Render: staticContent("x")}},
			EnvAmendments: []string{"PLANTED_BOOT={{.BootDir}}"},
		},
	}
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "env-witness",
		Kind:    "cli",
		Adapter: adapter,
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	var planted string
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:          workspace,
		LogPath:          filepath.Join(workspace, "session.log"),
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		OnBootDirPlanted: func(p string) { planted = p },
		Env:              os.Environ(), // need PATH for the script's shebang to resolve sh
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if !waitFor(2*time.Second, func() bool {
		_, err := os.Stat(witnessPath)
		return err == nil
	}) {
		t.Fatal("witness file never written — child probably didn't see PLANTED_BOOT")
	}
	got, err := os.ReadFile(witnessPath)
	if err != nil {
		t.Fatalf("read witness: %v", err)
	}
	if string(got) != planted {
		t.Errorf("witness = %q, want planted bootDir %q", got, planted)
	}
}

func TestAutoPlantBootDir_Runtime_FlagOff_NoFilesystemActivity(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	script := writeNoopStdinScript(t, root)
	adapter := &scriptedBootDirAdapter{
		binary: script,
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{{RelPath: "should-not-exist", Render: staticContent("x")}},
		},
	}
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "flag-off",
		Kind:    "cli",
		Adapter: adapter,
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: workspace,
		LogPath: filepath.Join(workspace, "session.log"),
		// AutoPlantBootDir intentionally not set.
		BootDirRoot: root,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	// Nothing should have been planted under root other than the script.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	for _, e := range entries {
		if e.Name() == filepath.Base(script) || e.Name() == filepath.Base(script)+".log" {
			continue
		}
		t.Errorf("unexpected entry %s in root — plant ran with flag off", e.Name())
	}
}
