package agentsessions

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
)

// fakeBootDirAdapter satisfies both provider.CLIAdapter and
// provider.BootDirProvider, with a spec the tests drive directly.
type fakeBootDirAdapter struct {
	name string
	spec provider.BootDirSpec
}

func (a *fakeBootDirAdapter) Name() string { return a.name }
func (a *fakeBootDirAdapter) BuildArgs(prompt, systemPrompt, cliSessionID string) []string {
	return nil
}
func (a *fakeBootDirAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) { return nil, nil }
func (a *fakeBootDirAdapter) Detect() (string, bool)                                { return "/bin/true", true }
func (a *fakeBootDirAdapter) BootDirSpec() provider.BootDirSpec                     { return a.spec }

// fakePlainAdapter implements CLIAdapter only — used to confirm the
// helper no-ops for adapters that don't surface a BootDirSpec.
type fakePlainAdapter struct{}

func (fakePlainAdapter) Name() string                                                 { return "plain" }
func (fakePlainAdapter) BuildArgs(prompt, systemPrompt, cliSessionID string) []string { return nil }
func (fakePlainAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error)        { return nil, nil }
func (fakePlainAdapter) Detect() (string, bool)                                       { return "/bin/true", true }

func TestPreparePlant_Disabled_NoFilesystemActivity(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: staticContent("never")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: false,
		BootDirRoot:      root,
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	if bootDir != "" {
		t.Errorf("bootDir = %q, want empty (flag off)", bootDir)
	}
	if sessionAdapter != adapter {
		t.Errorf("sessionAdapter should be the input adapter when flag off")
	}
	if planted.Workdir != opts.Workdir {
		t.Errorf("planted.Workdir = %q, want %q", planted.Workdir, opts.Workdir)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Errorf("root has %d entries, want 0 (no plant)", len(entries))
	}
}

func TestPreparePlant_AdapterWithoutBootDirProvider_NoOp(t *testing.T) {
	root := t.TempDir()
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, fakePlainAdapter{}, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	if bootDir != "" {
		t.Errorf("bootDir = %q, want empty (adapter has no BootDirProvider)", bootDir)
	}
	if _, ok := sessionAdapter.(fakePlainAdapter); !ok {
		t.Errorf("sessionAdapter should pass through unchanged")
	}
	if planted.Workdir != opts.Workdir {
		t.Errorf("planted.Workdir = %q, want %q", planted.Workdir, opts.Workdir)
	}
}

func TestPreparePlant_EmptyPlantedFiles_NoOp(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "empty",
		spec: provider.BootDirSpec{}, // no PlantedFiles
	}
	opts := StartOptions{
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		Workdir:          "/tmp/proj",
	}
	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	if bootDir != "" {
		t.Errorf("bootDir = %q, want empty (spec has no PlantedFiles)", bootDir)
	}
}

func TestPreparePlant_PlantedFilesWritten(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: staticContent("hello")},
				{RelPath: ".claude/settings.json", Render: staticContent("{\"x\":1}")},
				{RelPath: ".mcp.json", Render: staticContent("{}")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "boot content",
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "claudestream")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	if bootDir == "" {
		t.Fatal("bootDir empty")
	}
	if !strings.Contains(filepath.Base(bootDir), "claudestream") {
		t.Errorf("bootDir basename %q should embed runtime ID", filepath.Base(bootDir))
	}

	for _, tc := range []struct {
		rel  string
		want string
		mode os.FileMode
	}{
		{"CLAUDE.md", "hello", 0o644},
		{".claude/settings.json", "{\"x\":1}", 0o600},
		{".mcp.json", "{}", 0o600},
	} {
		path := filepath.Join(bootDir, tc.rel)
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", tc.rel, err)
		}
		if string(got) != tc.want {
			t.Errorf("%s content = %q, want %q", tc.rel, got, tc.want)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", tc.rel, err)
		}
		if info.Mode().Perm() != tc.mode {
			t.Errorf("%s mode = %o, want %o", tc.rel, info.Mode().Perm(), tc.mode)
		}
	}

	// Cleanup is the caller's job; verify the dir still exists.
	if _, err := os.Stat(bootDir); err != nil {
		t.Errorf("bootDir should exist after plant: %v", err)
	}
	cleanupBootDir(bootDir)
	if _, err := os.Stat(bootDir); !os.IsNotExist(err) {
		t.Errorf("cleanupBootDir should remove bootDir; stat err = %v", err)
	}
}

func TestPreparePlant_EnvAmendmentsApplied(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "marker", Render: staticContent("x")},
			},
			EnvAmendments: []string{
				"AGENT_BOOT_DIR={{.BootDir}}",
				"AGENT_PROJECT={{.ProjectDir}}",
				"AGENT_LITERAL=value",
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		Env:              []string{"PRESET=1"},
	}

	bootDir, planted, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	want := []string{
		"PRESET=1",
		"AGENT_BOOT_DIR=" + bootDir,
		"AGENT_PROJECT=/tmp/proj",
		"AGENT_LITERAL=value",
	}
	if len(planted.Env) != len(want) {
		t.Fatalf("planted.Env length %d, want %d (%v)", len(planted.Env), len(want), planted.Env)
	}
	for i, w := range want {
		if planted.Env[i] != w {
			t.Errorf("planted.Env[%d] = %q, want %q", i, planted.Env[i], w)
		}
	}

	// Confirm opts.Env wasn't mutated (helper copies the slice).
	if len(opts.Env) != 1 || opts.Env[0] != "PRESET=1" {
		t.Errorf("opts.Env mutated: %v", opts.Env)
	}
}

func TestPreparePlant_ProjectDirArgThreaded(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles:  []provider.PlantedFile{{RelPath: "marker", Render: staticContent("x")}},
			ProjectDirArg: "--add-dir {{.ProjectDir}}",
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		ExtraArgs:        []string{"--preset"},
	}

	bootDir, planted, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	want := []string{"--preset", "--add-dir", "/tmp/proj"}
	if len(planted.ExtraArgs) != len(want) {
		t.Fatalf("planted.ExtraArgs = %v, want %v", planted.ExtraArgs, want)
	}
	for i, w := range want {
		if planted.ExtraArgs[i] != w {
			t.Errorf("planted.ExtraArgs[%d] = %q, want %q", i, planted.ExtraArgs[i], w)
		}
	}
}

func TestPreparePlant_ProjectDirArg_EmptyProjectDir_NoSplice(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles:  []provider.PlantedFile{{RelPath: "marker", Render: staticContent("x")}},
			ProjectDirArg: "--add-dir {{.ProjectDir}}",
		},
	}
	opts := StartOptions{
		// Workdir intentionally empty — no project to point at.
		AutoPlantBootDir: true,
		BootDirRoot:      root,
	}
	bootDir, planted, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)
	if len(planted.ExtraArgs) != 0 {
		t.Errorf("planted.ExtraArgs = %v, want empty (no project)", planted.ExtraArgs)
	}
}

func TestPreparePlant_SpawnCwdSet_BootDir(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles:  []provider.PlantedFile{{RelPath: "marker", Render: staticContent("x")}},
			CwdPreference: provider.CwdBootDir,
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
	}
	bootDir, planted, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)
	if planted.Workdir != bootDir {
		t.Errorf("planted.Workdir = %q, want %q (bootDir for CwdBootDir)", planted.Workdir, bootDir)
	}
}

func TestPreparePlant_SpawnCwdSet_ProjectDir(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles:  []provider.PlantedFile{{RelPath: "marker", Render: staticContent("x")}},
			CwdPreference: provider.CwdProjectDir,
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
	}
	bootDir, planted, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)
	if planted.Workdir != "/tmp/proj" {
		t.Errorf("planted.Workdir = %q, want /tmp/proj (CwdProjectDir)", planted.Workdir)
	}
}

func TestPreparePlant_RenderFailure_CleansUp(t *testing.T) {
	root := t.TempDir()
	errBoom := errors.New("boom")
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "ok", Render: staticContent("ok")},
				{RelPath: "bad", Render: func(provider.PlantContext) (string, error) { return "", errBoom }},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want wrap of %v", err, errBoom)
	}
	if bootDir != "" {
		t.Errorf("bootDir = %q, want empty on render failure", bootDir)
	}
	// No tempdir should be left behind.
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Errorf("root has %d leftover entries, want 0", len(entries))
	}
}

func TestPreparePlant_OnBootDirPlantedFires(t *testing.T) {
	root := t.TempDir()
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{{RelPath: "marker", Render: staticContent("x")}},
		},
	}
	var seen string
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		OnBootDirPlanted: func(p string) { seen = p },
	}
	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)
	if seen != bootDir {
		t.Errorf("OnBootDirPlanted saw %q, want %q", seen, bootDir)
	}
}

func TestPreparePlant_BareModeInjection_ClaudeAdapter(t *testing.T) {
	root := t.TempDir()
	adapter := provider.NewClaudeAdapterDevBare()
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "boot content",
		Env:              []string{"HOME=" + t.TempDir()}, // for trust-seed render side effect
	}
	t.Setenv("HOME", t.TempDir())

	bootDir, planted, sessionAdapter, err := preparePlant(opts, adapter, "claude")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	// Clone identity: sessionAdapter must not be the input adapter
	// (mutating in place would race across concurrent sessions).
	clone, ok := sessionAdapter.(*provider.ClaudeAdapter)
	if !ok {
		t.Fatalf("sessionAdapter type = %T, want *provider.ClaudeAdapter", sessionAdapter)
	}
	if clone == adapter {
		t.Error("sessionAdapter is the runtime-level adapter — should be a clone")
	}

	// Bare-mode injection: paths point inside bootDir.
	if !strings.HasPrefix(clone.MCPConfigPath, bootDir) {
		t.Errorf("MCPConfigPath = %q, should start with bootDir %q", clone.MCPConfigPath, bootDir)
	}
	if !strings.HasPrefix(clone.AppendSystemPromptFile, bootDir) {
		t.Errorf("AppendSystemPromptFile = %q, should start with bootDir %q", clone.AppendSystemPromptFile, bootDir)
	}
	if !strings.HasPrefix(clone.SettingsPath, bootDir) {
		t.Errorf("SettingsPath = %q, should start with bootDir %q", clone.SettingsPath, bootDir)
	}
	if clone.ProjectDir != "/tmp/proj" {
		t.Errorf("clone.ProjectDir = %q, want /tmp/proj", clone.ProjectDir)
	}

	// Bare mode bakes --add-dir into BuildArgs via the injected fields;
	// ExtraArgs must NOT also splice it (double-add guard).
	if len(planted.ExtraArgs) != 0 {
		t.Errorf("bare-mode planted.ExtraArgs = %v, want empty (BuildArgs owns --add-dir)", planted.ExtraArgs)
	}

	// Verify the original adapter wasn't mutated.
	if adapter.MCPConfigPath != "" || adapter.AppendSystemPromptFile != "" || adapter.SettingsPath != "" || adapter.ProjectDir != "" {
		t.Errorf("runtime-level adapter was mutated; clone identity broken")
	}
}

func TestResolveBootDirRoot_Explicit(t *testing.T) {
	want := t.TempDir()
	got, err := resolveBootDirRoot(want, "/should/be/ignored")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveBootDirRoot_WorkspaceFallback(t *testing.T) {
	ws := t.TempDir()
	got, err := resolveBootDirRoot("", ws)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := filepath.Join(ws, "boot")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if _, err := os.Stat(want); err != nil {
		t.Errorf("boot dir was not created: %v", err)
	}
}

func TestResolveBootDirRoot_TmpFallback(t *testing.T) {
	got, err := resolveBootDirRoot("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != os.TempDir() {
		t.Errorf("got %q, want os.TempDir() %q", got, os.TempDir())
	}
}

func TestSanitizeBootDirID(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{"", "session"},
		{"claude-stream", "claude-stream"},
		{"app/server", "app_server"},
		{"foo.bar baz", "foo_bar_baz"},
	} {
		if got := sanitizeBootDirID(tc.in); got != tc.want {
			t.Errorf("sanitizeBootDirID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func staticContent(s string) func(provider.PlantContext) (string, error) {
	return func(provider.PlantContext) (string, error) { return s, nil }
}

// captureContext returns a renderer that records every PlantContext passed
// to it into the supplied slice (in PlantedFile order). Used to assert what
// the lib actually feeds the per-provider Render closures, not just what the
// final files contain.
func captureContext(out *[]provider.PlantContext, body string) func(provider.PlantContext) (string, error) {
	return func(ctx provider.PlantContext) (string, error) {
		*out = append(*out, ctx)
		return body, nil
	}
}

// TestPreparePlant_BootContent_Distinct_FromBootPrompt confirms that when
// callers set BootPrompt and BootContent to different values, the renderer
// receives them as distinct PlantContext fields. This is the load-bearing
// behavior for consumers (clockwork) that distinguish persona context
// (CLAUDE.md / AGENTS.md) from per-task kickoff (boot.md).
func TestPreparePlant_BootContent_Distinct_FromBootPrompt(t *testing.T) {
	root := t.TempDir()
	var captured []provider.PlantContext
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: captureContext(&captured, "claude-md-body")},
				{RelPath: "boot.md", Render: captureContext(&captured, "boot-md-body")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "persona-system-prompt",
		BootContent:      "per-task-kickoff",
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	if len(captured) != 2 {
		t.Fatalf("captured %d contexts, want 2", len(captured))
	}
	for i, ctx := range captured {
		if ctx.SystemPrompt != "persona-system-prompt" {
			t.Errorf("captured[%d].SystemPrompt = %q, want %q", i, ctx.SystemPrompt, "persona-system-prompt")
		}
		if ctx.BootContent != "per-task-kickoff" {
			t.Errorf("captured[%d].BootContent = %q, want %q", i, ctx.BootContent, "per-task-kickoff")
		}
	}
}

// TestPreparePlant_BootContent_Empty_FallsBack_To_BootPrompt confirms back-
// compat with v0.9.0–v0.9.2 callers (Mux et al) that conflate persona and
// kickoff into a single BootPrompt value. When BootContent is empty, the
// renderer must see BootPrompt for both PlantContext.SystemPrompt and
// PlantContext.BootContent — preserving the conflated behavior exactly.
func TestPreparePlant_BootContent_Empty_FallsBack_To_BootPrompt(t *testing.T) {
	root := t.TempDir()
	var captured []provider.PlantContext
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: captureContext(&captured, "x")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "single-conflated-prompt",
		// BootContent intentionally unset.
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	if len(captured) != 1 {
		t.Fatalf("captured %d contexts, want 1", len(captured))
	}
	if captured[0].SystemPrompt != "single-conflated-prompt" {
		t.Errorf("captured[0].SystemPrompt = %q, want %q", captured[0].SystemPrompt, "single-conflated-prompt")
	}
	if captured[0].BootContent != "single-conflated-prompt" {
		t.Errorf("captured[0].BootContent = %q, want %q (fallback)", captured[0].BootContent, "single-conflated-prompt")
	}
}

func TestPreparePlant_PlantContextOverlay_FlowsThrough_To_Renderer(t *testing.T) {
	root := t.TempDir()
	var captured []provider.PlantContext
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: captureContext(&captured, "x")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "persona-system-prompt",
		BootContent:      "per-task-kickoff",
		PlantContext: provider.PlantContext{
			AgentName:      "executor",
			MCPLoopbackURL: "http://127.0.0.1:54321/mcp",
			MuxCommand:     "/usr/local/bin/mux",
			MuxArgs:        []string{"mcp", "--proxy"},
			MuxEnv:         []string{"MUX_TOKEN=local-dev"},
		},
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	if len(captured) != 1 {
		t.Fatalf("captured %d contexts, want 1", len(captured))
	}
	ctx := captured[0]
	if ctx.AgentName != "executor" {
		t.Errorf("AgentName = %q, want %q", ctx.AgentName, "executor")
	}
	if ctx.MCPLoopbackURL != "http://127.0.0.1:54321/mcp" {
		t.Errorf("MCPLoopbackURL = %q, want %q", ctx.MCPLoopbackURL, "http://127.0.0.1:54321/mcp")
	}
	if ctx.MuxCommand != "/usr/local/bin/mux" {
		t.Errorf("MuxCommand = %q, want %q", ctx.MuxCommand, "/usr/local/bin/mux")
	}
	if got, want := strings.Join(ctx.MuxArgs, "\x00"), strings.Join([]string{"mcp", "--proxy"}, "\x00"); got != want {
		t.Errorf("MuxArgs = %#v, want %#v", ctx.MuxArgs, []string{"mcp", "--proxy"})
	}
	if got, want := strings.Join(ctx.MuxEnv, "\x00"), "MUX_TOKEN=local-dev"; got != want {
		t.Errorf("MuxEnv = %#v, want %#v", ctx.MuxEnv, []string{"MUX_TOKEN=local-dev"})
	}
}

func TestPreparePlant_PlantContextOverlay_LibFieldsOverridden(t *testing.T) {
	root := t.TempDir()
	var captured []provider.PlantContext
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: captureContext(&captured, "x")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "persona-system-prompt",
		BootContent:      "per-task-kickoff",
		PlantContext: provider.PlantContext{
			SystemPrompt: "caller-set-system",
			BootContent:  "caller-set-boot",
			ProjectDir:   "/caller/project",
			BootDir:      "/caller/boot",
		},
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	if len(captured) != 1 {
		t.Fatalf("captured %d contexts, want 1", len(captured))
	}
	ctx := captured[0]
	if ctx.SystemPrompt != "persona-system-prompt" {
		t.Errorf("SystemPrompt = %q, want %q", ctx.SystemPrompt, "persona-system-prompt")
	}
	if ctx.BootContent != "per-task-kickoff" {
		t.Errorf("BootContent = %q, want %q", ctx.BootContent, "per-task-kickoff")
	}
	if ctx.ProjectDir != "/tmp/proj" {
		t.Errorf("ProjectDir = %q, want %q", ctx.ProjectDir, "/tmp/proj")
	}
	if ctx.BootDir != bootDir {
		t.Errorf("BootDir = %q, want planted bootDir %q", ctx.BootDir, bootDir)
	}
}

func TestPreparePlant_PlantContextOverlay_Empty_NoChange(t *testing.T) {
	root := t.TempDir()
	var captured []provider.PlantContext
	adapter := &fakeBootDirAdapter{
		name: "fake",
		spec: provider.BootDirSpec{
			PlantedFiles: []provider.PlantedFile{
				{RelPath: "CLAUDE.md", Render: captureContext(&captured, "x")},
			},
		},
	}
	opts := StartOptions{
		Workdir:          "/tmp/proj",
		AutoPlantBootDir: true,
		BootDirRoot:      root,
		BootPrompt:       "single-conflated-prompt",
	}

	bootDir, _, _, err := preparePlant(opts, adapter, "test")
	if err != nil {
		t.Fatalf("preparePlant err: %v", err)
	}
	defer cleanupBootDir(bootDir)

	if len(captured) != 1 {
		t.Fatalf("captured %d contexts, want 1", len(captured))
	}
	ctx := captured[0]
	if ctx.SystemPrompt != "single-conflated-prompt" {
		t.Errorf("SystemPrompt = %q, want %q", ctx.SystemPrompt, "single-conflated-prompt")
	}
	if ctx.BootContent != "single-conflated-prompt" {
		t.Errorf("BootContent = %q, want %q", ctx.BootContent, "single-conflated-prompt")
	}
	if ctx.ProjectDir != "/tmp/proj" {
		t.Errorf("ProjectDir = %q, want %q", ctx.ProjectDir, "/tmp/proj")
	}
	if ctx.BootDir != bootDir {
		t.Errorf("BootDir = %q, want planted bootDir %q", ctx.BootDir, bootDir)
	}
	if ctx.AgentName != "" || ctx.MCPLoopbackURL != "" || ctx.MuxCommand != "" || len(ctx.MuxArgs) != 0 || len(ctx.MuxEnv) != 0 {
		t.Errorf("caller-owned fields should stay zero-valued: %#v", ctx)
	}
}
