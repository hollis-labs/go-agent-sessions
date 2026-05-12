package agentsessions

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/go-providers/provider"
)

// preparePlant materializes the adapter's BootDirSpec into a per-session
// tempdir when opts.AutoPlantBootDir is true and adapter implements
// provider.BootDirProvider. Returns:
//
//   - bootDir: absolute path of the planted dir, or "" if no plant happened.
//   - planted: opts with Workdir / Env / ExtraArgs rewired to the planted
//     layout. When no plant happens, planted == opts unchanged.
//   - sessionAdapter: per-session adapter (a clone with bare-mode paths
//     injected) to use for BuildArgs and downstream calls. When no plant or
//     no mutation is required, returns the input adapter unchanged.
//   - err: a render or filesystem error; the planted dir (if any) is
//     cleaned up before returning.
//
// Callers are responsible for cleanupBootDir(bootDir) at terminal state.
// runtimeID is folded into the tempdir name for diagnostic readability.
func preparePlant(opts StartOptions, adapter provider.CLIAdapter, runtimeID string) (bootDir string, planted StartOptions, sessionAdapter provider.CLIAdapter, err error) {
	planted = opts
	sessionAdapter = adapter

	if !opts.AutoPlantBootDir {
		return "", planted, sessionAdapter, nil
	}
	bp, ok := adapter.(provider.BootDirProvider)
	if !ok {
		return "", planted, sessionAdapter, nil
	}
	spec := bp.BootDirSpec()
	if len(spec.PlantedFiles) == 0 {
		return "", planted, sessionAdapter, nil
	}

	bootRoot, err := resolveBootDirRoot(opts.BootDirRoot, opts.WorkspaceDir)
	if err != nil {
		return "", opts, adapter, err
	}
	dir, err := os.MkdirTemp(bootRoot, fmt.Sprintf("agent-sessions-boot-%s-*", sanitizeBootDirID(runtimeID)))
	if err != nil {
		return "", opts, adapter, fmt.Errorf("agentsessions: create boot dir: %w", err)
	}

	projectDir := opts.Workdir
	bootContent := opts.BootContent
	if bootContent == "" {
		// Back-compat with v0.9.0–v0.9.2 callers that set only BootPrompt:
		// fall back so renderers receive the same value for both fields,
		// preserving the conflated behavior. Consumers that distinguish
		// agent persona from per-task kickoff set BootContent explicitly.
		bootContent = opts.BootPrompt
	}
	plantCtx := opts.PlantContext
	plantCtx.SystemPrompt = opts.BootPrompt
	plantCtx.BootContent = bootContent
	plantCtx.ProjectDir = projectDir
	plantCtx.BootDir = dir

	for _, pf := range spec.PlantedFiles {
		path := filepath.Join(dir, pf.RelPath)
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o750); mkErr != nil {
			_ = os.RemoveAll(dir)
			return "", opts, adapter, fmt.Errorf("agentsessions: plant %s: mkdir: %w", pf.RelPath, mkErr)
		}
		if pf.Render == nil {
			continue
		}
		content, rerr := pf.Render(plantCtx)
		if rerr != nil {
			_ = os.RemoveAll(dir)
			return "", opts, adapter, fmt.Errorf("agentsessions: plant %s: render: %w", pf.RelPath, rerr)
		}
		mode := plantedFileMode(pf)
		if werr := os.WriteFile(path, []byte(content), mode); werr != nil {
			_ = os.RemoveAll(dir)
			return "", opts, adapter, fmt.Errorf("agentsessions: plant %s: write: %w", pf.RelPath, werr)
		}
	}

	envAmend := substituteTemplates(spec.EnvAmendments, dir, projectDir)
	projectDirArg := substituteArgTokens(spec.ProjectDirArg, dir, projectDir)

	// Apply bare-mode injection for Claude adapters. Mutating the runtime-
	// level adapter would race across concurrent sessions, so clone first
	// and surface the cloned adapter back to the caller. Non-Claude or
	// non-bare adapters need no mutation; the original adapter is returned.
	sessionAdapter, projectDirArg = applyBareInjection(adapter, dir, projectDir, projectDirArg)

	planted.Workdir = spec.SpawnWorkdir(dir, projectDir)
	if len(envAmend) > 0 {
		planted.Env = append(append([]string(nil), opts.Env...), envAmend...)
	}
	if len(projectDirArg) > 0 {
		planted.ExtraArgs = append(append([]string(nil), opts.ExtraArgs...), projectDirArg...)
	}

	if opts.OnBootDirPlanted != nil {
		opts.OnBootDirPlanted(dir)
	}

	return dir, planted, sessionAdapter, nil
}

// cleanupBootDir removes a planted bootdir. Best-effort: failures are
// logged via the standard logger and never returned. Safe with empty
// input — the no-plant path passes "" and we no-op.
func cleanupBootDir(bootDir string) {
	if bootDir == "" {
		return
	}
	if err := os.RemoveAll(bootDir); err != nil {
		log.Printf("agentsessions: boot dir cleanup %s: %v", bootDir, err)
	}
}

// resolveBootDirRoot picks the parent directory for the per-session
// tempdir. Order: explicit BootDirRoot → WorkspaceDir+"/boot/" → os.TempDir().
// MkdirAll's the chosen root before returning (0o750).
func resolveBootDirRoot(explicit, workspaceDir string) (string, error) {
	var root string
	switch {
	case explicit != "":
		root = explicit
	case workspaceDir != "":
		root = filepath.Join(workspaceDir, "boot")
	default:
		return os.TempDir(), nil
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", fmt.Errorf("agentsessions: ensure boot root %s: %w", root, err)
	}
	return root, nil
}

// plantedFileMode picks the file mode for a planted file. .mcp.json and
// any *settings.json get 0o600 because they conventionally carry tokens
// or MCP loopback URLs that an attacker with read access could probe; all
// other files default to 0o644. Matches the convention the Mux/Nanite/
// Clockwork per-app planters used before absorption.
func plantedFileMode(pf provider.PlantedFile) os.FileMode {
	if pf.RelPath == ".mcp.json" || strings.HasSuffix(pf.RelPath, "settings.json") {
		return 0o600
	}
	return 0o644
}

// substituteTemplates replaces {{.BootDir}} / {{.ProjectDir}} in each
// "KEY=VALUE" env amendment. Nil/empty in → nil out so callers can
// short-circuit the append.
func substituteTemplates(in []string, bootDir, projectDir string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ReplaceAll(s, "{{.BootDir}}", bootDir)
		s = strings.ReplaceAll(s, "{{.ProjectDir}}", projectDir)
		out = append(out, s)
	}
	return out
}

// substituteArgTokens resolves a BootDirSpec.ProjectDirArg template into
// a space-tokenized argv slice. Empty template or empty projectDir
// returns nil — the spec convention is that ProjectDirArg only fires
// when there is a project to point at.
func substituteArgTokens(template, bootDir, projectDir string) []string {
	if template == "" || projectDir == "" {
		return nil
	}
	parts := strings.Fields(template)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ReplaceAll(part, "{{.BootDir}}", bootDir)
		part = strings.ReplaceAll(part, "{{.ProjectDir}}", projectDir)
		out = append(out, part)
	}
	return out
}

// applyBareInjection mutates a per-session clone of *provider.ClaudeAdapter
// when the adapter declares bare-mode, threading the planted paths into
// the adapter's MCPConfigPath / AppendSystemPromptFile / SettingsPath /
// ProjectDir fields. Returns the (possibly cloned) adapter and an updated
// projectDirArg slice (bare-mode bakes --add-dir into BuildArgs via the
// injected fields, so the runtime must NOT also splice it via ExtraArgs).
//
// Non-Claude adapters or non-bare ClaudeAdapters are returned unchanged
// along with the unaltered projectDirArg.
func applyBareInjection(adapter provider.CLIAdapter, bootDir, projectDir string, projectDirArg []string) (provider.CLIAdapter, []string) {
	claude, ok := adapter.(*provider.ClaudeAdapter)
	if !ok || !claude.Bare {
		return adapter, projectDirArg
	}
	clone := *claude
	inj := clone.BareInjectionPaths(bootDir, projectDir)
	clone.MCPConfigPath = inj.MCPConfigPath
	clone.AppendSystemPromptFile = inj.AppendSystemPromptFile
	clone.SettingsPath = inj.SettingsPath
	clone.ProjectDir = inj.ProjectDir
	// Bare BuildArgs already emits --add-dir for clone.ProjectDir; emitting
	// it again via ExtraArgs would double-add the flag.
	return &clone, nil
}

// sanitizeBootDirID restricts a runtime ID to the safe character set for
// filesystem path components. Same shape as the per-app planters used
// before absorption.
func sanitizeBootDirID(s string) string {
	if s == "" {
		return "session"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}
