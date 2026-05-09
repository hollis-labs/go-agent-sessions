package compliance_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/hollis-labs/go-agent-sessions/agentsessions"
	"github.com/hollis-labs/go-agent-sessions/compliance"
	llmtypes "github.com/hollis-labs/go-llm-types"
)

// echoAdapter mirrors the test fixture in the agentsessions package — a
// minimal CLIAdapter wrapping a shell script that emits stream-json-like
// lines. Duplicated here (rather than exported) to keep the public
// agentsessions surface minimal.
type echoAdapter struct{ script string }

func (a *echoAdapter) Name() string { return "echo-test" }
func (a *echoAdapter) BuildArgs(_, _, _ string) []string {
	return []string{}
}
func (a *echoAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case strings.HasPrefix(s, "delta:"):
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: strings.TrimPrefix(s, "delta:")}}, nil
	case strings.HasPrefix(s, "session:"):
		return []llmtypes.StreamEvent{{Type: llmtypes.EventSessionID, SessionID: strings.TrimPrefix(s, "session:")}}, nil
	case s == "done":
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDone}}, nil
	}
	return nil, nil
}
func (a *echoAdapter) Detect() (string, bool) { return a.script, a.script != "" }

func writeScript(t *testing.T, dir string, lines []string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("requires sh")
	}
	path := filepath.Join(dir, "fake-cli.sh")
	body := "#!/bin/sh\n"
	for _, l := range lines {
		body += "printf '%s\\n' '" + strings.ReplaceAll(l, "'", "'\\''") + "'\n"
	}
	body += "exit 0\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// TestComplianceHarnessRunsGreen drives the harness against an adapter
// runtime backed by a fake CLI script. Confirms the harness compiles
// and every baseline test passes against a well-behaved runtime.
func TestComplianceHarnessRunsGreen(t *testing.T) {
	dir := t.TempDir()
	script := writeScript(t, dir, []string{
		"session:ses_compliance_preset",
		"done",
	})

	rt, err := agentsessions.NewFromAdapter(agentsessions.AdapterRuntimeConfig{
		ID:      "compliance-echo",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps: agentsessions.Capabilities{
			ProviderSessionID: true,
			BinaryRequired:    true,
			CheckpointResume:  true,
		},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	compliance.Run(t, compliance.Harness{
		Runtime: rt,
		NewStartOptions: func(t *testing.T) agentsessions.StartOptions {
			return agentsessions.StartOptions{
				Workdir:         dir,
				SessionIDPreset: "ses_compliance_preset",
			}
		},
	})
}
