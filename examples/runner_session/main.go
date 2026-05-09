// Example: a long-lived agent Session driven by go-runner with a
// CLIAdapter, registered with the Manager so attach subscribers can
// stream output live.
//
// This example uses /bin/echo as a stand-in for a real CLI provider.
// Replace the adapter with your real one (claudestream, opencode, ...);
// the rest of the wiring is the same.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hollis-labs/go-agent-sessions/agentsessions"
	llmtypes "github.com/hollis-labs/go-llm-types"
)

// echoAdapter is a stand-in CLIAdapter for the example. It treats
// stdout lines as opaque text and emits a single delta per line.
type echoAdapter struct{}

func (echoAdapter) Name() string                           { return "echo" }
func (echoAdapter) BuildArgs(prompt, _, _ string) []string { return []string{prompt} }
func (echoAdapter) Detect() (string, bool)                 { return "/bin/echo", true }
func (echoAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	s := strings.TrimRight(string(line), "\r\n")
	return []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: s}}, nil
}

func main() {
	rt, err := agentsessions.NewFromAdapter(agentsessions.AdapterRuntimeConfig{
		ID:      "echo",
		Kind:    "cli",
		Adapter: echoAdapter{},
		Caps:    agentsessions.Capabilities{BinaryRequired: true},
	})
	if err != nil {
		log.Fatal(err)
	}

	m := agentsessions.NewManager(nil) // no state persistence in this example

	if err := m.Start(context.Background(), agentsessions.StartRequest{
		ID:      "demo-1",
		Runtime: rt,
		Options: agentsessions.StartOptions{
			Workdir:       ".",
			AttachEnabled: true,
		},
	}); err != nil {
		log.Fatal(err)
	}

	// Attach to the live byte stream.
	go func() {
		if err := m.Attach(context.Background(), "demo-1", os.Stdout); err != nil {
			log.Printf("attach: %v", err)
		}
	}()

	// Drive a turn.
	if err := m.SendInput("demo-1", []byte("hello from go-agent-sessions")); err != nil {
		log.Fatal(err)
	}

	// In a real consumer, you'd loop on SendInput driven by user/agent
	// input. For the example, stop right after the first turn.
	if err := m.Stop(context.Background(), "demo-1"); err != nil {
		log.Fatal(err)
	}
	code, err := m.WaitSession(context.Background(), "demo-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nsession exit code: %d\n", code)
}
