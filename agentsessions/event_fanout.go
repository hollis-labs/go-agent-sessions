package agentsessions

import (
	llmtypes "github.com/hollis-labs/go-llm-types"
)

// tryEventFanout mirrors ev to the caller-supplied typed fanout without
// ever blocking the session stream. A full channel drops the event.
func tryEventFanout(ch chan<- llmtypes.StreamEvent, ev llmtypes.StreamEvent) {
	if ch == nil {
		return
	}
	select {
	case ch <- ev:
	default:
	}
}
