package agentsessions

import "github.com/hollis-labs/go-providers/provider"

// tryEventFanout mirrors ev to the caller-supplied typed fanout without
// ever blocking the session stream. A full channel drops the event.
func tryEventFanout(ch chan<- provider.StreamEvent, ev provider.StreamEvent) {
	if ch == nil {
		return
	}
	select {
	case ch <- ev:
	default:
	}
}
