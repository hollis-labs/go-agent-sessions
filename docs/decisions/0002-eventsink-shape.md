# 0002 — EventSink emits only Manager-owned lifecycle events

**Status:** accepted (v0.1.0)
**Question (from prompt):** Should EventSink flatten go-runner events +
provider events, or expose them as separate channels?

## Decision

The library's `EventSink` receives **only Manager-owned lifecycle
events** (state transitions). Per-turn provider events (parsed
`provider.StreamEvent` from a Runtime, or `runner.Event` from the
underlying spawn) are **not** mirrored to EventSink. They flow through
the existing `StartOptions.Fanout` writer, and from there to attach
subscribers via the broker.

## Why

- Conflating Manager lifecycle events with adapter event streams gives
  the library two unrelated jobs in one interface. Each consumer would
  then have to filter, and at least one consumer would discover its own
  ordering bug (lifecycle events flushed in a different goroutine than
  adapter events).
- Mux ships this exact split today: state-changed events go to
  `events.Publisher`; PTY/stream bytes go through Fanout. Consumers
  that need the parsed event detail keep their own pipeline.
- Provider events have rich, adapter-specific shapes (`tool_use`,
  `usage`, `session_id`). Flattening into a generic envelope loses
  fidelity; replicating each shape into the library expands the public
  surface for limited gain.

## How to apply

- Consumers that want the bytes-as-stream view: use
  `Manager.AttachWith` (broker is already there).
- Consumers that want each parsed `provider.StreamEvent` as a
  structured event: use `StartOptions.EventFanout`. The byte
  `Fanout` remains available in parallel for raw streaming.
- The library reserves the right to add **purely Manager-owned** event
  kinds to `LifecycleEventKind` in future versions (e.g.
  `session.attached`, `session.health_degraded`). The SessionID +
  RuntimeID + Kind shape on `LifecycleEvent` keeps the door open
  without disturbing the no-flatten rule.
