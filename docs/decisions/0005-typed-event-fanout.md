# 0005 — Typed EventFanout mirrors parsed provider events

**Status:** accepted (v0.2.0)
**Question:** How should consumers receive parsed `provider.StreamEvent`
values without re-parsing the byte `Fanout` stream?

## Decision

Add `StartOptions.EventFanout chan<- provider.StreamEvent` as an
optional, best-effort mirror of per-turn provider events.

- `EventFanout` is **parallel to**, not a replacement for, the existing
  byte `Fanout`.
- Sends are **non-blocking**: `select { case ch <- ev: default: }`.
  Full channels drop events silently.
- Consumers **must not close** the channel before the session has fully
  stopped. The library does not recover from sends on a closed channel.
- `EventSink` remains Manager-lifecycle-only. Decision 0002 still
  holds; typed provider events do not move onto `EventSink`.

## Why

- The first real mux consumer of v0.1.0 confirmed the waste predicted in
  the original wrap: adapters emitted parsed events, mux JSON-lined them
  into the byte `Fanout`, then mux parsed them back into structured
  events for the TUI and HTTP attach surfaces.
- Some consumers legitimately still want the raw byte stream for logs,
  archival, debugging, or attach replay. Replacing `Fanout` would force
  those consumers to maintain a second byte pipeline anyway.
- Provider events are already typed on both constructor paths
  (`CLIAdapter.ParseLine` and `Provider.StreamChat`), so mirroring them
  is mechanically cheap.
- Backpressure should not be allowed to wedge the parse goroutine or the
  live session stream. The attach broker already prefers dropping over
  blocking; `EventFanout` matches that posture.

## How to apply

- Callers that need structured per-turn events pass a buffered
  `EventFanout` channel in `StartOptions`.
- Size the buffer for the consumer's tolerance. Unbuffered or undersized
  channels will lose events under slow consumers.
- Callers that need the raw byte stream keep using `Fanout`.
- Callers that need both set both fields and consume them independently.

## Explicit non-decision

This does **not** reopen the EventSink question. `EventSink` still emits
Manager-owned lifecycle events only; per-turn provider events stay on
the session-start fanout surfaces.
