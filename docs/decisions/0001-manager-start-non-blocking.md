# 0001 — Manager.Start is non-blocking

**Status:** accepted (v0.1.0)
**Question (from prompt):** Does `Manager.Start` block on the first state event, or return immediately and let the caller observe?

## Decision

`Manager.Start` returns as soon as the session is registered (state =
`running`) and the watch goroutine is spawned. It does **not** block on
any "first observable event" from the underlying Session.

## Why

- Mux already shipped this contract; consumers are written to it.
- Adapter-side latency is unbounded and varies wildly (PTY spawn vs API
  StreamChat vs subprocess). Blocking on a first-event signal would
  couple `Start`'s latency to the slowest adapter and force every caller
  to bring its own timeout.
- Consumers that need first-event semantics (e.g. "wait for the system
  banner before sending input") have a cleaner option already: subscribe
  via `Manager.Attach` and observe the byte stream directly. That
  option exists for any consumer; making `Start` block would penalize
  the 90% case for the 10%.

## How to apply

When the consumer needs to know "is the session ready for input?", they
poll `Manager.Health(id).Health.Alive`, subscribe to `LifecycleEvent`
via `EventSink`, or attach to the byte stream. None of these requires
`Start` to block.
