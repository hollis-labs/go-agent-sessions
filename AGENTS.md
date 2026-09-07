# go-agent-sessions

Turns a `go-providers` `Provider` or `CLIAdapter` into a long-lived agent
`Session` with a uniform control surface (`Wait`, `Stop`, `SendInput`, `Resize`,
`Health`, `CheckpointHints`), plus a `Manager` that registers sessions, watches
for terminal exit and fans output out to attach subscribers. It owns the
process, not the policy: persistence goes through caller-supplied sinks, and it
neither plans launches nor tracks tasks.

## Start Here

- `README.md` explains the Session/Manager surface and the PTY vs
  subprocess-per-turn split.
- `docs/decisions/` holds the ADRs that own the load-bearing choices —
  non-blocking `Start`, `Wait` on terminal, event fanout shape, ring defaults.
- `agentsessions/manager.go` owns registration, state transitions and attach.
- `agentsessions/attach.go` owns the bounded ring and subscriber fanout.
- `agentsessions/from_adapter.go` selects the runtime shape from `Capabilities`.
- `agentsessions/supervision.go` owns terminal-exit detection.
- `agentsessions/pty_session.go`, `streaming_stdio_session.go` and
  `jsonrpc_stdio_session.go` are the three long-lived runtime shapes.
- `compliance/compliance.go` is the shared harness every runtime shape passes.

## Commands

```bash
go test -race -count=1 ./...
go vet ./...
```

There is no CI workflow in this repo; the race detector is the gate that
matters, because most of the surface is concurrent.

## Boundaries

This module was absorbed into `agentkit` as `agentkit/agentsessions` at agentkit
v0.1.0 and has not changed since v0.10.0 (2026-05-21). New work belongs in
`agentkit`.

The attach broker must never block or slow the session it observes. The ring is
bounded and drops oldest; a subscriber that cannot keep up is dropped, not
waited on. `TestAttachBroker_RingBoundedAtCapacity`,
`TestAttachBroker_DropOnSlowSubscriber` and
`TestAttachBroker_LateSubscriberReplaysRing` hold that shape together, and
`TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup` is what makes resume-from-seq
trustworthy.

One turn at a time per session: `SendInput` is serialized and a concurrent send
while a turn is in flight is rejected rather than interleaved
(`TestAdapterRuntime_TurnInFlight_RejectsConcurrentSendInput`).

Stopping kills the child's whole process group, not just the child
(`TestKillWithGraceTerminatesChildProcessGroup`) — a PTY-backed CLI spawns
descendants that otherwise survive.
