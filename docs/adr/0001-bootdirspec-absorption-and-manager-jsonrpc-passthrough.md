# ADR 0001 — BootDirSpec absorption + Manager.JsonRpcCall passthrough

**Status:** Accepted
**Date:** 2026-05-11
**Release:** v0.9.0

## Context

Two cross-cutting limitations surfaced after v0.8.0 shipped the
long-lived stdio runtimes (`streamingStdio` + `jsonRpcStdio`) needed to
drive Claude's stream-input mode and Codex's `app-server`:

### 1. Per-app re-implementation of BootDirSpec planting

The lib already declared the runtime-vs-app split for `BootDirSpec`:
- **go-providers** owns the spec (which files go where, env amendments,
  cwd preference, project-dir argv pattern).
- **go-agent-sessions** owns the process lifecycle (spawn, I/O loops,
  supervisor, exit classification, attach fanout).
- **App tier** (agent-mux's `claudestream.plantBootDir`, nanite's
  `internal/runtime/agent`, clockwork's launcher) owns the *execution* —
  walking the spec, materializing the tempdir, template-substituting,
  cleaning up.

Each app reimplements the execution layer ~95% identically. The 5% that
differs is per-app glue (mux's `PlanScopedAdapter` for catalog-driven
binary paths, nanite's workspace conventions). The runtime already owns
process-lifecycle state; planting is *filesystem-tier* session state
with the same lifecycle. Hoisting it into the lib eliminates the
duplication and enforces a uniform invariant (cleanup fires on every
exit path) that the per-app planters currently get by accident.

### 2. `JsonRpcCaller` unreachable through Manager

`Manager` deliberately hides raw `Session` references — every caller
goes through Manager-level methods (`Stop`, `SendInput`, `Resize`,
`AttachWith`, etc.) so the supervisor invariants stay enforced. But
`JsonRpcCaller`, added in v0.8.0 for the `jsonRpcStdio` runtime, is a
per-`Session` capability with no Manager-level counterpart. Consumers
that use Manager (the entire portfolio: mux, nanite, clockwork) cannot
invoke `Call(method, params)` against a Codex `app-server` session
without bypassing Manager's invariants.

## Decision

### Part 1 — Absorb BootDirSpec planting

Add `StartOptions.AutoPlantBootDir bool` (opt-in for v0.9.0,
default-on candidate for v0.10.0 after consumers migrate). When true
AND the adapter implements `provider.BootDirProvider`, the runtime:

1. Creates a tempdir under `StartOptions.BootDirRoot`
   (or `WorkspaceDir+"/boot/"`, or `os.TempDir()`).
2. Walks `BootDirSpec.PlantedFiles` — for each, `MkdirAll` the dir,
   call `Render(plantCtx)`, write at appropriate mode (0o600 for
   `.mcp.json` / `settings.json`; 0o644 default).
3. Substitutes `{{.BootDir}}` / `{{.ProjectDir}}` in
   `EnvAmendments` and appends to `Env`.
4. Substitutes the same tokens in `ProjectDirArg` and appends to
   the runtime's argv after `adapter.BuildArgs(...)` (via the new
   `StartOptions.ExtraArgs` field).
5. Sets the spawn cwd to `BootDirSpec.SpawnWorkdir(bootDir, projectDir)`.
6. For Claude bare-mode adapters, applies `BareInjectionPaths` to a
   per-session clone of the adapter — preserves the runtime-level
   adapter's purity so concurrent sessions don't race on field
   mutations. (Bare-mode bakes `--add-dir` into `BuildArgs` via the
   injected `ProjectDir` field, so the runtime suppresses `ExtraArgs`
   splicing on the bare path to avoid double-add.)
7. Emits the optional `OnBootDirPlanted(path)` callback once on
   successful plant.
8. Calls `os.RemoveAll(bootDir)` exactly once at terminal state
   (regardless of exit cause: Stop, idle-kill, watchdog, restart-
   exhausted, clean exit). Cleanup failures log but never surface
   as session errors.

The four lifecycle runtime kinds (`pty`, `streamingStdio`,
`jsonRpcStdio`, `adapter`) all hook the plant identically in their
`Start()` paths and arrange cleanup at their existing terminal-state
hooks (legacy waiter goroutines + supervised-defer blocks).

Default-off preserves v0.8.0 behavior byte-identically — no filesystem
activity, no `StartOptions` mutation, zero behavioral change for
existing callers.

### Part 2 — `Manager.JsonRpcCall` passthrough

New Manager method:

```go
func (m *Manager) JsonRpcCall(ctx context.Context, id string, method string, params any) (json.RawMessage, error)
```

Internally:
1. Look up the session in `m.registry` under the existing read lock.
2. Release the lock before dispatching (`Call` may block — long-running
   JSON-RPC requests should not block other Manager operations).
3. Type-assert `e.sess.(JsonRpcCaller)`. If absent, return the new typed
   error `ErrSessionNotJsonRpcCapable`. If present, forward to
   `caller.Call(ctx, method, params)`.

Errors surfaced:
- `ErrSessionNotRunning` (existing) — unknown id.
- `ErrSessionNotJsonRpcCapable` (new) — session exists but its runtime
  is not JSON-RPC capable (PTY / streaming-stdio / adapter).
- `*JsonRpcError` — passed through from `Call`; `errors.As`-extractable.
- `ctx.Err()` — caller-cancelled.

This preserves the invariant that raw `Session` is never exposed —
the Manager surface grows by *one* capability-dispatch method
(symmetric with the existing `SendInput` / `Resize` shape), not by
relaxing the encapsulation boundary.

Unconditional in v0.9.0 — no feature flag, since consumers that don't
use `jsonRpcStdio` sessions are unaffected.

## Alternatives considered

### For Part 1

- **Keep per-app planters.** Status quo. Rejected: the duplication is
  not load-bearing, and the per-app implementations diverge on
  cleanup-on-exit coverage (each handles a different subset of
  Stop/idle-kill/watchdog/restart-exhausted/ctx-cancel paths). Hoisting
  unifies that coverage.
- **New top-level method `agentsessions.PlantBootDir(...)` exposed for
  apps to call themselves.** Same duplication problem; doesn't lift
  the cleanup-on-terminal invariant.
- **Default-on instead of opt-in.** Would require consumer migration in
  the same release. Deferred to v0.10.0 to give mux / nanite /
  clockwork independent migration windows.

### For Part 2

- **`Manager.Session(id) (Session, bool)` — return the raw Session.**
  Slippery slope: breaks the invariant for every method, not just
  JSON-RPC. Rejected.
- **`Manager.JsonRpcCaller(id) (JsonRpcCaller, bool)` — return a
  typed reference.** Creates stale-reference risk if the session
  stops underneath; callers would have to re-check on every call.
  Less safe than the dispatch shape.
- **Generalize as `Manager.Call(id, capability string, ...)`.**
  Capability dispatch by string introduces runtime-error surface
  where compile-time would do. Rejected.

The chosen passthrough mirrors the existing `Manager.SendInput` /
`Manager.Resize` / `Manager.Stop` shape exactly: each is a per-session
capability surfaced as a Manager-level method that internally
dispatches.

## Consequences

### Positive

- Mux's `internal/provider/cli/claudestream/plantBootDir` (~200 LOC),
  nanite's `internal/runtime/agent/` planter, and clockwork's
  `internal/runtime/agent/factory.go` planting bits become deletable
  on adopter timelines.
- Cleanup-on-every-exit-path is enforced uniformly; per-app drift on
  exit-cause coverage stops accumulating.
- Codex `app-server` (`SendTurn` and friends) becomes invokable
  through Manager — unblocks mux's v005-05 sprint and matches the
  Manager-mediated pattern the rest of the portfolio already uses.
- The Manager surface stays minimal: one new method, one new typed
  error. No structural changes to the Session/Runtime/Capabilities
  contracts.

### Negative

- One more `StartOptions` field (`AutoPlantBootDir`) for callers to
  understand. Documented and defaulted to preserve existing behavior.
- Adopters who want bare-mode Claude with AutoPlantBootDir get an
  extra adapter-clone allocation per session. Cost is negligible
  (one struct copy) and preserves concurrency safety.

### Migration

- v0.9.0 ships with `AutoPlantBootDir` opt-in. All current consumers
  see zero change.
- Consumer sprints adopt: mux v005-07 deletes `claudestream.plantBootDir`,
  nanite drops its per-profile planter, clockwork drops the planter
  bits from `factory.go`. Each can move independently.
- v0.10.0 flips the default to true once all three apps confirm
  green. Release notes call out the migration window well in advance.

## References

- Proposal: `agent-workspaces/execution/agent-mux/v005-05-long-lived-integration/2026-05-11/bootdirspec-absorption-proposal.md`
- Portfolio lifecycle axis: `agent-workspaces/knowledge/portfolio/cli-agent-long-lived-modes.md`
- Vanta memory: `followup_go_agent_sessions_v009_absorb_bootdirspec_planting`
- Mux reference implementation: `agent-mux/internal/provider/cli/claudestream/cliadapter.go` (`plantBootDir`)
- v0.8.0 lib changelog (runtime kinds this builds on)
