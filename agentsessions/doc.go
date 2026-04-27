// Package agentsessions provides a long-lived agent-session abstraction
// over the hollis-labs Go primitive libraries.
//
// A Session is the running handle for one agent: turn-based subprocess
// (driven by go-runner + a provider.CLIAdapter), long-lived PTY (a
// provider.Provider directly), or HTTP-streamed (a provider.Provider
// directly). All shapes expose the same Session interface — Wait, Stop,
// SendInput, Resize, Health, CheckpointHints — so consumers (agent-mux,
// clockwork, nanite) can drive any of them uniformly.
//
// A Manager registers Sessions, persists state transitions through
// caller-supplied sinks, watches for terminal exits, and broadcasts
// session output to attach subscribers via an in-memory ring buffer.
//
// # Composition
//
//   - go-providers — provider.Provider (long-lived, e.g. PTY/HTTP) or
//     provider.CLIAdapter (per-turn subprocess parsing). The library wraps
//     each shape into a Session via NewFromProvider / NewFromAdapter.
//   - go-runner — runner.Run drives the per-turn subprocess case.
//   - go-sandbox — sandbox.Profile travels via StartOptions.Profile and is
//     applied by go-runner under the hood.
//   - go-egress-proxy — NOT a dep. Consumers wanting allowlisted egress
//     start an egress.Proxy themselves and merge its env vars into
//     StartOptions.Env before calling Manager.Start.
//
// Persistence is consumer-owned: this library defines StateSink,
// AttachmentSink, and EventSink interfaces and ships none of their
// implementations.
//
// # Process-level State enum
//
// The library defines a fixed four-value State enum: launching, running,
// done, failed. Consumers map their domain FSM (clockwork tasks, mux
// logical agents) on top — the library only tracks process-level state.
//
// # Single-turn-in-flight
//
// SendInput is serialized per-Session by the Manager via a per-entry lock,
// matching the mux runtime contract. NewFromProvider / NewFromAdapter
// implementations additionally surface ErrTurnInFlight if a second
// SendInput arrives before the first turn's terminal event has flushed.
package agentsessions
