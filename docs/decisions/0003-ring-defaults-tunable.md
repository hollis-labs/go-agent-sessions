# 0003 — Ring buffer + subscriber depth: defaults match mux, per-Session tunable

**Status:** accepted (v0.1.0)
**Question (from prompt):** Default ring buffer size + subscriber depth
— match mux's 64 KiB / 64 chunks, or expose tunable.

## Decision

Both. The library's defaults are **64 KiB ring** and **64 chunks
subscriber depth**, matching mux. Either can be overridden per-Session
via `StartOptions.RingBytes` and `StartOptions.SubscriberDepth`. The
attach-broker implementation is the same code in both cases — the
constructor honours non-zero overrides and falls back to the constants
otherwise.

## Why

- Mux's defaults have been in production long enough that "they work
  for the typical case" is empirical, not just claimed.
- A 64 KiB scrollback ring uses ~64 KiB per session — meaningful for
  thousands of concurrent sessions, irrelevant for tens. Consumers with
  the former problem already need tuning; consumers with the latter
  shouldn't be forced to think about it.
- Subscriber depth absorbs slow consumers without penalising the
  producer; 64 chunks is enough headroom that bursty output rarely
  triggers drops in practice. When it does, the broker's
  drop-on-overflow policy keeps producers unblocked.

## How to apply

- Default callers: pass nothing. The constants are right.
- Long-running review sessions where larger scrollback matters: set
  `StartOptions.RingBytes = 1<<20` (1 MiB).
- High-burst sessions where dropping less is preferable to small
  scrollback: leave `RingBytes` alone, raise `SubscriberDepth` to ~256.
- Memory-constrained per-session (clockwork's worker pool, with many
  short sessions): set `RingBytes = 4096` to shrink the floor.

## Knobs that did not become public

- `StopDrainWindow` analogue for the broker. The broker `close()` is
  already synchronous and idempotent; consumers that need a deadline
  wrap their attach calls in a `context.WithTimeout`.
- Per-subscriber drop counters. The broker tracks bytes dropped per
  broker (not per subscriber); per-subscriber counters were not in the
  mux source and adding them at v0.1.0 would commit to a public shape
  before there's a consumer asking for it.
