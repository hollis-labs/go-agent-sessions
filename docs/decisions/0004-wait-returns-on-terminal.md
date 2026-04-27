# 0004 — Session.Wait / Manager.WaitSession return on terminal state, not on attach drain

**Status:** accepted (v0.1.0)
**Question (from prompt):** Should `Session.Wait` return on terminal
state regardless of attach subscribers, or wait for drain?

## Decision

`Session.Wait` and `Manager.WaitSession` return as soon as the
underlying session reaches a terminal state. They do **not** wait for
attach subscribers to finish draining the broker.

The watch goroutine closes the attach broker after recording the
terminal state (`StateDone` / `StateFailed`), which causes every active
subscriber's channel to close. Subscribers see EOF naturally on the
next `range ch`/`<-ch` and exit.

## Why

- Tying `Wait` to subscriber drain creates a path where a stuck client
  pins a session's lifecycle indefinitely. The library is not a place
  for that policy — it would have to ship its own subscriber-stalled
  detector and slow-consumer eviction, replicating problems the broker
  already solved with drop-on-overflow.
- Drain ordering is a consumer concern. A consumer that needs "wait
  until subscribers caught up" can hold the attach goroutine until it
  has read the bytes it cares about, then `Wait` separately.
- Mux made the same call (its `WaitSession` returns on `result.done`,
  which closes in the watch goroutine independently of attach state).

## How to apply

- Consumers that want "session done AND I've read everything in the
  ring" pattern: subscribe via Attach, wait for the channel to close
  (signals broker close), then call `WaitSession` to read the exit code.
  Both happen in any order; Attach close after broker close is
  guaranteed by the watch goroutine sequence.
- Consumers that want "exit code only, don't care about output":
  `WaitSession` is enough. No subscribers, no broker spin if
  `AttachEnabled=false`.

## Failure modes considered

- **Subscriber backpressures the producer.** Cannot happen — the
  broker drops on a full subscriber channel and increments
  `dropCnt`; `Write` is non-blocking.
- **Subscriber leaks.** Subscribers always exit when their channel
  closes (broker close after watch). A subscriber that ignores `<-ch`
  ok-status is the consumer's bug, not the library's.
- **Session terminates between subscribe and first Read.** Subscribe
  returns the replay snapshot synchronously, so no events are lost
  in this gap.
