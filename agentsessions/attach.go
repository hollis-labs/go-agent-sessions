package agentsessions

import (
	"context"
	"io"
	"sync"
)

// Default ring/fanout sizing. 64 KiB of scrollback is enough for most
// short-term replay without taking meaningful memory per session.
// Subscriber channel depth of 64 chunks absorbs brief consumer slowness
// before the broker starts dropping.
const (
	defaultRingBytes       = 64 * 1024
	defaultSubscriberDepth = 64
)

// Attach broker — public surface (frozen as of v0.5.0)
//
// The attach broker behind Manager.Attach / Manager.AttachWith is an
// in-memory drop-oldest ring + per-subscriber drop-on-slow fanout. The
// public observable contract is:
//
//   - Manager.Attach(ctx, sessionID, w) — replays whatever is currently in
//     the ring to w, then streams live bytes until ctx is canceled or the
//     session ends. Multiple concurrent Attach callers are supported;
//     cancellation of one does not affect the others.
//   - Manager.AttachWith(ctx, sessionID, w, AttachOptions) — same as above
//     with caller-controlled metadata. AttachOptions.SinceSeq is a
//     byte-count resume hint; the broker replays only ring bytes with
//     position > SinceSeq. Round-trip semantics: a client that disconnected
//     after observing N bytes can reconnect with SinceSeq=N and receive
//     exactly bytes N+1..head with no duplication. If SinceSeq points
//     before the ring's start (history evicted), the full ring is replayed
//     and the gap between SinceSeq and ringStart is silently lost — callers
//     detect this by comparing expected vs received byte counts.
//   - StartOptions.RingBytes / StartOptions.SubscriberDepth tune the per-
//     session ring size and per-subscriber channel depth. Zero uses the
//     defaults (64 KiB / 64 chunks).
//
// Drop policy:
//   - Ring: drop-oldest. When a Write would push the ring beyond ringCap,
//     the leading bytes are evicted. Total bytes ever written are tracked
//     so SinceSeq remains valid across evictions.
//   - Subscriber channel: drop-newest (current chunk). When a subscriber's
//     channel is full, the chunk is dropped for that subscriber only;
//     other subscribers and the ring are unaffected. Bytes-dropped is
//     counted on the broker but not currently exposed publicly.
//   - Producer (Write): never blocks on subscriber slowness. Write returns
//     io.ErrClosedPipe after broker close.
//
// Lifecycle:
//   - The broker is allocated on Manager.Start when StartOptions.AttachEnabled
//     is true. Without that flag, no broker memory is allocated and Attach
//     calls return ErrAttachDisabled.
//   - The broker is closed by the Manager's watch goroutine on session
//     terminal state. Late subscribers after close still receive the
//     replay (whatever's still in the ring) and an already-closed channel,
//     so they drain and exit cleanly.
//
// This contract is frozen as of v0.5.0. Reform (drop-policy alternatives,
// larger rings, etc.) is separate work; the audit deliverable for v0.5.0
// is documentation + the SinceSeq round-trip test
// (TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup).

// attachBroker multiplexes session output from a single producer (the
// session's copy goroutine) to zero or more attach subscribers. It
// implements io.Writer so it drops into a tee pipeline, and owns a
// bounded replay ring buffer so new subscribers see recent history before
// the live stream.
//
// Invariants:
//   - Write never blocks on a slow subscriber. If a subscriber's channel
//     is full, the chunk is dropped for that subscriber (others
//     unaffected). DroppedBytes counts these bytes per broker.
//   - close is idempotent. After close, Write returns io.ErrClosedPipe;
//     new subscribe calls return the replay and an already-closed channel
//     (so callers can drain history then exit).
//   - All subscriber channels are closed exactly once — either on
//     individual cancel() or on broker close(), whichever comes first.
//
// Lifted verbatim from agent-mux's runtime attach broker.
type attachBroker struct {
	ringCap      int
	chanCap      int
	dropCnt      int64
	totalWritten int64
	mu           sync.Mutex
	ring         []byte
	subs         map[int]chan []byte
	subNext      int
	closed       bool
}

func newAttachBroker(ringBytes, subscriberDepth int) *attachBroker {
	if ringBytes <= 0 {
		ringBytes = defaultRingBytes
	}
	if subscriberDepth <= 0 {
		subscriberDepth = defaultSubscriberDepth
	}
	return &attachBroker{
		ringCap: ringBytes,
		chanCap: subscriberDepth,
		ring:    make([]byte, 0, ringBytes),
		subs:    map[int]chan []byte{},
	}
}

// Write appends p to the replay ring (trimming the oldest bytes if the
// ring would overflow) and publishes a copy to every live subscriber. If
// a subscriber's channel is full the chunk is dropped for that subscriber
// and the dropped-byte counter is incremented.
func (b *attachBroker) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.appendRing(p)
	b.totalWritten += int64(len(p))

	cp := make([]byte, len(p))
	copy(cp, p)
	for _, ch := range b.subs {
		select {
		case ch <- cp:
		default:
			b.dropCnt += int64(len(cp))
		}
	}
	b.mu.Unlock()
	return len(p), nil
}

// computeReplay returns the bytes from ring that the caller needs to
// catch up to the head: the tail of the ring from byte offset
// (sinceSeq - ringStartSeq). When the ring doesn't cover sinceSeq —
// because the requested position was evicted — the full ring is returned
// (best-effort replay; gap is invisible to the caller). When sinceSeq >=
// totalWritten, the caller is at head and empty replay is returned.
//
// Pure function so tests can cover edge cases without spinning a broker.
func computeReplay(ring []byte, totalWritten, sinceSeq int64) []byte {
	if sinceSeq <= 0 || int64(len(ring)) == 0 {
		out := make([]byte, len(ring))
		copy(out, ring)
		return out
	}
	if sinceSeq >= totalWritten {
		return nil
	}
	ringStartSeq := totalWritten - int64(len(ring))
	if sinceSeq < ringStartSeq {
		out := make([]byte, len(ring))
		copy(out, ring)
		return out
	}
	offset := sinceSeq - ringStartSeq
	out := make([]byte, int64(len(ring))-offset)
	copy(out, ring[offset:])
	return out
}

// appendRing extends b.ring with p, keeping only the trailing ringCap
// bytes. Caller holds b.mu.
func (b *attachBroker) appendRing(p []byte) {
	if len(p) >= b.ringCap {
		b.ring = append(b.ring[:0], p[len(p)-b.ringCap:]...)
		return
	}
	spare := b.ringCap - len(b.ring)
	if len(p) > spare {
		drop := len(p) - spare
		b.ring = b.ring[drop:]
	}
	b.ring = append(b.ring, p...)
}

// subscribe returns (replay, live, cancel):
//   - replay: a snapshot of the current ring contents
//   - live:   a channel that receives future Write payloads
//   - cancel: idempotent function that unregisters and closes live
//
// After the broker is closed, subscribe still returns the current replay
// along with a channel that is already closed, so late subscribers drain
// history and then exit cleanly.
func (b *attachBroker) subscribe(depth int) (replay []byte, ch <-chan []byte, cancel func()) {
	return b.subscribeSince(depth, 0)
}

// subscribeSince is subscribe with a byte-offset hint. sinceSeq is a
// byte count since the broker's init; the returned replay contains only
// bytes with seq > sinceSeq still available in the ring. When sinceSeq
// is older than the oldest byte still in the ring, the caller silently
// receives the full ring (the gap between sinceSeq and ringStart is
// lost — callers detect this by comparing expected vs received bytes).
func (b *attachBroker) subscribeSince(depth int, sinceSeq int64) (replay []byte, ch <-chan []byte, cancel func()) {
	if depth <= 0 {
		depth = b.chanCap
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	replay = computeReplay(b.ring, b.totalWritten, sinceSeq)

	out := make(chan []byte, depth)
	if b.closed {
		close(out)
		return replay, out, func() {}
	}

	id := b.subNext
	b.subNext++
	b.subs[id] = out

	var once sync.Once
	cancel = func() {
		once.Do(func() {
			b.mu.Lock()
			if c, ok := b.subs[id]; ok {
				delete(b.subs, id)
				close(c)
			}
			b.mu.Unlock()
		})
	}
	return replay, out, cancel
}

// close tears down the broker: closes every subscriber channel, marks the
// broker closed, and causes future Write calls to return io.ErrClosedPipe.
// Idempotent.
func (b *attachBroker) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id, ch := range b.subs {
		close(ch)
		delete(b.subs, id)
	}
}

// copyStream pumps a broker subscription to w until ctx is canceled or
// the broker closes.
func copyStream(ctx context.Context, w io.Writer, replay []byte, ch <-chan []byte) error {
	if len(replay) > 0 {
		if _, err := w.Write(replay); err != nil {
			return err
		}
	}
	for {
		select {
		case chunk, ok := <-ch:
			if !ok {
				return nil
			}
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
