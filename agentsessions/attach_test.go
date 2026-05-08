package agentsessions

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// drainN reads up to n chunks from ch or returns what it got after timeout.
func drainN(ch <-chan []byte, n int, timeout time.Duration) []byte {
	deadline := time.After(timeout)
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		select {
		case chunk, ok := <-ch:
			if !ok {
				return buf.Bytes()
			}
			buf.Write(chunk)
		case <-deadline:
			return buf.Bytes()
		}
	}
	return buf.Bytes()
}

func TestAttachBroker_FanoutToSubscribers(t *testing.T) {
	b := newAttachBroker(0, 0)
	defer b.close()

	_, ch1, cancel1 := b.subscribe(0)
	defer cancel1()
	_, ch2, cancel2 := b.subscribe(0)
	defer cancel2()

	if _, err := b.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := drainN(ch1, 1, 200*time.Millisecond); !bytes.Equal(got, []byte("hello")) {
		t.Errorf("ch1 = %q", got)
	}
	if got := drainN(ch2, 1, 200*time.Millisecond); !bytes.Equal(got, []byte("hello")) {
		t.Errorf("ch2 = %q", got)
	}
}

func TestAttachBroker_LateSubscriberReplaysRing(t *testing.T) {
	b := newAttachBroker(64, 16)
	defer b.close()

	_, _ = b.Write([]byte("aaaa"))
	_, _ = b.Write([]byte("bbbb"))

	replay, ch, cancel := b.subscribe(0)
	defer cancel()

	if !bytes.Equal(replay, []byte("aaaabbbb")) {
		t.Errorf("replay = %q, want aaaabbbb", replay)
	}

	_, _ = b.Write([]byte("cccc"))
	if got := drainN(ch, 1, 200*time.Millisecond); !bytes.Equal(got, []byte("cccc")) {
		t.Errorf("live = %q", got)
	}
}

func TestAttachBroker_RingBoundedAtCapacity(t *testing.T) {
	b := newAttachBroker(4, 16) // tiny ring
	defer b.close()
	for i := 0; i < 10; i++ {
		_, _ = b.Write([]byte("x"))
	}
	replay, _, cancel := b.subscribe(0)
	defer cancel()
	if len(replay) != 4 {
		t.Errorf("replay len = %d, want 4 (ring cap)", len(replay))
	}
}

func TestAttachBroker_DropOnSlowSubscriber(t *testing.T) {
	b := newAttachBroker(1024, 1) // depth 1 — easy to fill
	defer b.close()

	// Subscribe but never read.
	_, _, cancel := b.subscribe(0)
	defer cancel()

	for i := 0; i < 50; i++ {
		_, _ = b.Write([]byte("xxxxxxxx"))
	}
	if b.dropCnt == 0 {
		t.Errorf("dropCnt = 0, want >0 with slow subscriber")
	}
}

func TestAttachBroker_CloseIdempotent(t *testing.T) {
	b := newAttachBroker(64, 16)
	b.close()
	b.close() // must not panic

	_, err := b.Write([]byte("after close"))
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Write after close = %v, want io.ErrClosedPipe", err)
	}
}

func TestAttachBroker_LateSubscriberAfterClose_DrainsThenEnds(t *testing.T) {
	b := newAttachBroker(64, 16)
	_, _ = b.Write([]byte("history"))
	b.close()

	replay, ch, cancel := b.subscribe(0)
	defer cancel()
	if !bytes.Equal(replay, []byte("history")) {
		t.Errorf("replay = %q", replay)
	}
	// ch should be already closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("channel didn't close")
	}
}

func TestAttachBroker_CancelRemovesSubscriber(t *testing.T) {
	b := newAttachBroker(64, 16)
	defer b.close()

	_, ch, cancel := b.subscribe(0)
	cancel()
	cancel() // idempotent

	// Channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("channel didn't close")
	}

	// Writes still work for remaining (zero) subscribers.
	if _, err := b.Write([]byte("noone")); err != nil {
		t.Errorf("Write: %v", err)
	}
}

func TestAttachBroker_ConcurrentWritesAndSubscribers(t *testing.T) {
	b := newAttachBroker(4096, 64)
	defer b.close()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		_, _, cancel := b.subscribe(0)
		defer cancel()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = b.Write([]byte("x"))
			}
		}()
	}
	wg.Wait()
	if b.totalWritten != 800 {
		t.Errorf("totalWritten = %d, want 800", b.totalWritten)
	}
}

func TestComputeReplay_Cases(t *testing.T) {
	tests := []struct {
		name         string
		ring         []byte
		totalWritten int64
		sinceSeq     int64
		want         []byte
	}{
		{"sinceSeq=0 returns full ring", []byte("abcd"), 4, 0, []byte("abcd")},
		{"sinceSeq>=totalWritten empty", []byte("abcd"), 4, 4, nil},
		{"sinceSeq inside ring", []byte("abcd"), 4, 2, []byte("cd")},
		{"sinceSeq before ringStart returns full", []byte("abcd"), 100, 50, []byte("abcd")}, // ringStart=96
		{"empty ring", nil, 0, 0, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeReplay(tt.ring, tt.totalWritten, tt.sinceSeq)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("computeReplay = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAttachBroker_SubscribeSince_SkipsSeenBytes(t *testing.T) {
	b := newAttachBroker(64, 16)
	defer b.close()

	_, _ = b.Write([]byte("aaaa"))
	_, _ = b.Write([]byte("bbbb"))

	// Subscriber resumes from byte 4 — should only see "bbbb"
	replay, _, cancel := b.subscribeSince(0, 4)
	defer cancel()
	if !bytes.Equal(replay, []byte("bbbb")) {
		t.Errorf("replay = %q, want bbbb", replay)
	}
}

// TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup exercises the
// disconnect-and-resume pattern through the Manager's public surface:
// attach a consumer, push 10 chunks, disconnect at seq=5 bytes, reconnect
// with SinceSeq=5, and assert that the second attach receives exactly the
// trailing 5 bytes without duplication or gap.
//
// This is the public contract documented on AttachOptions.SinceSeq;
// breaking this round-trip silently regresses any consumer that resumes
// attaches after a transient disconnect.
func TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("rt-since", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID:      "s",
		Runtime: rt,
		Options: StartOptions{
			Workdir:       t.TempDir(),
			AttachEnabled: true,
		},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	sess := rt.lastSession()

	// Push 10 bytes to the broker BEFORE any attach — these populate
	// the ring so the future attach replays history.
	for i := 0; i < 10; i++ {
		sess.emit([]byte{byte('A' + i)})
		time.Sleep(2 * time.Millisecond)
	}

	// First attach: no since_seq → expect full history (10 bytes).
	first := &syncBuf{}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_ = m.Attach(ctx1, "s", first)
	}()
	// Wait for replay to drain.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(first.String()) >= 10 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := first.String(); got != "ABCDEFGHIJ" {
		t.Fatalf("first attach replay = %q, want ABCDEFGHIJ", got)
	}
	cancel1()
	<-done1

	// Reconnect with SinceSeq=5 → should receive only "FGHIJ" (bytes 6-10).
	second := &syncBuf{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_ = m.AttachWith(ctx2, "s", second, AttachOptions{SinceSeq: 5})
	}()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(second.String()) >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := second.String(); got != "FGHIJ" {
		t.Errorf("resumed attach replay (since_seq=5) = %q, want FGHIJ (no gap, no dup)", got)
	}
	cancel2()
	<-done2

	// Cleanup: complete session so Manager.watch can drain.
	sess.complete(0)
	_, _ = m.WaitSession(context.Background(), "s")
}
