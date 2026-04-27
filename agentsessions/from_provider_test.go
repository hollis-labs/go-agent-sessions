package agentsessions

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hollis-labs/go-providers/provider"
)

type fakeProvider struct {
	events []provider.StreamEvent
	err    error
}

func (p *fakeProvider) StreamChat(ctx context.Context, req provider.ChatRequest) (<-chan provider.StreamEvent, error) {
	if p.err != nil {
		return nil, p.err
	}
	ch := make(chan provider.StreamEvent, len(p.events))
	go func() {
		defer close(ch)
		for _, ev := range p.events {
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
		}
	}()
	return ch, nil
}

func (p *fakeProvider) Complete(ctx context.Context, req provider.ChatRequest) (string, error) {
	return "", errors.New("not implemented in test")
}

func (p *fakeProvider) Capabilities() provider.ProviderCapabilities {
	return provider.ProviderCapabilities{}
}

func TestProviderRuntime_EventFanout_DeliversToBothFanouts(t *testing.T) {
	rt, err := NewFromProvider(ProviderRuntimeConfig{
		ID:   "provider-both",
		Kind: "api",
		Provider: &fakeProvider{events: []provider.StreamEvent{
			{Type: provider.EventSessionID, SessionID: "ses_provider"},
			{Type: provider.EventDelta, Content: "hello"},
			{Type: provider.EventDone},
		}},
	})
	if err != nil {
		t.Fatalf("NewFromProvider: %v", err)
	}

	var fanout bytes.Buffer
	eventCh := make(chan provider.StreamEvent, 8)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     t.TempDir(),
		Fanout:      &fanout,
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	want := []provider.StreamEvent{
		{Type: provider.EventSessionID, SessionID: "ses_provider"},
		{Type: provider.EventDelta, Content: "hello"},
		{Type: provider.EventDone},
	}
	if got := takeEvents(eventCh, len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("EventFanout events = %#v, want %#v", got, want)
	}
	if got := fanout.String(); got != "hello\n[turn_done]\n" {
		t.Fatalf("byte Fanout = %q, want %q", got, "hello\n[turn_done]\n")
	}
}

func TestProviderRuntime_EventFanout_NilByteFanout_StillWorks(t *testing.T) {
	rt, err := NewFromProvider(ProviderRuntimeConfig{
		ID:   "provider-typed-only",
		Kind: "api",
		Provider: &fakeProvider{events: []provider.StreamEvent{
			{Type: provider.EventUsage, Usage: &provider.Usage{OutputTokens: 5}},
			{Type: provider.EventDone},
		}},
	})
	if err != nil {
		t.Fatalf("NewFromProvider: %v", err)
	}

	eventCh := make(chan provider.StreamEvent, 8)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     t.TempDir(),
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	want := []provider.StreamEvent{
		{Type: provider.EventUsage, Usage: &provider.Usage{OutputTokens: 5}},
		{Type: provider.EventDone},
	}
	if got := takeEvents(eventCh, len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("EventFanout events = %#v, want %#v", got, want)
	}
}

func TestProviderRuntime_EventFanout_SlowConsumer_DropsNotBlocks(t *testing.T) {
	rt, err := NewFromProvider(ProviderRuntimeConfig{
		ID:   "provider-slow",
		Kind: "api",
		Provider: &fakeProvider{events: []provider.StreamEvent{
			{Type: provider.EventDelta, Content: "one"},
			{Type: provider.EventDelta, Content: "two"},
			{Type: provider.EventDelta, Content: "three"},
			{Type: provider.EventDone},
		}},
	})
	if err != nil {
		t.Fatalf("NewFromProvider: %v", err)
	}

	eventCh := make(chan provider.StreamEvent, 1)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     t.TempDir(),
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan error, 1)
	go func() {
		done <- sess.SendInput(context.Background(), []byte("ignored"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendInput: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendInput blocked on slow EventFanout consumer")
	}

	got := drainEvents(eventCh)
	if len(got) != 1 {
		t.Fatalf("received %d typed events, want exactly 1 buffered event after drops", len(got))
	}
}

func TestProviderRuntime_EventFanout_NilPreservesV010Behavior(t *testing.T) {
	rt, err := NewFromProvider(ProviderRuntimeConfig{
		ID:   "provider-nil",
		Kind: "api",
		Provider: &fakeProvider{events: []provider.StreamEvent{
			{Type: provider.EventDelta, Content: "hello"},
			{Type: provider.EventDelta, Content: " world"},
			{Type: provider.EventDone},
		}},
	})
	if err != nil {
		t.Fatalf("NewFromProvider: %v", err)
	}

	var fanout bytes.Buffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: t.TempDir(),
		Fanout:  &fanout,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	if got := fanout.String(); got != "hello world\n[turn_done]\n" {
		t.Fatalf("byte Fanout = %q, want %q", got, "hello world\n[turn_done]\n")
	}
}
