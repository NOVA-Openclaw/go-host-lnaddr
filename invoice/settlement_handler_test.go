package invoice

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btclog"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/nbd-wtf/go-nostr"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- Fakes ----------------------------------------------------------------

type streamEvent struct {
	invoice *lnrpc.Invoice
	err     error
}

type fakeStream struct {
	ctx    context.Context
	events []streamEvent
	pos    int
	mu     sync.Mutex
}

func newFakeStream(ctx context.Context, events ...streamEvent) *fakeStream {
	return &fakeStream{ctx: ctx, events: events}
}

func (f *fakeStream) Recv() (*lnrpc.Invoice, error) {
	f.mu.Lock()
	if f.pos < len(f.events) {
		e := f.events[f.pos]
		f.pos++
		f.mu.Unlock()
		return e.invoice, e.err
	}
	f.mu.Unlock()

	// No more scripted events: block until the context is canceled, matching
	// the behavior of a live gRPC stream.
	<-f.ctx.Done()
	return nil, f.ctx.Err()
}

func (f *fakeStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (f *fakeStream) Trailer() metadata.MD         { return metadata.MD{} }
func (f *fakeStream) CloseSend() error             { return nil }
func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) SendMsg(m interface{}) error  { return nil }
func (f *fakeStream) RecvMsg(m interface{}) error  { return nil }

type subscribeCall struct {
	stream *fakeStream
	err    error
}

type fakeLightningClient struct {
	lnrpc.LightningClient
	mu    sync.Mutex
	calls []*subscribeCall
	index int
	reqs  []*lnrpc.InvoiceSubscription
}

func (f *fakeLightningClient) SubscribeInvoices(ctx context.Context,
	req *lnrpc.InvoiceSubscription, _ ...grpc.CallOption) (
	lnrpc.Lightning_SubscribeInvoicesClient, error) {

	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	if f.index >= len(f.calls) {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	call := f.calls[f.index]
	f.index++
	f.mu.Unlock()
	return call.stream, call.err
}

func (f *fakeLightningClient) SubscriptionRequests() []*lnrpc.InvoiceSubscription {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*lnrpc.InvoiceSubscription, len(f.reqs))
	copy(out, f.reqs)
	return out
}

// --- Test harness ---------------------------------------------------------

type callbackRecord struct {
	amount  uint64
	comment string
}

type testHarness struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	client        *fakeLightningClient
	handler       *SettlementHandler
	notifications chan callbackRecord
	zaps          chan *zapReceipt
	wg            sync.WaitGroup
}

func newTestHarness(t *testing.T,
	setup func(ctx context.Context) *fakeLightningClient,
	backoff func(int) time.Duration) *testHarness {

	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	client := setup(ctx)
	h := &testHarness{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		client:        client,
		notifications: make(chan callbackRecord, 100),
		zaps:          make(chan *zapReceipt, 100),
	}

	h.handler = NewSettlementHandler(client, "")
	if backoff != nil {
		h.handler.backoff = backoff
	}
	h.handler.broadcastNotification = func(amt uint64, comment string) {
		h.notifications <- callbackRecord{amount: amt, comment: comment}
	}
	h.handler.publishZapReceipt = func(r *zapReceipt) {
		h.zaps <- r
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.handler.Run(ctx)
	}()

	return h
}

func (h *testHarness) stop() {
	h.cancel()
	h.wg.Wait()
}

func (h *testHarness) register(rHash []byte, comment string, zapReceipt *zapReceipt) {
	h.t.Helper()
	if err := h.handler.subscribeInvoiceSettlements(context.Background(),
		rHash, comment, zapReceipt); err != nil {
		h.t.Fatalf("register: %v", err)
	}
}

func settledInvoice(settleIndex uint64, rHash []byte, amtSat int64) *lnrpc.Invoice {
	return &lnrpc.Invoice{
		State:       lnrpc.Invoice_SETTLED,
		SettleIndex: settleIndex,
		RHash:       rHash,
		RPreimage:   bytes.Repeat([]byte{0xab}, 32),
		AmtPaidSat:  amtSat,
		SettleDate:  1234567890,
	}
}

func pendingInvoice(rHash []byte) *lnrpc.Invoice {
	return &lnrpc.Invoice{
		State: lnrpc.Invoice_OPEN,
		RHash: rHash,
	}
}

func waitForNotification(t *testing.T, ch <-chan callbackRecord,
	timeout time.Duration) (callbackRecord, bool) {

	t.Helper()
	select {
	case r := <-ch:
		return r, true
	case <-time.After(timeout):
		return callbackRecord{}, false
	}
}

func waitForZap(t *testing.T, ch <-chan *zapReceipt,
	timeout time.Duration) (*zapReceipt, bool) {

	t.Helper()
	select {
	case r := <-ch:
		return r, true
	case <-time.After(timeout):
		return nil, false
	}
}

func noNotification(t *testing.T, ch <-chan callbackRecord,
	timeout time.Duration) {

	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("unexpected notification: %+v", r)
	case <-time.After(timeout):
	}
}

func testDiscardLogger(t *testing.T) btclog.Logger {
	t.Helper()
	return btclog.NewBackend(io.Discard).Logger("TEST")
}

func captureLogger(t *testing.T) (*bytes.Buffer, btclog.Logger) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := btclog.NewBackend(buf).Logger("TEST")
	logger.SetLevel(btclog.LevelWarn)
	return buf, logger
}

// --- Group F: dedup / no-miss ---------------------------------------------

// F1 — Baseline happy path (no reconnect).
func TestSettlementHandler_BaselineHappyPath(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	rHash := []byte{1, 2, 3, 4}
	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{invoice: settledInvoice(1, rHash, 21)})},
		}}
	}, nil)
	defer h.stop()
	h.register(rHash, "hello", nil)

	rec, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected notification for settled invoice")
	}
	if rec.amount != 21 || rec.comment != "hello" {
		t.Fatalf("unexpected notification: %+v", rec)
	}
}

// F2 — Settlement during disconnect window is still delivered.
// F3 — No duplicate notification across reconnect.
// F4 — Reconnect subscribes with SettleIndex advanced.
func TestSettlementHandler_NoMissNoDuplicate_AdvancesSettleIndex(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	rHashA := []byte{1}
	rHashB := []byte{2}

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, rHashA, 100)},
				streamEvent{err: io.EOF},
			)},
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, rHashA, 100)}, // replay
				streamEvent{invoice: settledInvoice(2, rHashB, 200)},
			)},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()
	h.register(rHashA, "A", nil)
	h.register(rHashB, "B", nil)

	recA, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected notification for A")
	}
	if recA.comment != "A" {
		t.Fatalf("expected A, got %+v", recA)
	}

	recB, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected notification for B")
	}
	if recB.comment != "B" {
		t.Fatalf("expected B, got %+v", recB)
	}

	noNotification(t, h.notifications, 200*time.Millisecond)

	reqs := h.client.SubscriptionRequests()
	if len(reqs) != 2 {
		t.Fatalf("expected 2 SubscribeInvoices calls, got %d", len(reqs))
	}
	if reqs[0].SettleIndex != 0 {
		t.Fatalf("first subscription SettleIndex should be 0, got %d", reqs[0].SettleIndex)
	}
	if reqs[1].SettleIndex != 1 {
		t.Fatalf("second subscription SettleIndex should be 1 (last processed), got %d",
			reqs[1].SettleIndex)
	}
}

// F5 — Unregistered/foreign invoice during backlog replay is ignored.
func TestSettlementHandler_UnregisteredInvoiceIgnored(t *testing.T) {
	defer goleak.VerifyNone(t)

	buf, logger := captureLogger(t)
	SetLogger(logger)

	rHashKnown := []byte{1}
	rHashForeign := []byte{9}

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, rHashKnown, 10)},
				streamEvent{invoice: settledInvoice(2, rHashForeign, 99)},
			)},
		}}
	}, nil)
	defer h.stop()
	h.register(rHashKnown, "known", nil)

	rec, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected notification for known invoice")
	}
	if rec.comment != "known" {
		t.Fatalf("unexpected notification: %+v", rec)
	}

	time.Sleep(50 * time.Millisecond)
	noNotification(t, h.notifications, 100*time.Millisecond)

	// Stop the handler before reading the captured log buffer to avoid racing
	// the logger goroutine.
	h.stop()

	logs := buf.String()
	if !strings.Contains(logs, "unregistered invoice") {
		t.Fatalf("expected WARN log for unregistered invoice, got:\n%s", logs)
	}
	if !strings.Contains(logs, hex.EncodeToString(rHashForeign)) {
		t.Fatalf("expected WARN log to include foreign rHash, got:\n%s", logs)
	}
}

// F6 — Mixed settle outcomes across a disconnect.
func TestSettlementHandler_MixedOutcomesAcrossDisconnect(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	rHashA := []byte{1}
	rHashB := []byte{2}
	rHashC := []byte{3}

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{err: io.EOF})},
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, rHashA, 10)},
				streamEvent{invoice: settledInvoice(2, rHashB, 20)},
			)},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()
	h.register(rHashA, "A", nil)
	h.register(rHashB, "B", nil)
	h.register(rHashC, "C", nil)

	got := make(map[string]callbackRecord)
	for i := 0; i < 2; i++ {
		rec, ok := waitForNotification(t, h.notifications, time.Second)
		if !ok {
			t.Fatalf("expected 2 notifications, got %d", i)
		}
		got[rec.comment] = rec
	}

	if got["A"].amount != 10 || got["B"].amount != 20 {
		t.Fatalf("unexpected notifications: %+v", got)
	}

	noNotification(t, h.notifications, 200*time.Millisecond)
}

// F7 — Duplicate delivery from the stream itself is deduped.
func TestSettlementHandler_DuplicateDeliveryDeduped(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	rHash := []byte{1}
	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, rHash, 42)},
				streamEvent{invoice: settledInvoice(1, rHash, 42)},
			)},
		}}
	}, nil)
	defer h.stop()
	h.register(rHash, "dup", nil)

	rec, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected one notification")
	}
	if rec.amount != 42 {
		t.Fatalf("unexpected: %+v", rec)
	}

	noNotification(t, h.notifications, 200*time.Millisecond)
}

// --- Group E: reconnect behavior ------------------------------------------

// E1 — io.EOF reconnects.
func TestSettlementHandler_EOFCausesReconnect(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{err: io.EOF})},
			{stream: newFakeStream(ctx)},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()

	for len(h.client.SubscriptionRequests()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
}

// E2 — gRPC Unavailable reconnects.
func TestSettlementHandler_UnavailableCausesReconnect(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{err: status.Error(codes.Unavailable, "boom")})},
			{stream: newFakeStream(ctx)},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()

	for len(h.client.SubscriptionRequests()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
}

// E3 — server-side Canceled reconnects.
func TestSettlementHandler_ServerCanceledCausesReconnect(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{err: status.Error(codes.Canceled, "server gone")})},
			{stream: newFakeStream(ctx)},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()

	for len(h.client.SubscriptionRequests()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
}

// E4 — our own ctx cancel exits cleanly without reconnect.
func TestSettlementHandler_OwnCtxCancelExits(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx)},
		}}
	}, nil)
	defer h.stop()

	time.Sleep(20 * time.Millisecond)
	if len(h.client.SubscriptionRequests()) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(h.client.SubscriptionRequests()))
	}

	h.stop()

	if len(h.client.SubscriptionRequests()) != 1 {
		t.Fatalf("expected no reconnect after ctx cancel, got %d subscriptions",
			len(h.client.SubscriptionRequests()))
	}
}

// E5 — SubscribeInvoices call itself errors: retry with backoff.
// E6 — Successful resubscribe after one failed attempt.
func TestSettlementHandler_SubscribeErrorRetriesThenRecovers(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{err: errors.New("lnd transient")},
			{stream: newFakeStream(ctx, streamEvent{invoice: settledInvoice(1, []byte{1}, 5)})},
		}}
	}, func(int) time.Duration { return time.Millisecond })
	defer h.stop()
	h.register([]byte{1}, "recovered", nil)

	_, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected settlement after recovery")
	}

	reqs := h.client.SubscriptionRequests()
	if len(reqs) != 2 {
		t.Fatalf("expected 2 SubscribeInvoices calls, got %d", len(reqs))
	}
}

// E7/E8/E9 — backoff bounded below, above, and resets after success.
func TestSettlementHandler_BackoffBoundsAndReset(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	var mu sync.Mutex
	var attempts []int

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx, streamEvent{err: io.EOF})},
			{stream: newFakeStream(ctx,
				streamEvent{invoice: settledInvoice(1, []byte{1}, 1)},
				streamEvent{err: io.EOF},
			)},
			{stream: newFakeStream(ctx)},
		}}
	}, func(attempt int) time.Duration {
		mu.Lock()
		attempts = append(attempts, attempt)
		mu.Unlock()
		return time.Millisecond
	})
	defer h.stop()
	h.register([]byte{1}, "x", nil)

	_, ok := waitForNotification(t, h.notifications, time.Second)
	if !ok {
		t.Fatal("expected settlement")
	}

	for len(h.client.SubscriptionRequests()) < 3 {
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(attempts) < 2 {
		t.Fatalf("expected at least 2 backoff calls, got %v", attempts)
	}
	if attempts[0] != 0 {
		t.Fatalf("first retry should use attempt 0, got %d", attempts[0])
	}
	foundReset := false
	for i := 1; i < len(attempts); i++ {
		if attempts[i] == 0 {
			foundReset = true
			break
		}
	}
	if !foundReset {
		t.Fatalf("backoff did not reset after success; attempts=%v", attempts)
	}
}

// E10 — rapid flapping: no panic, no leak, not near-zero time.
func TestSettlementHandler_RapidFlapping(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	const n = 20
	delay := time.Millisecond

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		calls := make([]*subscribeCall, n)
		for i := 0; i < n; i++ {
			calls[i] = &subscribeCall{stream: newFakeStream(ctx, streamEvent{err: io.EOF})}
		}
		return &fakeLightningClient{calls: calls}
	}, func(int) time.Duration { return delay })
	defer h.stop()

	start := time.Now()
	for len(h.client.SubscriptionRequests()) < n {
		time.Sleep(5 * time.Millisecond)
		if time.Since(start) > time.Second {
			t.Fatal("timed out waiting for flapping reconnects")
		}
	}
	elapsed := time.Since(start)

	if elapsed < time.Duration(n-1)*delay/2 {
		t.Fatalf("reconnected too fast (%v), backoff not applied", elapsed)
	}
}

// --- Group G: graceful shutdown / goroutine hygiene -----------------------

// G1 — Shutdown while blocked in Recv.
func TestSettlementHandler_ShutdownWhileBlockedInRecv(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx)}, // blocks forever until ctx canceled
		}}
	}, nil)
	defer h.stop()

	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		h.stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after ctx cancel while blocked in Recv")
	}
}

// G2 — Shutdown mid-backoff-sleep.
func TestSettlementHandler_ShutdownMidBackoff(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{err: errors.New("boom")},
		}}
	}, func(int) time.Duration { return time.Hour })
	defer h.stop()

	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		h.stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit promptly when canceled during backoff")
	}
}

// G3 — No leaked goroutines after shutdown.
func TestSettlementHandler_NoLeakAfterShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx)
	client := &fakeLightningClient{calls: []*subscribeCall{{stream: stream}}}

	sh := NewSettlementHandler(client, "")
	sh.backoff = func(int) time.Duration { return time.Millisecond }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sh.Run(ctx)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	time.Sleep(20 * time.Millisecond)
}

// G4 — Repeated start/stop cycles don't accumulate leaks.
func TestSettlementHandler_RepeatedStartStopNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream := newFakeStream(ctx)
		client := &fakeLightningClient{calls: []*subscribeCall{{stream: stream}}}

		sh := NewSettlementHandler(client, "")
		sh.backoff = func(int) time.Duration { return time.Millisecond }

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sh.Run(ctx)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()
	}
}

// G5 — Shutdown with in-flight unsettled registrations.
func TestSettlementHandler_ShutdownWithUnsettledRegistrations(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	h := newTestHarness(t, func(ctx context.Context) *fakeLightningClient {
		return &fakeLightningClient{calls: []*subscribeCall{
			{stream: newFakeStream(ctx)},
		}}
	}, nil)
	defer h.stop()
	h.register([]byte{1}, "unsettled", nil)
	h.register([]byte{2}, "also unsettled", nil)

	noNotification(t, h.notifications, 100*time.Millisecond)
}

// --- Group I: process restart ---------------------------------------------

// I1 — Settlement for an invoice created before a process restart is an
// accepted loss; the handler logs a WARN with the rHash and does not crash.
func TestSettlementHandler_RestartAcceptedLoss(t *testing.T) {
	defer goleak.VerifyNone(t)

	buf, logger := captureLogger(t)
	SetLogger(logger)

	// P1: invoice X registered and notified.
	ctx1, cancel1 := context.WithCancel(context.Background())
	rHashX := []byte{0xab}
	stream1 := newFakeStream(ctx1,
		streamEvent{invoice: settledInvoice(1, rHashX, 7)},
	)
	client1 := &fakeLightningClient{calls: []*subscribeCall{{stream: stream1}}}
	sh1 := NewSettlementHandler(client1, "")
	sh1.broadcastNotification = func(uint64, string) {}
	sh1.publishZapReceipt = func(*zapReceipt) {}

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sh1.Run(ctx1)
	}()
	sh1.subscribeInvoiceSettlements(context.Background(), rHashX, "X", nil)

	time.Sleep(50 * time.Millisecond)
	cancel1()
	wg1.Wait()

	// P2: fresh handler, empty registry. It receives X's settlement in backlog.
	ctx2, cancel2 := context.WithCancel(context.Background())
	stream2 := newFakeStream(ctx2,
		streamEvent{invoice: settledInvoice(1, rHashX, 7)},
	)
	client2 := &fakeLightningClient{calls: []*subscribeCall{{stream: stream2}}}
	sh2 := NewSettlementHandler(client2, "")
	notified := make(chan callbackRecord, 1)
	sh2.broadcastNotification = func(amt uint64, comment string) {
		notified <- callbackRecord{amount: amt, comment: comment}
	}
	sh2.publishZapReceipt = func(*zapReceipt) {}

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		sh2.Run(ctx2)
	}()
	defer func() {
		cancel2()
		wg2.Wait()
	}()

	time.Sleep(50 * time.Millisecond)

	noNotification(t, notified, 100*time.Millisecond)

	// Stop P2 before reading the captured log buffer to avoid racing the
	// logger goroutine.
	cancel2()
	wg2.Wait()

	logs := buf.String()
	if !strings.Contains(logs, "unregistered invoice") {
		t.Fatalf("expected WARN log for dropped pre-restart settlement, got:\n%s", logs)
	}
	if !strings.Contains(logs, hex.EncodeToString(rHashX)) {
		t.Fatalf("expected WARN log to include rHash, got:\n%s", logs)
	}
}

// I2 — First subscription after process start does not replay old settlements
// as new notifications.
func TestSettlementHandler_FirstSubscriptionDoesNotReplayAsNew(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx,
		streamEvent{invoice: settledInvoice(99, []byte{0xdd}, 123)},
	)
	client := &fakeLightningClient{calls: []*subscribeCall{{stream: stream}}}

	sh := NewSettlementHandler(client, "")
	notified := make(chan callbackRecord, 1)
	sh.broadcastNotification = func(amt uint64, comment string) {
		notified <- callbackRecord{amount: amt, comment: comment}
	}
	sh.publishZapReceipt = func(*zapReceipt) {}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sh.Run(ctx)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	time.Sleep(50 * time.Millisecond)
	noNotification(t, notified, 100*time.Millisecond)
}

// --- Callback integration: zap receipt path -------------------------------

func TestSettlementHandler_ZapReceiptPublished(t *testing.T) {
	defer goleak.VerifyNone(t)
	SetLogger(testDiscardLogger(t))

	rHash := []byte{1}
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx,
		streamEvent{invoice: settledInvoice(1, rHash, 50)},
	)
	client := &fakeLightningClient{calls: []*subscribeCall{{stream: stream}}}

	sh := NewSettlementHandler(client, nostr.GeneratePrivateKey())
	sh.broadcastNotification = func(uint64, string) {}
	published := make(chan *zapReceipt, 1)
	sh.publishZapReceipt = func(r *zapReceipt) {
		published <- r
	}

	r := &zapReceipt{
		event: nostr.Event{
			Kind: nostr.KindZap,
			Tags: nostr.Tags{{"description", "{}"}},
		},
		relays: []string{"wss://relay.example"},
	}
	// Register before starting Run so the invoice is never observed before
	// its callback is known.
	sh.subscribeInvoiceSettlements(context.Background(), rHash, "zap", r)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sh.Run(ctx)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	rec, ok := waitForZap(t, published, time.Second)
	if !ok {
		t.Fatal("expected zap receipt publish")
	}
	if rec.event.Kind != nostr.KindZap {
		t.Fatalf("unexpected kind %d", rec.event.Kind)
	}
}
