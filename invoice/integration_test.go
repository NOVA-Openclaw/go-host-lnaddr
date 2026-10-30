package invoice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/btcsuite/btclog"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/nbd-wtf/go-nostr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockLightningClient is a minimal test double for lnrpc.LightningClient.
// It records AddInvoice calls and returns deterministic values.
type mockLightningClient struct {
	lnrpc.LightningClient

	lastInvoice *lnrpc.Invoice
	invoices    []*lnrpc.Invoice
	addErr      error
}

func (f *mockLightningClient) AddInvoice(_ context.Context, in *lnrpc.Invoice,
    _ ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {

	f.lastInvoice = in
	f.invoices = append(f.invoices, in)
	if f.addErr != nil {
		return nil, f.addErr
	}
	return &lnrpc.AddInvoiceResponse{
		PaymentRequest: "lnbc1testpr",
		RHash:          []byte{1, 2, 3, 4},
	}, nil
}

// Implement SubscribeInvoices with a no-op stream for this test. We don't need
// to exercise settlement publishing to validate request processing per spec.
func (f *mockLightningClient) SubscribeInvoices(_ context.Context,
    _ *lnrpc.InvoiceSubscription, _ ...grpc.CallOption) (
    lnrpc.Lightning_SubscribeInvoicesClient, error) {

	return &noopInvoiceStream{}, nil
}

type noopInvoiceStream struct{}

func (n *noopInvoiceStream) Recv() (*lnrpc.Invoice, error) { // blocks forever
	time.Sleep(time.Hour)
	return nil, context.Canceled
}

func (n *noopInvoiceStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (n *noopInvoiceStream) Trailer() metadata.MD         { return metadata.MD{} }
func (n *noopInvoiceStream) CloseSend() error             { return nil }
func (n *noopInvoiceStream) Context() context.Context     { return context.Background() }
func (n *noopInvoiceStream) SendMsg(m interface{}) error  { return nil }
func (n *noopInvoiceStream) RecvMsg(m interface{}) error  { return nil }

// helper to build a valid zap request event JSON string for amount and
// recipient pk.
func makeZapRequest(t *testing.T, amountMsat int, recipientPub string) string {
	t.Helper()

	// Generate a sender key
	sk := nostr.GeneratePrivateKey()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}

	e := nostr.Event{
		PubKey:    pk,
		CreatedAt: nostr.Now(),
		Kind:      nostr.KindZapRequest,
		Tags: nostr.Tags{
			{"relays", "wss://relay.example1", "wss://relay.example2"},
			{"amount", strconv.Itoa(amountMsat)},
			{"p", recipientPub},
		},
		Content: "zap test",
	}
	if err := e.Sign(sk); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	b, err := e.MarshalJSON()
	if err != nil {
		t.Fatalf("Marshal zap: %v", err)
	}
	return string(b)
}

func TestInvoiceCreationWithZapRequest_FollowsSpecBasics(t *testing.T) {
	// Setup: fake LND client and manager
	fl := &mockLightningClient{}
	sh := NewSettlementHandler(fl, "") // empty nsec to skip signing/publish path
	mgr := NewInvoiceManager(&ManagerConfig{LndClient: fl, SettlementHandler: sh})

	// HTTP server with only the invoice handler
	mux := http.NewServeMux()
	cfg := Config{
		MinSendableMsat:  1000,
		MaxSendableMsat:  2_000_000,
		MaxCommentLength: 150,
		SuccessMessage:   "ok",
	}
	mux.HandleFunc("/invoice/", mgr.HandleInvoiceCreation(cfg))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Prepare a valid zap request for amount.
	amount := 21000
	// recipient pubkey is what clients expect from /.well-known/lnurlp as
	// nostrPubkey, but here we just need any hex key.
	recSk := nostr.GeneratePrivateKey()
	recPk, _ := nostr.GetPublicKey(recSk)
	zapJSON := makeZapRequest(t, amount, recPk)

	// Call /invoice with matching amount and nostr event
	u, _ := url.Parse(ts.URL + "/invoice/")
	q := u.Query()
	q.Set("amount", strconv.Itoa(amount))
	q.Set("nostr", zapJSON) // handler expects raw JSON (already string); http lib will encode
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("GET invoice: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(b))
	}

	var inv Invoice
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		t.Fatalf("decode invoice: %v", err)
	}
	if inv.Pr == "" {
		t.Fatalf("expected pr to be set")
	}

	// Assert LND AddInvoice was called with description exactly the zap
	// JSON.
	if fl.lastInvoice == nil {
		t.Fatalf("AddInvoice was not called")
	}
	if fl.lastInvoice.Memo != zapJSON {
		t.Fatalf("expected description to be zap JSON; got %q",
			fl.lastInvoice.Memo)
	}

	// And that DescriptionHash is SHA256(description)
	h := sha256.Sum256([]byte(zapJSON))
	if !bytes.Equal(fl.lastInvoice.DescriptionHash, h[:]) {
		t.Fatalf("description hash mismatch: got %x want %x",
			fl.lastInvoice.DescriptionHash, h[:])
	}
}

func TestInvoiceCreationWithZapRequest_AmountMismatchIs400(t *testing.T) {
	fl := &mockLightningClient{}
	sh := NewSettlementHandler(fl, "")
	mgr := NewInvoiceManager(&ManagerConfig{
		LndClient:         fl,
		SettlementHandler: sh},
	)

	mux := http.NewServeMux()
	cfg := Config{MinSendableMsat: 1, MaxSendableMsat: 100000000}
	mux.HandleFunc("/invoice/", mgr.HandleInvoiceCreation(cfg))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// build zap for 1000 msat but query uses 2000 msat
	recPk, _ := nostr.GetPublicKey(nostr.GeneratePrivateKey())
	zapJSON := makeZapRequest(t, 1000, recPk)

	u, _ := url.Parse(ts.URL + "/invoice/")
	q := u.Query()
	q.Set("amount", "2000")
	q.Set("nostr", zapJSON)
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, string(b))
	}
}

// --- Group C: LND request carries Private ---

func discardLogger(t *testing.T) btclog.Logger {
	t.Helper()
	return btclog.NewBackend(io.Discard).Logger("TEST")
}

func setupInvoiceTest(t *testing.T, privateRouteHints bool) (*mockLightningClient, *httptest.Server) {
	t.Helper()
	SetLogger(discardLogger(t))
	fl := &mockLightningClient{}
	sh := NewSettlementHandler(fl, "")
	mgr := NewInvoiceManager(&ManagerConfig{
		LndClient:         fl,
		SettlementHandler: sh,
	})
	mux := http.NewServeMux()
	cfg := Config{
		MinSendableMsat:   1000,
		MaxSendableMsat:   2_000_000,
		MaxCommentLength:  150,
		SuccessMessage:    "ok",
		PrivateRouteHints: privateRouteHints,
	}
	mux.HandleFunc("/invoice/", mgr.HandleInvoiceCreation(cfg))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return fl, ts
}

func getInvoice(t *testing.T, ts *httptest.Server, amount int, nostr ...string) *http.Response {
	t.Helper()
	u, _ := url.Parse(ts.URL + "/invoice/")
	q := u.Query()
	q.Set("amount", strconv.Itoa(amount))
	if len(nostr) > 0 {
		q.Set("nostr", nostr[0])
	}
	u.RawQuery = q.Encode()
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("GET invoice: %v", err)
	}
	return resp
}

func TestInvoiceCreation_PrivateRouteHintsTrue_SetsPrivate(t *testing.T) {
	fl, ts := setupInvoiceTest(t, true)

	resp := getInvoice(t, ts, 21000)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(b))
	}

	if fl.lastInvoice == nil {
		t.Fatalf("AddInvoice was not called")
	}
	if !fl.lastInvoice.Private {
		t.Fatalf("expected Invoice.Private true, got false")
	}
}

func TestInvoiceCreation_PrivateRouteHintsFalse_DefaultsOff(t *testing.T) {
	fl, ts := setupInvoiceTest(t, false)

	resp := getInvoice(t, ts, 21000)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(b))
	}

	if fl.lastInvoice.Private {
		t.Fatalf("expected Invoice.Private false, got true")
	}
}

func TestInvoiceCreation_PrivateRouteHintsTrue_ZapRequest(t *testing.T) {
	fl, ts := setupInvoiceTest(t, true)

	amount := 21000
	recPk, _ := nostr.GetPublicKey(nostr.GeneratePrivateKey())
	zapJSON := makeZapRequest(t, amount, recPk)

	resp := getInvoice(t, ts, amount, zapJSON)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(b))
	}

	if fl.lastInvoice == nil {
		t.Fatalf("AddInvoice was not called")
	}
	if !fl.lastInvoice.Private {
		t.Fatalf("expected Invoice.Private true for zap request")
	}
	if fl.lastInvoice.Memo != zapJSON {
		t.Fatalf("expected zap JSON memo, got %q", fl.lastInvoice.Memo)
	}
	h := sha256.Sum256([]byte(zapJSON))
	if !bytes.Equal(fl.lastInvoice.DescriptionHash, h[:]) {
		t.Fatalf("description hash mismatch")
	}
}

func TestInvoiceCreation_OnlyPrivateDiffers(t *testing.T) {
	// Issue the same plain request twice, once with PrivateRouteHints true
	// and once false; verify every field except Private is identical.
	flTrue, tsTrue := setupInvoiceTest(t, true)
	respTrue := getInvoice(t, tsTrue, 21000)
	defer respTrue.Body.Close()
	if respTrue.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(respTrue.Body)
		t.Fatalf("true cfg: expected 201, got %d: %s", respTrue.StatusCode, string(b))
	}
	invTrue := flTrue.lastInvoice

	flFalse, tsFalse := setupInvoiceTest(t, false)
	respFalse := getInvoice(t, tsFalse, 21000)
	defer respFalse.Body.Close()
	if respFalse.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(respFalse.Body)
		t.Fatalf("false cfg: expected 201, got %d: %s", respFalse.StatusCode, string(b))
	}
	invFalse := flFalse.lastInvoice

	if invTrue.ValueMsat != invFalse.ValueMsat {
		t.Fatalf("ValueMsat differs: %d vs %d", invTrue.ValueMsat, invFalse.ValueMsat)
	}
	if invTrue.Memo != invFalse.Memo {
		t.Fatalf("Memo differs")
	}
	if !bytes.Equal(invTrue.DescriptionHash, invFalse.DescriptionHash) {
		t.Fatalf("DescriptionHash differs")
	}
	if invTrue.Private == invFalse.Private {
		t.Fatalf("expected Private to differ, both %v", invTrue.Private)
	}
}

func TestInvoiceCreation_PrivateRouteHintsTrue_NoRouteHintsPopulated(t *testing.T) {
	fl, ts := setupInvoiceTest(t, true)

	resp := getInvoice(t, ts, 21000)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(b))
	}

	if len(fl.lastInvoice.RouteHints) != 0 {
		t.Fatalf("expected no RouteHints to be populated by the server, got %v",
			fl.lastInvoice.RouteHints)
	}
}

func TestInvoiceCreation_PrivateRouteHints_NoStateLeak(t *testing.T) {
	fl, ts := setupInvoiceTest(t, true)

	resp1 := getInvoice(t, ts, 21000)
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp1.Body)
		t.Fatalf("first request: expected 201, got %d: %s", resp1.StatusCode, string(b))
	}

	resp2 := getInvoice(t, ts, 21000)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("second request: expected 201, got %d: %s", resp2.StatusCode, string(b))
	}

	if len(fl.invoices) != 2 {
		t.Fatalf("expected 2 AddInvoice calls, got %d", len(fl.invoices))
	}
	if !fl.invoices[0].Private || !fl.invoices[1].Private {
		t.Fatalf("expected both invoices to have Private=true")
	}
}

func TestInvoiceCreation_PrivateRouteHintsTrue_AddInvoiceError(t *testing.T) {
	SetLogger(discardLogger(t))
	fl := &mockLightningClient{addErr: fmt.Errorf("lnd offline")}
	sh := NewSettlementHandler(fl, "")
	mgr := NewInvoiceManager(&ManagerConfig{
		LndClient:         fl,
		SettlementHandler: sh,
	})
	mux := http.NewServeMux()
	cfg := Config{
		MinSendableMsat:   1000,
		MaxSendableMsat:   2_000_000,
		MaxCommentLength:  150,
		PrivateRouteHints: true,
	}
	mux.HandleFunc("/invoice/", mgr.HandleInvoiceCreation(cfg))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := getInvoice(t, ts, 21000)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 on AddInvoice error, got %d: %s", resp.StatusCode, string(b))
	}

	if fl.lastInvoice == nil || !fl.lastInvoice.Private {
		t.Fatalf("expected Private=true set on the AddInvoice request before error")
	}
}
