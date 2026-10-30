package invoice

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	"github.com/hieblmi/go-host-lnaddr/notifier"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/nbd-wtf/go-nostr"
)

// defaultBackoff returns the delay before reconnect attempt i. The first
// reconnect (i==0) waits base; each subsequent attempt doubles the delay up
// to max. attempt is capped to avoid int overflow.
func defaultBackoff(attempt int) time.Duration {
	const (
		base = 100 * time.Millisecond
		max  = 30 * time.Second
	)
	if attempt > 20 {
		attempt = 20
	}
	d := base * (1 << attempt)
	if d > max {
		d = max
	}
	return d
}

// settlementRegistration holds the data needed to notify the caller when an
// invoice settles. The notified flag provides handler-level deduplication so
// each registered rHash triggers at most one callback, even if LND delivers
// the same settled invoice more than once (reconnect replay or a pathological
// stream). Registrations are never pruned, so dedup state is bounded by the
// number of invoices created in this process lifetime.
type settlementRegistration struct {
	rHash      []byte
	comment    string
	zapReceipt *zapReceipt
	notified   bool
}

// SettlementHandler listens for invoice settlement and triggers side effects
// like notifications and optional Nostr zap receipts.
//
// It maintains a single persistent SubscribeInvoices stream that reconnects
// with bounded exponential backoff. Invoices are registered via
// subscribeInvoiceSettlements; the handler then watches the stream and invokes
// the registered callback exactly once per rHash.
type SettlementHandler struct {
	lndClient lnrpc.LightningClient
	nsec      string

	// broadcastNotification and publishZapReceipt are injectable seams for
	// tests. In production they default to notifier.BroadcastNotification
	// and publishZapReceipt.
	broadcastNotification func(amount uint64, comment string)
	publishZapReceipt     func(receipt *zapReceipt)

	mu            sync.Mutex
	registrations map[string]*settlementRegistration

	// lastProcessedSettleIndex is only accessed from Run's goroutine, so it
	// does not need the mutex.
	lastProcessedSettleIndex uint64

	// backoff returns the duration to wait before reconnect attempt i.
	// Injectable for tests so they don't sleep for real.
	backoff func(attempt int) time.Duration
}

func NewSettlementHandler(
	lndClient lnrpc.LightningClient, nsec string) *SettlementHandler {

	return &SettlementHandler{
		lndClient:             lndClient,
		nsec:                  nsec,
		broadcastNotification: notifier.BroadcastNotification,
		publishZapReceipt:     publishZapReceipt,
		registrations:         make(map[string]*settlementRegistration),
		backoff:               defaultBackoff,
	}
}

func publishZapReceipt(zapReceipt *zapReceipt) {
	zapctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	wg := sync.WaitGroup{}
	for _, relayAddr := range zapReceipt.relays {
		wg.Add(1)
		go func(relayAddr string) {
			defer wg.Done()
			relay, err := nostr.RelayConnect(zapctx, relayAddr)
			if err != nil {
				log.Warnf("Error connecting to relay %s",
					relayAddr)

				return
			}
			err = relay.Publish(zapctx, zapReceipt.event)
			if err != nil {
				log.Warnf("Error publishing zap receipt to "+
					"relay %s: %s", relayAddr, err)
			} else {
				log.Infof("Published zap receipt to "+
					"relay %s", relayAddr)
			}
			err = relay.Close()
			if err != nil {
				log.Warnf("Error closing relay %s: %s",
					relayAddr, err)
			}
		}(relayAddr)
	}
	wg.Wait()
}

// subscribeInvoiceSettlements registers an invoice for settlement tracking.
// The actual subscription is owned by Run, which must be started once for the
// lifetime of the handler.
func (s *SettlementHandler) subscribeInvoiceSettlements(ctx context.Context,
	rHash []byte, comment string, zapReceipt *zapReceipt) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.registrations[hex.EncodeToString(rHash)] = &settlementRegistration{
		rHash:      rHash,
		comment:    comment,
		zapReceipt: zapReceipt,
	}
	s.mu.Unlock()

	return nil
}

// Run starts the persistent SubscribeInvoices stream. It blocks until ctx is
// canceled, reconnecting with bounded exponential backoff on transient errors.
// Only one Run call per handler is supported.
func (s *SettlementHandler) Run(ctx context.Context) {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		stream, err := s.lndClient.SubscribeInvoices(
			ctx, &lnrpc.InvoiceSubscription{
				SettleIndex: s.lastProcessedSettleIndex,
			},
		)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warnf("subscribe invoices error: %v", err)
			if !s.waitBackoff(ctx, attempt) {
				return
			}
			attempt++
			continue
		}

		// Successful stream: reset the backoff.
		attempt = 0

		for {
			invoice, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warnf("invoice stream error: %v", err)
				if !s.waitBackoff(ctx, attempt) {
					return
				}
				attempt++
				break
			}

			s.handleInvoice(invoice)
		}
	}
}

// waitBackoff sleeps for the backoff duration or returns false if ctx is
// canceled first. This keeps shutdown prompt even during a reconnect wait.
func (s *SettlementHandler) waitBackoff(ctx context.Context, attempt int) bool {
	d := s.backoff(attempt)
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *SettlementHandler) handleInvoice(invoice *lnrpc.Invoice) {
	if invoice.State != lnrpc.Invoice_SETTLED {
		return
	}

	if invoice.SettleIndex > s.lastProcessedSettleIndex {
		s.lastProcessedSettleIndex = invoice.SettleIndex
	}

	rHashHex := hex.EncodeToString(invoice.RHash)

	s.mu.Lock()
	reg, ok := s.registrations[rHashHex]
	s.mu.Unlock()

	if !ok {
		log.Warnf("settlement for unregistered invoice rHash=%s "+
			"settle_index=%d", rHashHex, invoice.SettleIndex)
		return
	}

	if reg.notified {
		// Handler-level dedup: already delivered for this rHash.
		return
	}
	reg.notified = true

	s.broadcastNotification(uint64(invoice.AmtPaidSat), reg.comment)

	if reg.zapReceipt != nil && s.nsec != "" {
		reg.zapReceipt.event.CreatedAt = nostr.Timestamp(invoice.SettleDate)
		reg.zapReceipt.event.Tags = append(
			reg.zapReceipt.event.Tags,
			nostr.Tag{"preimage",
				hex.EncodeToString(invoice.RPreimage)})
		err := reg.zapReceipt.event.Sign(s.nsec)
		if err != nil {
			log.Warnf("Error signing zap receipt: %s", err)
		} else {
			log.Infof("Publishing zap receipt: %+v",
				reg.zapReceipt.event)
			s.publishZapReceipt(reg.zapReceipt)
		}
	}
}

// test helper: reset internal state so a handler can be reused in leak tests.
func (s *SettlementHandler) resetForTest() {
	s.mu.Lock()
	s.registrations = make(map[string]*settlementRegistration)
	s.lastProcessedSettleIndex = 0
	s.mu.Unlock()
}

// isRHashRegistered is a test helper.
func (s *SettlementHandler) isRHashRegistered(rHash []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.registrations[hex.EncodeToString(rHash)]
	return ok
}
