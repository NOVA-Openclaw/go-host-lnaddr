# Test Case Design — SE Run #1431 / nova-workspace#284
**Component:** `go-host-lnaddr` fork (upstream `hieblmi/go-host-lnaddr` v0.1.1 @ `b92fc83`) —
`invoice/invoice_manager.go` (private invoices), `invoice/settlement_handler.go` (settlement
stream), `main.go` (`ServerConfig`), and the `lnaddr-nova`/`lnaddr-valid` systemd units on the
Services host.
**Author:** Gem (QA) — SE run #1431 step 3
**Status:** DRAFT — test design precedes step 4 implementation; no code has been written yet.

## Background

Lightning Address / zap invoices served by `go-host-lnaddr` carry no private route hints, so
payers can't route to the node's only channel (Olympus, private) — nova-workspace#284.

Step 2 (`notes/se1431-step2-validation.md`, Coder, 2026-10-29) validated the plan against upstream
source, LND docs, and the Services host:

1. `invoice/invoice_manager.go:284` builds `lnrpc.Invoice` without `Private`; no config option
   exists. Add `PrivateRouteHints bool` to `ServerConfig` (default `false`, NOVA's deployed config
   sets `true`); when true, set `invoice.Private = true` before `AddInvoice`.
2. `invoice/settlement_handler.go:69-80` opens one `SubscribeInvoices` stream **per invoice** and,
   on any `stream.Recv()` error, logs and returns **without reconnecting** — this is what killed
   settlement logging on 2026-08-11 (`invoice stream error: EOF`). Replace with **one** persistent
   stream that reconnects with backoff.
3. Unit-file cleanup: the **system** units (`/etc/systemd/system/lnaddr-{nova,valid}.service`, ports
   9990/9991) are the real, active, `Restart=on-failure` units; the **user** units are stale dead
   leftovers. Unit `Documentation=` points at the wrong fork (`andrerfneves/...` instead of
   `hieblmi/...`), and `After=lnd.service` is dangling (no `lnd.service` exists on the Services
   host; LND is reached remotely).
4. Deploy (Graybeard, sudo) and verify **without spending**: decode a fresh invoice from each
   address and confirm a route hint through Olympus (ZEUS LSP, pubkey `031b3013…`). The only
   real end-to-end check is a live zap from I)ruid — never a test payment made by this suite.

## Acceptance Criteria

- **AC1** — `PrivateRouteHints` true causes the `lnrpc.Invoice` passed to `AddInvoice` to have
  `Private == true`; false/absent leaves it `false` (zero value, matching today's behavior).
- **AC2** — The config loader (`loadConfig`, JSON and TOML) parses `PrivateRouteHints` correctly,
  including documented stdlib quirks (encoding/json's case-insensitive key fallback vs.
  BurntSushi/toml's case-sensitive matching; both formats silently ignore unmapped/misspelled keys
  by default) and rejects type-mismatched values with a non-nil error.
- **AC3** — Settlement notification uses **one** persistent `SubscribeInvoices` stream that
  reconnects with bounded backoff after `EOF`/`Unavailable`/other transient stream errors, does
  **not** reconnect-loop on our own deliberate shutdown (`ctx` cancel), delivers **no duplicate**
  settlement notifications across a reconnect, and **does** deliver settlement notifications for
  invoices that settle during a disconnect window.
- **AC4** — Graceful shutdown terminates the settlement goroutine(s) with no leaked goroutines.
- **AC5** — `lnaddr-nova.service` / `lnaddr-valid.service` pass `systemd-analyze verify` cleanly,
  have no dangling `After=`/`Requires=` target, correct `Documentation=`, and the stale user units
  are removed.
- **AC6 (manual/deploy verification, no payment)** — a freshly decoded invoice from each Lightning
  Address exposes a route hint whose `pubkey` matches the Olympus/ZEUS LSP channel partner, with
  fee/CLTV parameters matching Olympus's current channel policy.

## Test Fixture Conventions

- **No live LND anywhere in the automated suite.** All LND interaction goes through a stub
  implementing `lnrpc.LightningClient` — extend the existing `mockLightningClient` pattern already
  in `invoice/integration_test.go` (it already stubs `AddInvoice` and `SubscribeInvoices` via
  `noopInvoiceStream`) — plus a scriptable fake `lnrpc.Lightning_SubscribeInvoicesClient` whose
  `Recv()` returns a pre-programmed sequence of invoices and/or errors (`io.EOF`,
  `status.Error(codes.Unavailable, ...)`, `context.Canceled`) so reconnect tests are deterministic.
- **Config-parsing tests** load fixture bytes from `t.TempDir()` files, never real deploy config
  paths. `loadConfig` is currently unexported in package `main`; config TCs either live in
  `main_test.go` calling it directly, or — if the implementer keeps it unexported and test access
  is awkward — a thin exported wrapper/alias is acceptable. Flag for step 4: confirm `loadConfig`
  (or an equivalent) is reachable from a test in the same package.
- **Backoff must be testable without real sleep durations.** The reconnect loop needs an
  injectable clock/sleep seam (e.g., a `backoff func(attempt int) time.Duration` field, or a short
  configured base/max for tests) so the suite doesn't spend wall-clock time proportional to
  production backoff values. Flag for step 4 as an implementation requirement driven by these TCs.
- **Goroutine-leak checks** use `go.uber.org/goleak` (already present in `go.sum`) via
  `goleak.VerifyNone(t)`, or an explicit `runtime.NumGoroutine()` before/after delta with a bounded
  settle wait.
- **systemd unit TCs** run `systemd-analyze verify` (read-only, non-destructive) against the
  deployed unit files or a read-only copy pulled for inspection. No `systemctl start/stop/restart`
  is part of these checks — the actual cutover restart is the separate deploy step, done by
  Graybeard with sudo.
- **No payments, ever, anywhere in this suite or its manual checklist** (Blockhenge-style no-spend
  rule). The Olympus route-hint verification is decode-only. The only acceptable end-to-end payment
  confirmation is a live zap from I)ruid, performed by a human, outside this test plan's scope.

---

## Test Cases

### Group A — Config: `PrivateRouteHints` in JSON (Plan item 1, AC2)

**A1 — Absent key defaults to `false`**
Setup: JSON config fixture identical to `sample-config.json` but with no `PrivateRouteHints` key.
Expected: `loadConfig` succeeds; `ServerConfig.PrivateRouteHints == false`. Backward-compat guard
for every config already deployed without this field. Strict.

**A2 — Explicit `true`**
Expected: parses to `true`, no error. Strict.

**A3 — Explicit `false`**
Expected: parses to `false`, no error. Strict.

**A4 — Explicit JSON `null`**
Setup: `"PrivateRouteHints": null`.
Expected: per `encoding/json` semantics, JSON `null` is a no-op for a non-pointer `bool` field —
no error, field retains its zero value `false`. Strict (documents real stdlib behavior).

**A5 — Wrong type: string `"true"`**
Expected: `loadConfig` returns a non-nil error referencing a type mismatch on the field; the
returned `ServerConfig` is not used further (isolate: assert on the error, not on log output).
Strict.

**A6 — Wrong type: number `1`**
Expected: non-nil error (bool field, numeric JSON value). Strict.

**A7 — Wrong type: array `[]`**
Expected: non-nil error. Strict.

**A8 — Wrong type: object `{}`**
Expected: non-nil error. Strict.

**A9 — Case variant, lowercase key `"privateroutehints": true`**
Expected: document the ACTUAL `encoding/json` behavior — Go's unmarshaler matches object keys to
struct tags with an exact-match-preferred, case-insensitive-fallback rule, so this is expected to
**match** `PrivateRouteHints` and set it `true`. Assert the observed behavior explicitly; call out
in review that this is a stdlib quirk, not an intentional feature. Strict.

**A10 — Case variant, all-caps key `"PRIVATEROUTEHINTS": true`**
Same reasoning/expectation as A9. Strict.

**A11 — Misspelled key `"PrivatRouteHints": true` (missing an `e`) — HIGH SEVERITY**
Expected: silently ignored (encoding/json does not error on unmapped keys by default);
`PrivateRouteHints` stays `false`, `loadConfig` returns **no error**. This reproduces the exact bug
class the issue exists to fix — an operator believes the flag is set, but a typo means it silently
isn't. Strict, and flagged in Decisions below as a finding beyond the literal plan ask.

**A12 — Duplicate key, conflicting values (`PrivateRouteHints` appears twice in one object)**
Expected: per JSON/Go decode order, the last occurrence wins. Assert actual behavior matches.
Strict.

**A13 — Malformed JSON elsewhere in the file (trailing comma) alongside a valid `PrivateRouteHints` line**
Expected: whole-file parse error from `loadConfig`; field-level concerns are never reached. Strict.

**A14 — UTF-8 BOM-prefixed file with `PrivateRouteHints: true`**
Expected: document actual behavior — `encoding/json` does not strip a leading BOM by default, so
this is expected to fail to parse. Treat as informational (pre-existing format quirk, not unique to
this field) rather than a required fix. Exploratory.

**A15 — Large-but-valid file (several hundred KB of other valid fields) with `PrivateRouteHints: true`**
Expected: parses successfully; soft timing bound (<1s) — guards against an accidental
performance regression from the new field. Exploratory (timing is advisory).

**A16 — Round-trip regression: full `sample-config.json` + `PrivateRouteHints` still parses correctly**
Expected: every pre-existing field in `sample-config.json` round-trips unchanged; new field does
not disturb unrelated parsing. Strict.

---

### Group B — Config: `PrivateRouteHints` in TOML (Plan item 1, AC2)

TOML (BurntSushi) differs from JSON in two load-bearing ways worth testing explicitly: key matching
against struct tags is **case-sensitive**, and duplicate keys in the same table are a **parse
error** (not "last wins").

**B1 — Absent key defaults to `false`**
Expected: `loadConfig` succeeds; `PrivateRouteHints == false`. Strict.

**B2 — Explicit `PrivateRouteHints = true`**
Expected: parses to `true`. Strict.

**B3 — Explicit `PrivateRouteHints = false`**
Expected: parses to `false`. Strict.

**B4 — Wrong type: string `PrivateRouteHints = "true"`**
Expected: `toml.Unmarshal` returns a non-nil type-mismatch error (TOML string vs. Go `bool`).
Strict.

**B5 — Wrong type: integer `PrivateRouteHints = 1`**
Expected: non-nil error — TOML has no bool/int coercion. Strict.

**B6 — Wrong type: array `PrivateRouteHints = [true]`**
Expected: non-nil error. Strict.

**B7 — Wrong type: inline table `PrivateRouteHints = { }`**
Expected: non-nil error. Strict.

**B8 — Case variant, lowercase key `privateroutehints = true` — CONTRAST WITH A9**
Expected: BurntSushi/toml key matching is case-sensitive against the `toml:"PrivateRouteHints"`
tag, so this key is **not** mapped; no error (TOML doesn't error on unmapped keys by default);
`PrivateRouteHints` stays `false`. Document explicitly that the *same* lowercase key behaves
**differently** across the two config formats (JSON: matches per A9; TOML: silently ignored) — this
divergence is itself a footgun worth calling out to whoever writes the NOVA deploy configs. Strict.

**B9 — Misspelled key `PrivateRouteHint = true` (no trailing `s`) — HIGH SEVERITY**
Expected: silently ignored, no error, field stays `false`. Same bug class and severity as A11.
Strict, flagged in Decisions.

**B10 — Duplicate key in the same table**
Setup: `PrivateRouteHints = true` followed later by `PrivateRouteHints = false` in the same
top-level table.
Expected: TOML spec disallows duplicate keys; `toml.Unmarshal` returns a **parse error** (not
"last wins" — this is the opposite of A12's JSON behavior, worth contrasting explicitly). Strict.

**B11 — Key defined in the wrong section (`[Nostr]` instead of top level)**
Setup: `PrivateRouteHints = true` nested under `[Nostr]`.
Expected: does not populate the top-level `ServerConfig.PrivateRouteHints` (stays `false`); does
not corrupt `NostrConfig` decoding either (no error, extra key ignored within that sub-table).
Strict.

**B12 — Malformed TOML elsewhere (unterminated string) alongside a valid `PrivateRouteHints` line**
Expected: whole-file parse error. Strict.

**B13 — Trailing inline comment: `PrivateRouteHints = true # enable private route hints`**
Expected: parses to `true`; comment correctly stripped. Strict (guards comment-handling
regressions near the new key).

**B14 — Round-trip regression: full `sample-config.toml` + `PrivateRouteHints` still parses correctly**
Expected: every pre-existing field round-trips unchanged. Strict.

---

### Group C — LND request carries `Private` (Plan item 1 end-to-end, AC1)

Drive these through the **public HTTP handler** (`mgr.HandleInvoiceCreation(cfg)`, exactly the
pattern already used in `invoice/integration_test.go`) against a stub `lnrpc.LightningClient` that
captures the `*lnrpc.Invoice` passed to `AddInvoice`. This keeps the tests agnostic to exactly
where in the config→`Params`→`lnrpc.Invoice` chain the implementer threads the new field.

**C1 — `PrivateRouteHints` true → `Invoice.Private == true` (plain, non-zap request)**
Strict.

**C2 — `PrivateRouteHints` false/absent → `Invoice.Private == false` (default-off regression guard)**
Confirms upstream's default stays off; only NOVA's deployed config turns it on. Strict.

**C3 — `PrivateRouteHints` true with a zap-request (nostr) invoice**
Expected: `Invoice.Private == true` AND zap-specific fields (`Memo == zapJSON`,
`DescriptionHash == sha256(zapJSON)`) remain correct — confirms the new field doesn't interfere
with the zap path. Strict.

**C4 — Isolation: only `Private` differs between flag-on and flag-off requests**
Setup: identical request params, once with the flag true and once false.
Expected: `ValueMsat`, `Memo`, `DescriptionHash` are bit-for-bit identical between the two
captured invoices; only `Private` differs. Strict.

**C5 — No explicit `RouteHints` population**
Expected: `Invoice.RouteHints` stays `nil`/empty even when `PrivateRouteHints` is true — per the
step-2 notes, LND auto-generates hop hints from `Private: true`; the fix should not also
hand-construct a `RouteHints` list (that would be redundant and could be stale/wrong). Strict.

**C6 — No state leak across sequential requests**
Setup: two sequential HTTP requests against the same running manager/config.
Expected: both captured invoices reflect the same configured `Private` value; no toggling or
drift between requests. Strict.

**C7 — AddInvoice error path unaffected by `Private`**
Setup: stub `AddInvoice` returns an error, with `PrivateRouteHints` true.
Expected: existing 400/error-handling behavior in `HandleInvoiceCreation` is unchanged — Private
is set on the request object before the call, and the error path doesn't change because of it.
Strict (regression guard on existing error branch).

---

### Group D — Manual/deploy verification (Plan item 5) — **NOT part of `go test ./...`, NO PAYMENTS**

This group is a post-deploy checklist run once, by a human or supervised agent, after Graybeard's
sudo restart. Every step is decode-only / read-only against the live node. **No invoice in this
group is ever paid.**

**Restart-window caveat (see Group I):** any zap invoice issued before Graybeard's restart and
settled during or after the restart window falls under Group I's accepted-loss behavior (I1) — no
zap receipt is expected for it, and this checklist does not attempt to verify that (no payments).
This is expected behavior, not a regression to investigate.

**D1 — Decode a fresh invoice from `nova@<domain>`**
Request one invoice (HTTP `GET /invoice/?amount=...`, no payment), decode the returned bolt11 with
a read-only decoder (`lncli decodepayreq` or equivalent). Expected: `route_hints` is present and
non-empty.

**D2 — Hop-hint pubkey matches Olympus/ZEUS LSP**
Expected: at least one hop hint's `pub_key` matches Olympus's published pubkey (`031b3013…` per
the scope comment).

**D3 — Hop-hint fee/CLTV parameters match Olympus's current channel policy**
Cross-check `fee_base_msat`, `fee_proportional_millionths`, `cltv_expiry_delta` against a
read-only channel-policy query (e.g. `lncli getchaninfo`) — no channel mutation. Expected: hint
params match the live policy (a stale hint would be present-but-unusable).

**D4 — Repeat D1-D3 for `valid@<domain>`** (port 9991 instance).

**D5 — Contrast with a pre-fix invoice (if still available)**
Expected: a pre-deploy invoice (decoded and cached before the restart, if one exists) has NO route
hints — confirms the fix, not coincidental LND state, caused the change.

**D6 — Blast-radius check**
Expected: invoice amount, description hash, and expiry are unchanged from pre-fix behavior —
only the hint/private behavior changed.

**D7 — Explicit negative confirmation**
No `lncli payinvoice`, no zap, no wallet-initiated payment of any kind is executed during D1-D6 by
this agent or any automated script. The only acceptable end-to-end payment confirmation is a live
zap from I)ruid, performed by a human, outside this test plan's scope.

---

### Group E — Settlement stream: reconnect behavior (Plan item 2, AC3)

**Assumed interface (implementation note for step 4, to confirm):** `SettlementHandler` exposes a
method such as `Run(ctx context.Context)` that opens one `SubscribeInvoices` stream and loops:
on a non-`ctx`-caused `stream.Recv()` error, discards the stream, applies a backoff delay, and
re-subscribes; on `ctx.Done()` it exits cleanly without treating that as a reconnect trigger. Tests
use a fake `lnrpc.LightningClient` whose `SubscribeInvoices` returns a scripted sequence of fake
stream objects, each a stub `Lightning_SubscribeInvoicesClient` with a programmable `Recv()`.

**E1 — `Recv()` returns `io.EOF` → reconnects**
Expected: handler does not exit permanently; calls `SubscribeInvoices` again after a backoff
delay. Strict.

**E2 — `Recv()` returns gRPC `Unavailable` → reconnects**
Same expectation as E1. Strict.

**E3 — `Recv()` returns server-side `Canceled` (not our own ctx) → reconnects**
Distinguishes "the stream broke" from "we're shutting down" (contrast with E4). Strict.

**E4 — Our own `ctx` is canceled (deliberate shutdown) → exits cleanly, does NOT reconnect**
Expected: no further `SubscribeInvoices` calls after cancellation; `Run` returns/signals done
promptly. Negative case for E1-E3. Strict.

**E5 — Resubscribe attempt itself fails (`SubscribeInvoices` call errors, not just `Recv()`)**
Expected: handler retries with backoff rather than giving up permanently — must not silently die
after one failed resubscribe either, which would just move the original bug one level up. Strict.

**E6 — Successful resubscribe after one failed attempt**
Expected: once `SubscribeInvoices` succeeds again, invoices on the new stream are processed
normally (cross-reference Group F for dedup/no-miss). Strict.

**E7 — Backoff lower bound**
Expected: first retry delay is `>=` the configured base/minimum backoff — not an immediate
hot-loop retry (guards against a reconnect storm hammering LND). Strict.

**E8 — Backoff upper bound**
Expected: delay does not exceed the configured maximum backoff even after many consecutive
failures. Strict.

**E9 — Backoff resets after a successful reconnect**
Expected: after a period of successful stream operation, a subsequent failure's first retry delay
returns to the base delay, not continuing from a previous peak. Strict.

**E10 — Rapid flapping (20 consecutive immediate failures, accelerated/injected backoff clock)**
Expected: no panic, no goroutine leak per attempt (cross-reference Group G), and total wall time
for N attempts is consistent with N × backoff (not near-zero, i.e. not busy-looping). Strict.

---

### Group F — Settlement stream: no duplicate / no missed settlements across reconnect (Plan item 2, AC3)

**F1 — Baseline happy path (no reconnect)**
Expected: an invoice settles once; its registered callback fires exactly once. Regression guard
before the harder reconnect cases. Strict.

**F2 — Settlement during a disconnect window is still delivered ("no missed settlement")**
Setup: invoice A is registered; stream disconnects (EOF) BEFORE A settles; stream reconnects;
A then settles on the NEW stream.
Expected: A's settlement callback still fires exactly once. This is the literal requirement and
depends on index/backlog continuity (see F4) or an equivalent mechanism. Strict.

**F3 — No duplicate notification across reconnect ("no duplicate settlement")**
Setup: invoice B settles and is processed; the stream THEN disconnects and reconnects (using
whatever backlog mechanism satisfies F2).
Expected: B's callback does NOT fire a second time after reconnect — no duplicate notification, no
duplicate zap-receipt publish. Natural counterpart to F2: solving F2 with backlog replay creates
the real risk of this failing, so both must be tested together. Strict.

**F4 — Reconnect subscribes with `InvoiceSubscription.SettleIndex` advanced, not zero**
Setup: capture the `*lnrpc.InvoiceSubscription` argument on the SECOND `SubscribeInvoices` call
after a reconnect.
Expected: `SettleIndex` reflects the last successfully processed index, not a hardcoded `0` — a
zero `SettleIndex` means "no backlog, new events only" per LND semantics and would silently
reproduce F2's bug. Strict. (This is the mechanism-level test backing F2's behavioral assertion.)

**F5 — Unregistered/foreign invoice during backlog replay is ignored**
Setup: a settled invoice with an `rHash` this handler never registered (e.g., belongs to the
OTHER Lightning Address's manager instance) appears in the backlog replay.
Expected: no callback fires, no panic/crash on an unknown `rHash`. Strict.

**F6 — Mixed settle outcomes across a disconnect**
Setup: 3+ invoices registered before a disconnect; two settle during the disconnect window, one
does not.
Expected: after reconnect, exactly the two settled invoices get exactly one callback each; the
third (still pending) gets none yet and remains correctly tracked for a later settlement. Strict.

**F7 — Defense in depth: duplicate delivery from the stream itself (independent of reconnect)**
Setup: a pathological/duplicate fake stream delivers the same settled invoice twice in direct
succession (not via a reconnect boundary at all).
Expected: handler-level dedup (e.g., tracking already-notified `rHash`es) still prevents a second
callback — index-based continuation (F4) alone doesn't guarantee LND delivers exactly once.
Exploratory (broader reading of "no duplicate," included because the failure mode — e.g.
double-publishing a zap receipt to relays — is user-visible; see Decisions).

---

### Group G — Graceful shutdown / goroutine hygiene (Plan item 2, implied robustness requirement, AC4)

**G1 — Shutdown while idle in `Recv()`**
Expected: canceling `ctx` while blocked in `Recv()` makes the settlement goroutine exit within a
bounded time (e.g. <1s in test) — no indefinite block. Strict.

**G2 — Shutdown mid-backoff-sleep**
Expected: canceling `ctx` while a backoff wait is in progress also exits promptly — the backoff
wait must be interruptible (e.g. `select` on a timer and `ctx.Done()`, not a bare `time.Sleep`),
otherwise shutdown can stall up to the full backoff duration. Strict.

**G3 — No leaked goroutines after shutdown**
Expected: `goleak.VerifyNone(t)` (or an explicit `runtime.NumGoroutine()` before/after delta) shows
no leaked goroutines attributable to the settlement handler's main receive loop. Strict.
Footnote: `publishZapReceipt` is launched fire-and-forget (`go publishZapReceipt(zapReceipt)`) with
no shutdown hook today — pre-existing behavior, out of scope for this issue's fix, but worth
flagging (see Decisions) since it's adjacent to the same file.

**G4 — Repeated start/stop cycles don't accumulate leaks**
Setup: start the handler against a fresh fake client, shut it down, repeat 5+ times in one test.
Expected: no cumulative goroutine growth across cycles — protects both the test harness and any
future config-reload-style reuse of the handler. Strict.

**G5 — Shutdown with in-flight unsettled registrations**
Setup: multiple invoices registered but not yet settled when shutdown is triggered.
Expected: no panic; no spurious callbacks delivered for still-pending invoices after the handler
has stopped. Strict.

---

### Group H — systemd unit files (Plan item 3, AC5) — manual/host-level, read-only, no restarts here

**H1 — `systemd-analyze verify lnaddr-nova.service` exits 0, no warnings**

**H2 — `systemd-analyze verify lnaddr-valid.service` exits 0, no warnings**

**H3 — No dangling `After=`/`Wants=`/`Requires=` target**
Expected: confirm `lnd.service` is removed from `After=` (per step-2 finding — no `lnd.service`
exists on the Services host; LND is remote), OR, if intentionally kept, confirm it was a deliberate
documented decision rather than an oversight — `systemd-analyze verify` is lenient about missing
optional `After=` targets by design, so "verify passes" alone is necessary but not sufficient
evidence the dangling dependency was actually addressed.

**H4 — `Documentation=` URL corrected**
Expected: both unit files point at `hieblmi/go-host-lnaddr` (or the NOVA fork, once it exists),
not the stale `andrerfneves/go-host-lnaddr` mismatch noted in step 2.

**H5 — Unrelated unit fields unchanged (regression guard)**
Expected: both units retain `Restart=on-failure`, `User=nova`, `ExecStart` pointing at
`/home/nova/go/bin/lnaddr`, and the correct distinct `--config`/port per instance (9990 nova,
9991 valid) — the cleanup should touch only the two stated fixes (Documentation, dangling After=),
not working behavior.

**H6 — Stale user units removed**
Expected: `~/.config/systemd/user/lnaddr-nova.service` and `lnaddr-valid.service` are absent after
cleanup; `systemctl --user status lnaddr-nova lnaddr-valid` (read-only) reports "not found," not
"inactive (dead)."

**H7 — Post-deploy re-verify**
Expected: re-running `systemd-analyze verify` AFTER the binary swap (post-deploy) still passes —
confirms accompanying `ExecStart`/`WorkingDirectory` path edits (if any) didn't regress verify
status as a side effect.

---

### Group I — Settlement stream: process restart (Plan item 2 extension, AC3 restart edge case)

Project Leadership review (step 4 gap): the invoice registry (in-memory `rHash` → callback/zap-request
mapping) and the settlement handler's connection state live only in process memory. The deploy step
itself restarts `lnaddr-nova`/`lnaddr-valid` — so an invoice created before that restart and settled
after it (or during the restart window) is a real, not hypothetical, case every deploy. These TCs pin
the chosen behavior so it's a documented decision, not an accident of whatever the refactor happens to
do.

**I1 — Settlement for an invoice created before a process restart — CHOSEN BEHAVIOR: accepted loss**
Setup: invoice X is created and registered (rHash → callback) in process instance P1; P1 is stopped
(simulating the deploy restart) before X settles; a fresh process instance P2 starts with an empty
in-memory registry (no prior registration carries over) and begins its settlement subscription per
I2's rule; X then settles.
Expected: P2 has no registered callback for X's `rHash`, so no zap receipt is published for X — this
loss is accepted and documented, not silently swallowed. If P2's settlement stream observes X settling
(e.g., it appears in whatever backlog/notification P2 receives) with no matching registration, the
handler logs a **WARN** including X's `rHash` (reusing the F5 "unregistered rHash" no-crash path, with
the addition of the WARN log so the drop is observable in `journalctl`/logs rather than silent) and
does **not** panic or crash. Strict.

**Recommendation: accepted loss (documented), not persisted-index recovery — with reasoning:**
Recovery via persisted index + `LookupInvoice` was considered and rejected *for this issue's scope*.
The `SettleIndex` is only half the state needed to actually deliver a zap receipt: `publishZapReceipt`
needs the original nostr zap-request JSON, relay list, and description hash that were only ever held
in the in-memory registry entry created at invoice-creation time — none of that is in LND's own
`LookupInvoice` response. Recovering real zap-receipt delivery across a restart would require a new
persistence layer for invoice-creation metadata (not just an index), keyed durably by `rHash`, written
synchronously enough to survive an unexpected crash (not just a planned restart) — that's materially
more surface area than this issue (#284, route hints + reconnect logging) asked for, and risks the
same kind of half-finished-durability bug this issue exists to fix in the stream layer. Accepted loss
is also honest about the actual failure mode today (pre-fix, ALL restarts silently lose in-flight
settlements with no log at all) — the fix only needs to make the loss *visible and bounded* (WARN +
rHash), not eliminate it. I recommend tracking full persisted recovery as an explicit **follow-up
issue** (new in-memory-registry-survives-restart ticket), separate from #284.

**I2 — First subscription after process start does not replay old settlements as new notifications**
Setup: invoice Y settles and its callback fires (zap receipt published) in process instance P1,
*before* a restart; P1 stops; process instance P2 starts fresh (empty registry, per I1) and opens its
first `SubscribeInvoices` call.
Expected: Y's settlement is not re-delivered to P2 as a "new" notification that fires a second
callback/zap receipt — P2 has no registration for Y regardless (satisfies this trivially under I1's
accepted-loss design, since `SettleIndex` starts at a value that does not replay P1's already-handled
backlog as actionable for P2), but assert it explicitly: this is the concrete regression this group
exists to prevent, and it must hold even if a future follow-up (persisted-index recovery, per I1's
recommendation) changes the subscribe-time `SettleIndex` — a persisted index must advance *past*
already-notified settlements, not just past the start of the process. Strict.

**I3 — Group D's deploy-window check documents the expected zap-receipt gap (see Group D note, no new assertion)**
Not a new automated test: a cross-reference confirming Group D's manual checklist, which runs
immediately after Graybeard's restart, explicitly expects and documents (rather than treats as a
surprise) that any zap invoice issued before the restart and paid during or after the restart window
falls under I1's accepted-loss behavior. See the note added to the Group D header. Informational —
documentation cross-check only, not an executable TC.

---

## TC Count by Area

| Group | Area | Count | Automated? |
|---|---|---|---|
| A | JSON config — `PrivateRouteHints` | 16 | Yes (Go unit tests) |
| B | TOML config — `PrivateRouteHints` | 14 | Yes (Go unit tests) |
| C | LND request carries `Private` | 7 | Yes (stub lnrpc client) |
| D | Manual deploy verification (Olympus route hint) | 7 | No — manual, no-spend checklist |
| E | Settlement stream reconnect | 10 | Yes (fake stream/client) |
| F | Settlement stream dedup / no-miss | 7 | Yes (fake stream/client) |
| G | Graceful shutdown / goroutine hygiene | 5 | Yes (goleak / NumGoroutine) |
| H | systemd unit files | 7 | No — manual, read-only host checks |
| I | Settlement stream: process restart | 3 | Partial — I1/I2 Go unit tests (fresh handler instance, empty in-memory registry), I3 manual/deploy-note only |
| **Total** | | **76** | 61 automatable in `go test ./...`, 15 manual/host checklist |

## Done Criteria

**This step (SE #1431 step 3 / test design) is done when:**
1. This document is committed on `se1431/test-design`, covering all 8 groups above.
2. Every Go-level TC (Groups A, B, C, E, F, G — 59 cases) is implementable using only the stub
   `lnrpc.LightningClient` / fake stream objects described here — no live LND dependency, no real
   sleeps beyond an injectable/short backoff seam, no network access required for `go test ./...`.
3. Groups D and H are explicitly scoped as manual/deploy-time checklists, not part of the automated
   suite, so CI is never blocked on hardware/network access to the Services host or live LND node.
4. The step 4 implementer has reviewed this doc and either (a) implements against it as-is, since
   the C-group tests assert through the public HTTP handler rather than internal wiring and so
   tolerate different threading choices for `PrivateRouteHints`, or (b) files a documented deviation
   for any TC that conflicts with a concrete implementation choice (e.g., a different settlement
   handler method name/signature than the assumed `Run(ctx)`).
5. No test in this suite — automated or manual — pays an invoice, requests a real LND connection,
   or mutates the live Services host. (Blockhenge-style no-spend rule, held throughout.)

**The underlying issue (#284) is done — separate, broader bar, outside this step's authority —
only when:** `PrivateRouteHints` ships and is set `true` in NOVA's deployed config (not upstream's
default), the persistent reconnecting `SubscribeInvoices` stream is deployed and running under the
system units, the systemd unit fixes (H1-H6) are live, D1-D6's manual decode checks pass on both
Lightning Addresses, and — optionally, outside any agent's authority — a real zap from I)ruid
confirms end-to-end routing.

## Decisions and Reasoning (what was deliberately left out, and why)

- **No live-LND integration tests, anywhere.** The Blockhenge-style no-spend rule plus the explicit
  "no payments" instruction for this task mean any TC requiring a real settled invoice is
  manual-only (Group D). I did not attempt a regtest/signet LND harness to partially automate D1-D3
  — that's new test infrastructure the step-2-approved plan didn't ask for, and stands a real risk
  of being pointed at the live node by mistake if built carelessly. If a non-production LND harness
  becomes available later, D1-D3 could move into the automated suite against it; that's a follow-up,
  not part of this design.
- **No literal Go test code.** Step 3 is design, not implementation — step 4 writes the actual
  `_test.go` files. Where the architecture is still ambiguous (exactly where `PrivateRouteHints`
  is threaded from `ServerConfig` into `MakeInvoice`; the settlement handler's post-refactor method
  name/signature; the backoff injection seam), I called out explicit assumptions as implementation
  notes rather than silently picking one, so the implementer isn't forced to redesign tests built
  around a guess that turns out wrong. The C-group tests are written to assert through the public
  HTTP handler specifically so they survive any reasonable choice here.
- **Two adversarial findings beyond the literal plan ask (A11/B9): a misspelled config key silently
  defaults to `false` in both JSON and TOML, with no error.** I kept these as required (Strict) TCs
  because they reproduce the exact bug class — an operator believing a route-hint flag is set when
  it silently isn't — that issue #284 exists to fix. I recommend (not require, since strict config
  validation wasn't part of the approved step-2 plan) that the implementer consider
  `json.Decoder.DisallowUnknownFields()` / TOML strict-mode decoding as a follow-up issue, but did
  not fold that recommendation into this issue's done-criteria.
- **B8 explicitly contrasts JSON vs. TOML case-sensitivity, and A12/B10 explicitly contrast
  duplicate-key handling (JSON: last-wins: TOML: parse error).** These aren't separate bugs to fix,
  but they're real format-level divergences that whoever edits NOVA's deploy configs (both a
  `.json` and presumably matching intent in future `.toml`) should know about; documenting them in
  test form makes the divergence a tested fact, not tribal knowledge.
- **Left out:** TCs for `RPCHost`/macaroon/TLS config edge cases, IPv4-vs-IPv6 parsing, or any other
  pre-existing `ServerConfig` field — those are untouched by this issue's plan; adding TCs there
  would be scope creep.
- **Left out:** pinning an exact backoff algorithm or multiplier. The step-2 notes don't specify
  concrete backoff constants yet, so E7-E9 assert only the properties any reasonable implementation
  must have (bounded below, bounded above, resets after success) rather than constraining the
  implementer's choice of algorithm beyond what the approved plan calls for.
- **Left out:** Dockerfile and `.github/workflows` CI changes — not mentioned in any of the plan's 4
  items.
- **Included despite not being a literal bullet in the task (F7):** duplicate delivery from the
  fake stream itself, independent of a reconnect boundary. "No duplicate settlement notifications
  after reconnect" could be read narrowly (only across reconnects) or broadly (dedup robustness in
  general); I included the broader case since it's a low-cost TC against a user-visible failure mode
  (e.g. double-publishing a zap receipt to Nostr relays) that the same backlog-replay mechanism
  (added to satisfy F2) could plausibly reintroduce.
- **Flagged, not turned into a required TC:** `publishZapReceipt`'s fire-and-forget goroutine (no
  shutdown hook) noted under G3 as a pre-existing adjacent issue, left out of AC4's scope since it
  predates this issue and isn't part of the settlement-stream refactor the plan describes.
- **Group D and H are explicitly manual,** per the task's own instruction ("manual/deploy
  verification TC, NO payments") — I did not propose scripting/automating the Olympus decode check
  into CI, since that would require live LND/node credentials in a test runner, which the no-spend,
  no-live-LND constraints for this suite rule out.
- **Process restart (Group I): invoice registry loss is accepted, not silently hidden.** The deploy
  step restarts `lnaddr-nova`/`lnaddr-valid`, and the in-memory `rHash → callback` registry (plus the
  zap-request metadata needed to actually publish a receipt) does not survive that. I recommend (I1)
  documenting this as an accepted loss — WARN-level log with the `rHash` if the drop is observed, no
  panic — rather than building persisted-index + `LookupInvoice` recovery now. Full recovery needs to
  persist invoice-creation metadata (zap-request JSON, relay list), not just LND's `SettleIndex`,
  which is a materially bigger durability project than this issue's scope; I've flagged it as a
  follow-up issue rather than folding it into #284's done-criteria. I2 guards the inverse risk (a
  future persisted-index fix accidentally replaying already-notified settlements as duplicates after
  restart), and I3 just makes sure Group D's post-restart manual checklist doesn't mistake the
  accepted gap for a bug.
