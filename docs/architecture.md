# shuttle architecture

For people who need to understand or modify the shuttle implementation — the broker model, the
three publish paths, and why the code is shaped this way. The operational invariant checklist a
diff is checked against lives in [AGENTS.md](../AGENTS.md); the svdpi/systemc backend boundary and
the full DPI protocol live in [backends.md](backends.md); positioning and usage live in the
[README](../README.md).

## The model everything derives from

**A simulation process is a coroutine, not an OS process** (inherited from weave): at any instant
exactly one process runs C++-side code, it is never preempted, and it suspends only at explicit
weave blocking points. Every guarantee in this document follows from that model; a reader who holds
it can derive rules that are not written down.

The second half of the model is what shuttle adds on top: **publishing is snapshot plus scheduled
handoff**. A publish call locks in the payload and the target set at that instant (an immutable
*frame*), and every target's frame is then handed to a callback entry whose capability was fixed at
registration. Nothing about a frame changes after the call; the only question the framework answers
is when and through which entry each target's callback runs, and what terminal state the attempt
reached. Simulation time is the scarce resource, so the interesting design decisions are all about
which callbacks may consume it and where they are allowed to run.

Consequences that are used everywhere below:

- Broker state (`topics` / `subs` / `pubs`) is plain data with no locks; any check-modify sequence
  without a suspension point in between is atomic. `shared_ptr` reference counts crossing the
  language boundary are safe for the same reason.
- All "background" delivery runs in weave tasks; the broker introduces no threads. FIFO ordering
  and cross-subscriber parallelism are properties of the weave scheduler, not of broker code.
- State that must outlive a suspension has to be consistent *before* the suspension point, because
  from that instant another process may inspect it.

## Boundaries and contracts

```
                      ┌──────────────────────────── user code ────────────────────────────┐
                      │  C++: Publisher<T> subscribe<T> Publication Subscription Trigger  │
                      │  SV : shuttle_publisher #(T) shuttle_subscribe sink(_nb) handle   │
                      └───────────────┬────────────────────────────────────────┬──────────┘
                                      │ public headers (include/shuttle/*.hpp) │ SV package (sv/shuttle_pkg.sv)
        ┌─────────────────────────────▼───────────┐        ┌───────────────────▼───────────┐
        │  broker core (src/broker/)              │        │ shuttle_pkg.sv                │
        │  Topic table / wildcards / type binding │        │  registry / sub / handle      │
        │  three publish schedulings / retained   │<──DPI─>│  trigger / event / publisher  │
        │  C++ API and handles: src/*.cpp         │        │  frame pull + {>>{}} packing  │
        └───────┬────────────────────────▲────────┘        └───────────────────────────────┘
                │ backend-agnostic       │ backend boundary (5-entry detail::backend facade,
     ┌──────────▼─────────┐   ┌──────────┴─────────┐          src/backend/backend.hpp)
     │ backend/svdpi/     │   │ backend/systemc/   │
     │ bridge + DPI face  │   │ no SV bridge:      │
     │ FrameContext+scope │   │ dispatch* throws   │
     └──────────┬─────────┘   └────────────────────┘
                ▼
        weave (task/event/port; both backends)  ← concurrency, single-active-process contract
```

- The broker core knows nothing about DPI or SystemC. It reaches the outside only through the
  5-entry `detail::backend` facade in `src/backend/backend.hpp` (`available` / `dispatch` /
  `dispatch_nb` / `note_retired_delivery` / `take_retired_delivery`). Time is **not** in that
  facade: `Meta::time_fs` and all timeout arithmetic read `weave::now()` directly.
- `src/backend/<backend>/` directories never include each other; there are no
  `SHUTTLE_BACKEND_*` macros. Switching backend = switching directory (selected by
  `add_files("src/backend/<backend>/**.cpp")` in `xmake.lua`). svdpi is the sole exception to
  backend isolation: it is the only FFI-carrying backend, and its `facade.cpp` additionally sees
  broker internals (`broker/broker.hpp`, `detail/publication.hpp`). There will never be a second
  FFI caller.
- The C++ ↔ SV boundary itself — payload passing, scope, the import/export tables, the byte layout
  — is defined authoritatively in [backends.md](backends.md).
- `include/shuttle/detail/abi.hpp` is a second public layer (stable ABI): installed and versioned,
  but user code includes it only indirectly through `publisher.hpp` / `subscription.hpp`, because
  the public templates (`Publisher<T>` / `subscribe<T>`) must see the `pub_*` / `sub_*`
  declarations in the user's translation unit.
- `mailbox.hpp` and `requester.hpp` are header-only components: no `.cpp`, no exported symbols,
  zero broker state. Their state lives in the header objects themselves.

## Terminology

| Term | Meaning |
|---|---|
| frame | the immutable snapshot one publish produces: payload + `Meta{topic_name, type_tag, topic_seq, time_fs, corr_id}`. All three flavors build the frame first |
| target | one subscriber this frame matched. The target set is fixed at the publish call from a registry snapshot |
| entry | the single function a subscriber is invoked through: `blocking` (may consume simulation time) or `nonblocking` (zero-delay) |
| delivery | the broker's attempt to pair one target with one frame; ends in exactly one terminal state (below) |
| delivery unit | the background executor of deliveries: one weave task per frame for `serial`, one per subscriber for `concurrent` |
| chain | the FIFO ordering structure: one per topic (`Topic::chain`, shared by `serial` and `now`'s defer), or one per (topic, subscriber) for `concurrent` (`Topic::sub_chains`) |
| terminal state | a target's outcome for a frame: delivered / retired / failed / deferred / skipped |
| bookkeeping | the per-frame / per-subscriber / per-topic counters for those states |

The SV enums mirror the C++ ones (`Capability::blocking` ↔ `SHUTTLE_CAP_BLOCKING`); on the SV side
a blocking entry is an export task and a nonblocking entry is an export function.

## Key design decisions

### One registration = one subscriber = one entry

The entry a subscriber is invoked through is decided by the function called at registration: C++
`subscribe(fn)` / SV `shuttle_subscribe` = blocking entry; C++ `subscribe(nb(fn))` / SV
`shuttle_subscribe_nb` = nonblocking entry; `serve(...)` follows the same split. There is no
runtime `Capability` parameter anywhere in the public API, and a class is never probed (e.g. with
`$cast` against interface classes) to auto-register both entries.

Why: the entry is a property of the *registration*, not of the class, and it must be visible at the
call site. A runtime parameter would make the entry invisible there and would desynchronise
`subscriber_count()` / `publish_now` behavior between the two languages. A subscriber that needs
both entries registers twice. The rejected alternative — auto-registration of both entries per
class — was removed precisely because it turned capability into a class-level property, which is a
category error against the C++ model.

Observable consequence: `Subscriber` stores exactly one `Capability cap`, and `deliver` reads only
that. The SV bridge's `caps` argument accepts exactly 1 or 2 and rejects everything else.

### Blocking vs nonblocking describes the callback, not the call

`blocking` / `nonblocking` state whether the *callback* may consume simulation time — not whether
the entry blocks its caller. `publish_now`'s synchronous segment invokes only nonblocking entries;
with the default `defer` policy, blocking targets of the same publish still run, but from the
background chain, after `publish_now` returns. On the SV side the two entries are an export task
and an export function.

`NowPolicy` applies only to blocking targets:

| Policy | Behavior | Timing consequence |
|---|---|---|
| `reject` | throws `CapabilityError` (C++) / returns null + last_error (SV) | the frame is never published: no `seq`, retained or counter advances |
| `skip` | drops those targets, counted in `skipped()` | other targets unaffected |
| `defer` (default) | hands the whole frame to the topic's serial chain | those callbacks run after `publish_now` returns |

For nonblocking targets `skip` and `defer` are indistinguishable: both complete before
`publish_now` returns. The hard constraint underneath is DPI: an export *task* may not run inside
an import *function* call chain (`Error-[DPI-INCDIC]`), so `dispatch_now` partitions targets by
capability first and the synchronous loop contains only nonblocking ones.

### The serial chain hands off the tail at publish time — or at trigger time

`publish_serial` and `publish_now(defer)` share one FIFO chain per topic, which yields the
testable law: **background delivery order on a topic equals enqueue order** — immediate-trigger
frames enqueue inside the publish call, delayed frames enqueue at their trigger moment.
`publish_concurrent` uses the same rule on one chain per (topic, subscriber): subscribers run in
parallel, each subscriber's consecutive frames stay strictly FIFO. The two chains never mix:
`serial` frames never enter a per-subscriber chain and vice versa, and no cross-flavor ordering is
promised.

The handoff itself is non-commutative and must happen inside the same non-suspending segment. Each
frame captures the chain-tail event to wait on and installs its own as the new tail:

```cpp
// immediate frame: handoff inside the publish call
auto prev = std::make_shared<weave::Event>(topics[topic].chain);  // tail this frame waits on
auto mine = std::make_shared<weave::Event>();                     // tail this frame releases
topics[topic].chain = *mine;                                      // install

// delayed frame: wait_trigger first, then the same three steps inside the task
```

A delivery unit waits on `prev` and sets `mine` on completion — success or failure. Reading
`topics[topic].chain` at *completion* time instead of capturing it at enqueue time was the
original bug: a concurrent publish installs a different frame's tail, the event this frame should
have set never fires, and every later frame on the topic blocks forever — observed as simulation
time frozen at the last event with no error. `mine->set()` sits outside the `catch`, so a failed
or cancelled unit still releases the chain.

Delayed frames must not hold the tail at publish time. An `after(Event)` delay is unbounded;
holding the tail would pin every later frame on the topic (including frames with no trigger at
all) and `publish_serial(...).wait()` deadlocks permanently — worst when the trigger is satisfied
by a callback of a *later* frame on the same topic. `tests/shuttle_tests_trigger.cpp` case8/case9 pin
both laws; case10/case11 pin the per-subscriber variant.

The price of the serial chain is head-of-line blocking across subscribers: one subscriber's
callback must fully finish before the next starts. That is an accepted trade-off; the escape hatch
is `publish_concurrent`, which keeps exactly the per-subscriber FIFO and drops only the
cross-subscriber ordering.

The chain tail is a `ChainLink{ev, owner}` held by `shared_ptr`, because a waiter must see the
*live* value — a by-value copy would freeze `owner` at handoff time. `owner` (the task id that
will set the event) is written by the delivery unit itself when it starts and cleared to −1 before
`ev.set()`; the publisher cannot fill it in because it does not know which task will deliver.
`Broker::remove_sub` erases that subscriber's tail slot so unsubscribing never strands chain-tail
events in `topics`.

### Self-waiting is diagnosed at the wait site, not the enqueue site

Publishing to the same topic inside a callback and then `wait()`ing on the new frame deadlocks
under any FIFO design — in `serial` the inner frame queues behind the current one, and in
`concurrent` the same is true whenever the target set includes the publisher's own registration.
The diagnosis relies on `ChainLink::owner`: `detail::check_self_blocked` runs *before* the
suspension in `Publication::wait()` / `wait(timeout)` / SV `shuttle_c_wait_pub`, and ends the run
through `weave::fatal` when any link in `blocked_by` is owned by the current task.

Why the wait site: publishing to one's own topic *without* waiting (relay/forward) is legal, and in
that case the tail really is held by the current callback's delivery task — the same observation
that must not count as a violation at enqueue time. Why a fatal and not an exception: the frame has
no result to hand back, so a `catch` could only turn the diagnosis into a silent hang; the report
(the same `weave::fatal` entry the core uses for contract violations) carries the wait graph and
ends the simulation on both backends (`$fatal` / `SC_REPORT_FATAL`). What stays undiagnosed is a
cycle between two *unrelated* tasks — chain ownership only proves "waiting for oneself", not an
arbitrary wait graph.

### Unsubscribe takes effect immediately; in-flight deliveries retire

A snapshot guarantees *who was selected*, not *that they will be called*. Both retirement sources
are recorded as the `retired` terminal state (part of `dropped`, never `delivered`, no `Failure`):

- C++ side: `Broker::remove_sub` sets `Subscriber::removed`; `deliver` checks the flag before the
  kind dispatch. The flag is necessary because the snapshot holds a `shared_ptr` — without the
  check the removed callback would really run.
- SV side: `shuttle_registry` drops silently via `retired_[id]` (only never-registered ids
  `$error`) and calls `shuttle_c_sub_retired`; `Broker::deliver` converts the attempt via
  `take_retired_delivery()`.

The two sources are disjoint and neither replaces the other: the C++ flag cannot see SV-side
`shuttle_subscription::unsubscribe()` (it deletes the registry entry directly), and the SV report cannot
cover C++ subscribers. Earlier only the SV source produced `retired` while C++ delivered to
removed callbacks — a backend-specific terminal state, which is a category error. The reverse
(keeping the frame on the SV side too) was rejected: it requires cross-language in-flight
reference counting and callbacks running after `unsubscribe()` returned, both worse than dropping
a frame.

### The target set is name ∩ type, fixed at snapshot; type mismatch is not a target

Target selection happens once, at the publish call (`Broker::match_subs(topic, tag)` intersecting
`Subscriber::filter_tag` with the frame's `meta.type_tag`). A type-disjoint subscriber is not a
target: no `target_count`, no delivery unit, no `dropped`, absent from `subscriber_count()`.
`deliver` therefore never re-checks the tag.

Why: `subscribe<T>("uart.*")` subscribes to *type-T events under `uart.*`*; a frame of another
type was never its frame. Mis-typed tags are caught earlier, as `TypeMismatchError`, by
`Broker::check_filter_early` at subscription and `Broker::check_bind_compat` at first bind — the
latter exists because SV subscribers carry a tag without declaring a binding, and without it a
wrong tag would silently exclude the subscriber from every frame depending on registration order.
The last line of defense inside `deliver`: a `raw_ops` frame (unbound topic, pure bytes) hitting a
typed subscriber throws `PublishError` instead of bit-casting `vector<byte>` to `T`.

### Exactly one completion advance point

`Publication::finished()` means "targets that reached a terminal state", and the only place that
advances it is `Broker::target_finished()`:

```cpp
void target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->target_count && !pub->done_flag) pub_complete(pub);
}
```

Every path — successful delivery, retired skip, callback exception, a serial unit's unit-level
failure (unattempted targets are still counted) — goes through it. There is no branch that sets
`finished = target_count` wholesale at unit end; that shape would make `done()` true before
delivery actually finished. `pub_complete()` runs hooks before setting `done_ev`, so a waiter
woken by the event never observes the "SV process not yet woken" intermediate state. A publish
with zero targets completes immediately (`publish_serial` short-circuits too). Everything that
"did not arrive" is countable: policy `skip` lands in `skipped()`, snapshot-time unsubscribe in
`retired()`, type-disjoint frames never enter the target set.

The five terminal states:

| State | Meaning | Counted at |
|---|---|---|
| delivered | callback ran to completion (SV: export returned) | `Topic::delivered`, `Subscriber::received` |
| retired | never entered the callback, nobody at fault: unsubscribed after snapshot, before callback | `Publication::retired()`, `Topic::retired`, `Subscriber::retired` |
| failed | delivery did not complete cleanly: callback threw, frame failed to materialize, cancelled, unit-level failure | `Topic::failed`, `Subscriber::failed`, `failures()` |
| deferred | not yet started at return time (only `now`'s defer segment); later falls into one of the three above | `Publication::deferred()` (a marker) |
| skipped | never entered the callback, nobody at fault: removed from the frame-level target set by policy (only `now` + `skip`) | `Publication::skipped()`, `Topic::skipped`, `Subscriber::skipped` |

Accounting invariants (`dropped = retired + skipped + failed` at both topic and subscriber level):

1. `done()` iff `finished() == subscriber_count()`, all three flavors, no exceptions;
2. `subscriber_count() + skipped()` = this frame's target-subscriber count (name ∩ type, fixed at snapshot);
3. `finished()` counts only delivered + retired + failed; hence `finished() ≤ subscriber_count()`;
4. subscriber-level `received() + dropped()` counts every frame whose name ∩ type matched it — note the frame-level/subscriber-level denominator differs by the `skip` step: `dropped()` counts policy-skipped frames, `subscriber_count()` does not.

### Wildcards belong to subscription patterns only

Publish, `topic_declare` and `topic_set_retained` take concrete names; `>` must stand alone in the
last segment of a pattern, `*` alone in any segment; empty segments and empty strings are rejected
on both sides (`TopicPatternError`). Validation runs **before any state is built**.

Why publish-side rejection matters: routing is string comparison, so allowing
`publish_serial("uart.*")` would erect a phantom topic named `uart.*` in the `topics` table — with
its own `seq`, binding and retained slot — and it would really fan out to `uart.*`, `uart.>` and
`>` subscribers. It looks like broadcasting to a subtree; it is a name coincidence, and `uart0.*`
subscribers would not receive it. Worse, `check_bind_compat` skips wildcard subscribers, so the
misuse would not even surface a wrong tag. Why subscription-side validation matters: `match`
accepts the rest of the pattern once it sees `>`, so `a.>.b` silently degenerates to `a.>`; `a.rx*`
only matches the literal `a.rx*`. Both are "registered successfully, never receives" silences.

Read-only queries (`topic_retained`) do not validate: an illegal name simply reads as "never
bound". In `now_preflight`, name validation precedes the policy decision — the same illegal name
must not change its error with `NowPolicy`.

### `topic_name` and `topic_seq` are two orthogonal axes, and the publisher cannot see its own seq

`Meta::topic_name` answers "which channel" (shared by all frames of a topic, so it does not
identify a frame); `Meta::topic_seq` answers "which frame on that channel" (per-topic, from 1, so
`a.1` and `b.1` coexist). The pair identifies a frame within the broker's lifetime. Neither can go:

- a single global seq makes the gap between adjacent frames on one topic meaningless, while all
  counters are per-topic and `topic_seq` exists precisely to be reconciled with them (a gap means a
  missed frame; on the `serial` chain it must be strictly increasing);
- name alone makes frames indistinguishable — a retained replay could not be told from a live
  frame (a late subscriber first sees the *old* `topic_seq`, then live frames continue: the jump is
  the "replayed history" signal);
- the name itself cannot go: a wildcard registration matches several concrete names, so a callback
  must learn the concrete source; the `topics` table is keyed by it. Frames always carry the
  concrete name; patterns never enter `Meta`.

`topic_seq` crosses the DPI boundary as-is (`longint topic_seq`), so C++ logs and SV `$display`
align on `(topic_name, topic_seq)`. It can never serve as a request-response correlation id,
because the publisher cannot see its own frame's `topic_seq` (see the correlation decision below).
The field names carry their scope prefix precisely because a bare `seq` was misread as global.

### `publish_now` has a reentrancy bound

Because callbacks run on the publisher's stack, a subscriber that `publish_now`s again recurses
rather than suspends; two mutually-publishing subscribers overflow the stack with no diagnosis.
`dispatch_now` carries a file-scope depth counter (`kMaxNowDepth = 8`) turning the cycle into
a `PublishError`. The bound is a diagnostic ceiling, not a policy: legitimate nesting
(request frame → responder handler → reply frame) reaches depth 2–3. Within the synchronous
segment a single callback failure does not abort the frame — it is recorded in `failures()` and
`first_error`, the loop continues, and the first exception is rethrown to the caller only after
all targets finished. On the SV side `shuttle_c_publish_now` converts the exception into a return
code plus `shuttle_c_last_error()`.

### Responses travel on `publish_now`; blocking entries are never reply targets

Replies use `detail::pub_now` with `NowPolicy::skip` (SV `shuttle_publisher::reply` likewise),
never `publish_serial`: with a `nb(handler)` responder and `send_now()`, the entire round trip
runs on the requester's stack — `send_now()` returns with the reply already in the `Call`
(`call.queued() == 1`), zero RTT, no simulation time consumed.

`skip` is the whole response-channel contract, and it is a delivery policy, not a registration
rule: a response topic is an ordinary topic to the broker. A blocking entry on it is legal to
register (a wildcard pattern covering the topic registers too) but is never a reply target —
the frame counts it in `skipped()` and the `skipped` component of `dropped()`, its callback
never runs, and it never touches the topic's serial chain, so a slow blocking observer cannot
delay a requester, a responder, or another observer. A reply is half of a rendezvous; a copy
queued behind a blocking observer's chain is a reply in name only, which is why the policy is
`skip` and not `defer`.

To watch the reply stream, register `nb()`: reply frames are delivered to every matching
nonblocking subscriber, the requester's own `Call` table included, and `Meta::corr_id`
distinguishes them — nonzero means a correlated reply (echoed from the request), `0` means a
hand-published frame. The topic name itself stays public (`resp_topic()`, hardcoded
cross-language strings are legal), and publishing to the topic by hand is legal; hand-published
frames reach blocking entries there per the publisher's own policy.

### The correlation id lives in the frame header, not the payload

`Meta::corr_id` is the request-response correlation id: allocated by the requester, echoed back
by the responder, used to route a reply to its `Call`. It is the only business-writable field of
`Meta` (the rest are broker-filled); `0` means "not a correlated exchange". The public publish
trio does not expose it — `Publisher::publish_*` always sends `corr_id = 0`; only `Requester::send_*`,
`serve` and SV `shuttle_publisher::reply` write it (via the trailing argument of the internal
`detail::pub_*`).

Why not in the payload, in three load-bearing arguments:

- the publisher cannot see its own `topic_seq`, so a seq-derived id could not be written into the
  payload before publishing;
- ordering requires the id to be in the table *before* the publish: `send_now` + nonblocking entry
  means the responder replies on the publisher's stack synchronously — one step late and the reply
  is stale (`stale_responses()` grows);
- `topic_seq` is per-topic bookkeeping (`a.req` and `b.req` both start at 1, and `serve` supports
  wildcard request topics), so exposing it would collide ids and turn a diagnostic field into an
  ABI contract.

The counter is a process-level plain `static` (sound under the single-active-process model), so
two `Requester` instances sharing a response topic do not collide. The reply table holds
`weak_ptr`s; the `Call` holds the only strong reference, so dropping the handle (or `cancel()`)
releases the slot and late replies land in `stale_responses()` — countable, never silently
misdelivered. Without the id, out-of-order replies misassign (two in-flight requests, FIFO order,
swapped answers are undetectable when values coincide — pinned by `shuttle_tests_requester::case2`), a
3-target request cannot answer "how many replies remain", a late reply from a timed-out request
poisons the next one, and SV responders have no integer to echo back.

`wait_all()`'s expected reply count is a convention, not knowledge: the framework defaults to
"each target of the request frame replies exactly once" (`publication().subscriber_count()`);
with non-replying observer subscribers, use `wait_for(n)` / `recv`. Request and response topics
must differ — the responder publishes its reply inside the request callback, and a shared topic
would queue the reply behind the callback that published it; the constructors throw
`std::invalid_argument`.

### The SV requester is the same design, with the counter across the DPI

`shuttle_requester #(TReq, TResp)` / `shuttle_call #(TResp)` mirror `Requester` / `Call` shape for
shape: the call slot is registered before the frame goes out (an nb responder answers inside the
publish call), `wait_for` bounds the whole wait with one deadline, and `wait_all` reads its
expected count from the request frame's `subscriber_count()`. One piece cannot live in SV and
becomes a DPI entry instead — `shuttle_c_take_corr_id` (the [`detail::take_corr_id`] counter is
process-level C++ state; a second counter would let an SV call and a C++ call on one response
topic mint the same id).

The differences are the language, not the design: SV has no exceptions, so a failed constructor
leaves `valid()` false with `last_error()` set, `send_*` returns a null call instead of throwing,
and `recv`/`wait_*` report `ok == 0` for both timeout and cancellation (`TimeoutError` /
`CancelledError` do not exist there). The demux needs a handlable subscription callback but the
queue belongs to a parameterized class, so `shuttle_reply_target` is a non-parameterized base
whose `accept_response` the requester overrides. Requests go out through the publish DPI entries
directly, not through `shuttle_publisher` — the public trio has no `corr_id` parameter, on purpose, and
that is exactly why `shuttle_publisher::reply` bypasses it too. Cancellation is explicit (`cancel()`)
because SV has no `weak_ptr`: a slot the caller merely forgot is invisible, so `prune()` can only
drop cancelled calls, and replies for those are counted in `stale_responses()`.

### Mailbox is a subscriber plus a queue, not a new delivery mechanism

`Mailbox<T>` composes two existing capabilities: a subscriber identity via the ordinary
`detail::sub_typed` / `sub_raw` registration with a **fixed nonblocking entry**, and its own
`std::deque` + `weave::Event`. The broker gains no state and no exported symbols — hence the
header-only implementation. The entry must be nonblocking because the mailbox's callback only
copies into the queue, and it runs inside `publish_now`'s synchronous segment (publisher's stack);
a blocking mailbox callback would deadlock the publisher. The useful corollary: `publish_now`
fills a mailbox synchronously, so a request to a purely nonblocking peer completes with zero RTT.

Bounded mailboxes must pick `drop_oldest` / `drop_newest` / `reject` at construction;
`OverflowPolicy::block` is rejected because blocking the deliverer deadlocks the publisher.
`overflowed()` ("accepted by the queue, then dropped") and `Subscription::dropped()` ("never
delivered to this subscriber") are two different ledgers; `reject` reuses the normal `Failure`
channel rather than inventing a new one. Closing is idempotent (unsubscribe + wake waiters), and
the queue stays readable after `close()`. The SV mailbox (`shuttle_pkg::shuttle_mailbox`) is pure
SV over `shuttle_subscribe_nb` and weave's `weave_sv_wait` / `weave_sv_trigger` — no new DPI
entries; SV has no exceptions, so only `drop_oldest` / `drop_newest` exist there.

The mailbox is a timing primitive, not an RPC primitive: a single-responder exchange without a
correlation id can be built on it (and the demo shows exactly that), but `Requester` is its
*sibling*, not its user — the requester reuses the same subscription primitives directly and does
not include `mailbox.hpp`.

### Cross-language frames pass the payload by reverse pull

C++→SV delivery does not pass the payload as a DPI argument: the C++ side parks the encoded frame
in a process-level `FrameContext` (an RAII save/restore stack, nesting-safe), and the SV export
body pulls it with `shuttle_c_frame_len()` + `shuttle_c_frame_payload(b)` — the array is SV-owned, so one DPI call per frame and one `memcpy` on the C++ side. The trigger was VCS
2018.09's `svdpi.h` lacking `svCreateArray` — C++ cannot construct an open array — plus the
lifetime problem of caller-stack arguments. The safety premise is the single-active-process
contract: while the export executes, no other process can overwrite the parked frame. The full
protocol — import/export tables, byte layout, scope rules — is [backends.md](backends.md)'s
subject; the decision recorded here is only that the bridge depends on the port contract, never
the other way round.

### Delayed triggers name an edge, they never pass an event id

`SHUTTLE_TRIG_EDGE`'s `trig_name` is a weave edge *name*; the C++ side does
`weave::sv::Edge(name)` find-or-insert in weave's own name table, so both sides share one id.
Shuttle holds no event objects and offers no by-id restoration — the earlier
`shuttle_event` + `shuttle_c_event_*` + `SHUTTLE_TRIG_EVENT` shadow event system was deleted and
must not return: an id crossing DPI forces someone to own an "id → event" table, losing early
failure and single ownership. Waits go through `Edge::wait()` / `Pulse::wait()` rather than raw
`weave_sv_wait`, inheriting weave's suspension bookkeeping (cancellation injection, the wait
graph, and "no current task while suspended" — the last one is what makes the self-wait diagnosis
above sound). Edge semantics are edge, not latch: a `fire()` before the wait registers is lost (use
`after(weave::Event&)` for latching). VCS 2018.09 reserves `edge`, so the SV field is `edge_name`.

### Inheritance is delegated level by level, never flattened at the leaves

`SHUTTLE_REFLECT_DERIVED(Derived, Base, own_fields...)` registers `Derived` by encoding the base
subobject through `Codec<Base>` instead of repeating the inherited field names; the SV twin is
`` `shuttle_base(Base) `` inside the derived class's three-piece block. Both produce the same byte
layout as listing every field flat at the leaf — the base encoding first, then the own fields —
because the C++ base subobject is laid out before the derived members and the codec emits fields
in declaration order. The delegation form is the one to write: at depth *n* a flat listing repeats
the inherited names *n* times, and a base field added at level *k* edits every descendant. With
delegation each level names its direct base once, so a chain composes and a base change touches
only the base's own registration. Byte-layout compatibility is what makes the two forms
interchangeable: a leaf registered flat and a leaf registered by delegation decode each other, and
existing frames need no migration.

Constraints follow from the mechanism, not from taste: exactly one public base per level (the
encode side passes `Derived` where `Base` is expected, decode assigns through
`static_cast<Base&>`); `Codec<Base>` must already be registered when the derived registration is
seen; and a pure pass-through level that contributes no field of its own needs a hand-written
`Codec` specialization, because the field macro cannot expand to nothing. On the SV side the
unpack leg of `` `shuttle_base `` copies the derived handle into a base-typed local first: VCS
2018.09 rejects a derived handle for a `ref` formal (`Error-[IRPC]`), and the copy is safe because
the generated entry points have already constructed the object the handle refers to.

### Object references cross the boundary as an internal id

The encoded type space is the encodable payloads: a frame is a value with declared fields, so a
class carrying a queue, a nested handle or an interface cannot be a payload. Widening the format is
not an option — a handle breaks value semantics, and `chandle` cannot enter `{>>{...}}` at all. The
sanctioned indirection is `sv/shuttle_ref.svh`: the object stays in an SV-side table, the frame
carries a 4-byte reference, and its type tag is `$typename(T)`. Nothing in the broker learns about
it — serial, concurrent, now, triggers, retained replays and cross-language routing apply to the
reference frame unchanged, and a C++ peer that only routes it back sees one big-endian u32.
The SV side encodes that u32 through `shuttle_u32` (`pack_obj` / `unpack_into`), the same codec a
reflected `logic [31:0]` field uses, so there is one byte layout, not two.

The user-facing surface deliberately keeps the shapes the value path already has, so the table
never appears in user code:

| Reference path | Value path it mirrors |
|---|---|
| `shuttle_ref_publisher #(T)::publish_now/serial/concurrent(obj, trig)` | `shuttle_publisher #(T)::publish_*` |
| `set_retained(bit)` | the same |
| `shuttle_ref_mailbox #(T)::new(topic[, capacity, overflow])`, `try_recv`/`recv`/`size`/`close` | `shuttle_mailbox` |
| `shuttle_ref_subscription #(T)::blocking/nonblocking(topic, sink)` | `shuttle_subscribe` / `shuttle_subscribe_nb` |
| `shuttle_ref_sink #(T)::invoke_ref(value, meta)` (task) | `shuttle_sink::invoke` (unpacked for you) |
| `shuttle_ref_sink_nb #(T)::invoke_ref_nb(value, meta)` (function) | `shuttle_sink_nb::invoke_nb` |
| `shuttle_ref_msg #(T)::unpack(payload)` inside an untyped sink | `T::unpack(payload)` |
| `typedef shuttle_ref_codec #(T) MyLink;` + `` `shuttle_field(obj, MyLink) `` | a codec for a declared field (nested, vector, enum) |

The entry kind fixes the sink kind, exactly as it does on the value path: `::blocking` takes a
`shuttle_ref_sink #(T)` whose `invoke_ref` is a task and may consume simulation time (it runs inside
the topic's background delivery task, on the serial chain), `::nonblocking` takes a
`shuttle_ref_sink_nb #(T)` whose `invoke_ref_nb` is a function and must not suspend (it is invoked
inline, also by `publish_now`). The method names differ, so one class may implement both and hold
one entry of each kind. Resolving the handle itself never costs time.

The `_ref` spelling is not decoration: a bare `invoke` / `invoke_nb` would collide with
`shuttle_sink` / `shuttle_sink_nb` in any class that consumes value frames and reference frames at
once, and SV has no overloading to separate them by parameter type.

SV has no overloading, so one class cannot implement `shuttle_ref_sink #(A)` and
`shuttle_ref_sink #(B)`: both declare `invoke_ref`, and the compiler rejects the second as already
declared (the same holds for two `shuttle_ref_sink_nb`, which both declare `invoke_ref_nb`). A
receiver that takes several reference types implements the untyped
`shuttle_sink_nb::invoke_nb(payload, meta)` (or the blocking `shuttle_sink::invoke`) once and
resolves each arm with `shuttle_ref_msg #(T)::unpack(payload)` — one line per type, and
`meta.type_tag` picks the arm exactly as it picks a message type on the value path. A type
hierarchy keeps the typed shape instead: implement one `shuttle_ref_sink #(Base)` (or its `_nb`
flavour) and `$cast` down inside `invoke_ref` (the encoded tag is then the base's name, so a
subscriber filtering on a
derived name does not match). A queue-shaped receiver needs no adapter at all: one
`shuttle_ref_mailbox #(T)` per type, drained by one service loop.

There is no reference flavour of `shuttle_requester` / `shuttle_serve` / `shuttle_mailbox`, and
none is needed: request/response carries any reflected message, and a message may carry an object
next to its data. The codec is the whole mechanism — declare the field with `shuttle_ref_codec`
(through a typedef, because `` `shuttle_field `` takes a codec name) and the request, the reply,
the retained frame or the mailbox entry hold a reference instead of a copy:

```systemverilog
typedef shuttle_ref_codec #(FObj) FObj_link;
class AskObj;
  FObj         obj;          // 4 bytes in the encoding
  logic [31:0] mode;
  `shuttle_reflect_begin(AskObj)
    `shuttle_field(obj, FObj_link)
    `shuttle_field(mode, shuttle_u32)
  `shuttle_reflect_end
endclass
shuttle_requester #(AskObj, RspVal) rq = new ("svc.ask", "svc.rsp");  // unchanged
```

The two forms share one table and one encoding, so an object can enter through a reference frame,
leave inside a value frame, and still resolve to the same handle on the far side.

Four constraints follow from the reference being a table index:

- `T` must be the *same* class type on every side that names it (declare it in a package or in a
  file both sides include): the table and the tag are per-`#(T)` specialization, and no runtime
  registration step exists. Because the tag is `$typename(T)`, a package-scope class tags its
  frames `pkg::T` — a C++ mirror reflects that exact spelling with a single `std::uint32_t` field.
- ids are allocated monotonically and never reused, so a stale reference is a loud `$error` at
  delivery and never a silent alias onto a newer object; the mailbox keeps such frames out of the
  queue and counts them in `unresolved()`.
- the table holds a strong reference, so the publisher calls `free(obj)` (or `clear()`) only
  after every in-flight and retained frame carrying that reference has been consumed. Retained
  frames re-resolve at replay time — which is what makes a post-publish mutation visible to a late
  subscriber, and also what turns an early free into a loud failure instead of a dangling read.
- the same object published twice keeps one reference, so a hot publisher does not grow the table
  per frame; growth follows distinct objects, and `free` is the only way it shrinks.

The mechanism is pure SV — no DPI entry, no broker state — because only the owning side resolves;
the other side treats the reference as data. It is not a second messaging path: the frame still
runs through the broker with the broker's counters, ordering and completion semantics, and the
reference only decides what the payload's number means. Packaging a whole object graph this way is
deliberate user-level composition, in the same spirit as `Mailbox` / `Requester`.

### `Meta` strings are views, and `pubs` holds only external publishes

`Meta::topic_name` / `Meta::type_tag` are `std::string_view`s into storage the broker already
owns and never moves (the `topics` key and `Binding::tag`), so building a frame costs no header
allocation and copying `Meta` is two (pointer, length) pairs. They are not NUL-terminated:
DPI export tables need `const char*`, so `src/backend/svdpi/bridge.cpp` materializes
`std::string` only on the SV delivery path; pure C++ usage is unaffected. Users write `meta.topic_name == "dev.tx"` and `std::string s{meta.topic_name}`
— copy-initialization `std::string s = meta.topic_name;` does not compile (the `string_view` →
`string` constructor is explicit). The price is ABI: `Meta` can no longer be aggregate-initialized
from strings by user code, and old binaries must be recompiled against a new `libshuttle.so`.

The `pubs` table registers only *external* (SV) publishes, because an SV handle knows just a
`pub_id` and must resolve it back to `PubState`; C++ `Publication`s hold their state and inserting
every frame would grow `pubs` without bound. It grows monotonically (`pub_id` is never reused);
long-running SV-side high-frequency regressions still need a periodic cleanup policy.

### Handle lifetimes

`Publication` is a copyable `shared_ptr<Impl>`. `Subscription` is a `shared_ptr<Impl>` whose
*last* destruction unsubscribes (`Impl::~Impl` → `Broker::remove_sub`); `unsubscribe()` is
idempotent. The SV `shuttle_subscription` is an id pair plus an `active_` flag, does not unsubscribe on
destruction (SV has no deterministic destruction), and the SV `shuttle_publication` mirrors
`Publication`'s methods over `pub_id`. "Unsubscribe takes effect immediately" means both: later
frames no longer select the subscriber, and already-snapshotted frames skip it as retired.

## Layering of the public surface

Dependencies point downwards; `shuttle.hpp` is the umbrella and defines what is public:

```
shuttle/types.hpp        Capability / SubKind / NowPolicy / OverflowPolicy / errors
shuttle/meta.hpp         Meta{topic_name, topic_seq, type_tag, time_fs, corr_id}
shuttle/codec.hpp        Writer / Reader (big-endian) + SHUTTLE_REFLECT / SHUTTLE_REFLECT_DERIVED
shuttle/trigger.hpp      Trigger + after/after_all/after_any factories
shuttle/publisher.hpp    Publisher<T> (pub_* ABI visible here; pulls detail/abi.hpp)
shuttle/subscription.hpp subscribe<T> / subscribe_bytes / Subscription (pulls detail/abi.hpp)
shuttle/publication.hpp  Publication (handle over shared Impl)
shuttle/mailbox.hpp      Mailbox<T> — header-only, no broker state
shuttle/requester.hpp    Requester / Call / serve — header-only, no broker state
shuttle/broker.hpp       read-only introspection (shuttle::broker::topic_info …)
shuttle.hpp              umbrella
```

The installed `include/shuttle/detail/` carries `abi.hpp` (the stable second layer the templates
need), `wait.hpp` (`detail::deadline_after` / `detail::wait_wake`, shared by the three
event-or-timeout races in mailbox / requester / `Publication::wait`), and the reflection support.
`xmake.lua`'s header glob must stay `include/(shuttle/**.hpp)`: a narrower glob silently drops
`detail/abi.hpp` from installed packages and user code including `publisher.hpp` fails to compile.

## Scope

Shuttle provides typed publish-subscribe delivery scheduling inside a simulation process, in C++,
in SystemVerilog, and across the DPI boundary. Deliberately not provided:

- RPC pairing in the base model (the request-response sugar of the mailbox/requester sections is
  a composition, and request/reply topics are a convention, not broker knowledge);
- cross-machine communication, persistence and replay;
- a UVM adaptation layer;
- backpressure for plain subscriptions (`SubOptions::max_pending` / `OverflowPolicy` are honored
  only by `Mailbox`);
- shuttle's own clock — simulation time is weave's (`weave::now()`), and there is deliberately no
  `shuttle::now_fs()`-style forwarding wrapper (one existed, and was removed).

Known limitations: SV-side `$error` does not propagate back to C++ (a protocol consequence; a
strict error channel would extend `dispatch`'s return value); cross-language payloads are
value-semantics with a declared-field subset — no variants, pointers, references or handles in the
encoding, so an object graph crosses as internal object ids (see above), never as a payload;
`TopicInfo::deferred` (topic-cumulative) and `Publication::deferred()` (per-frame) share a name
across two granularities.

## Design philosophy for large language models

Shuttle assumes an LLM knows pub-sub messaging and asyncio-style scheduling, but not EDA
simulation semantics; the framework's job is to make the simulation-specific part — which
callbacks may consume simulation time, and what "delivery order" can mean when there is exactly
one CPU — explicit and countable. Generalisation to reach: *never* introduce a second
communication mechanism when a composition of entry capability + queue + topic convention
expresses the need; *never* let a terminal state exist that is not visible in the counters; and
*never* add broker state for a header-only component. Every past violation of these three grew a
shadow system (a parallel event table, a backend-specific terminal state, a duplicate clock) that
later had to be deleted.
