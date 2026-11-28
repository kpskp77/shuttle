# Shuttle user guide

English | [简体中文](user-guide_zh.md)

Concepts, terminology, and the public API of shuttle. Read this after the
[README](../README.md) quick start; the design rationale behind every guarantee
lives in [architecture](architecture.md), and the DPI protocol in
[backends](backends.md).

## The execution model

Everything in shuttle follows from one premise: **a simulation process is a
coroutine, not a thread.** Exactly one process runs at a CPU instant; it
suspends only at explicit weave blocking points and is never preempted.
Consequences you rely on every day:

- A callback runs to its next suspension point without interference. There are
  no locks in the broker and none in your subscriber code.
- "Blocking" means *this process waits for simulation time*, not *this thread
  stalls*. A callback that waits freezes only itself.
- `publish_now`'s synchronous section runs on the publisher's own stack: the
  publisher cannot be re-entered and cannot suspend there.

All concurrency orchestration (tasks, time, events) is delegated to weave.
Shuttle adds no clock and no scheduler of its own — simulation time is always
`weave::now()`.

## Core concepts

### Topic, pattern, type tag

A **topic** is a dotted name (`uart0.tx`). Publishers use concrete names only.
A subscription may use a **pattern** with NATS-style wildcards: `*` matches
exactly one segment, `>` matches any non-empty tail and must be the last
segment (`uart0.*`, `uart.>`). A wildcard in a publish/declare name throws
`TopicPatternError`.

Each topic binds one **type tag** at first declaration; a publisher or
subscriber declaring the same topic with a different tag throws
`TypeMismatchError`. The tag is the string given to `SHUTTLE_REFLECT` on the
C++ side, and the `localparam type_tag` / `type_tag` argument on the SV side —
the two must agree for cross-language delivery.

### Subscriber and entry

A **subscriber** is one registered callback. Every registration declares
exactly one **entry**, fixed at the registering call — never a runtime flag:

| Entry | C++ registration | SV registration | May consume simulation time |
|---|---|---|---|
| blocking | `subscribe<T>(topic, cb)` | `shuttle_subscribe(topic, sink, tag)` | yes |
| nonblocking | `subscribe<T>(topic, nb(cb))` | `shuttle_subscribe_nb(topic, sink_nb, tag)` | no (zero-delay) |

The entry decides *where* the callback may run:

- `publish_serial` / `publish_concurrent` invoke whichever entry the
  registration used.
- `publish_now` invokes **only nonblocking entries** synchronously on the
  publisher's stack; blocking targets are partitioned out beforehand and
  handled per `NowPolicy`.

One registration = one subscriber = one entry. If the same object needs both
behaviors, register twice (two handles, two counters, unsubscribed
separately).

The `nb(fn)` contract ("this callback never blocks") is upheld by the user;
the broker performs no runtime check. If a nonblocking callback suspends
inside `publish_now`, it suspends the publisher itself.

### Publish flavors

`Publisher<T>` (C++) and `shuttle_publisher #(T)` (SV) offer the same trio.
Method names express **scheduling only**; the subscriber's entry decides the
callback kind:

| Flavor | Where callbacks run | Ordering | Handle |
|---|---|---|---|
| `publish_now(msg, policy)` | synchronously on the publisher's stack (nonblocking entries only) | strict FIFO | returned after the sync section |
| `publish_serial(msg, trig)` | background, one serial chain per topic | per-topic FIFO, join order | returned immediately |
| `publish_concurrent(msg, trig)` | background, one task per subscriber | per-(topic, subscriber) FIFO; subscribers run in parallel | returned immediately |

All three take a **snapshot** at the call: the message copy and the subscriber
set are locked in; later subscribe/unsubscribe calls do not change who this
frame targets. Unsubscribe still takes effect immediately — a snapshotted
subscriber that unsubscribes before its callback runs ends the frame in the
`retired` terminal state, not `delivered`.

Ordering across flavors is not promised: a `concurrent` frame and a `serial`
frame of the same topic may overtake each other.

**Self-wait trap.** Publishing to your own topic from inside a callback and
`wait()`ing on the new frame deadlocks by construction (the inner frame queues
behind the frame that waits for it). Shuttle diagnoses this at the wait site
and ends the run through `weave::fatal`. Publishing without waiting (relay) is
legal; use a different topic or a `Mailbox`.

### NowPolicy

What `publish_now` does about subscribers whose entry is blocking (they cannot
be called on the publisher's stack):

- `NowPolicy::defer` (default) — hands them to the topic's background serial
  chain; the returned `Publication` covers their completion.
- `NowPolicy::skip` — drops them from this frame's target set; counted in
  `skipped()` and in the `skipped` component of `dropped()`.
- `NowPolicy::reject` — throws `CapabilityError`; the frame is never published
  (type binding, seq, and the retained snapshot are untouched).

### Publication: every frame is accounted

Each target subscriber ends in exactly one **terminal state**, identical in
both languages:

| State | Meaning |
|---|---|
| `delivered` | callback ran to completion |
| `failed` | callback threw / frame could not be materialized / delivery cancelled / trigger failed |
| `retired` | unsubscribed after the snapshot, before the callback |
| `skipped` | publisher's policy chose not to deliver (`NowPolicy::skip` only) |
| `deferred` | handed to the background serial chain by `publish_now` (`NowPolicy::defer`) |

The `Publication` handle exposes all of them as counters with two invariants:
`finished() ≤ subscriber_count()`, `done() ≡ finished() == subscriber_count()`,
and `subscriber_count() + skipped()` equals the frame's full target set.
One subscriber's failure never stops the others; `result()` waits and rethrows
the first failure. A `wait()` that returns is a statement about counter
values — read `failures()` (C++) or `first_error()` (SV) before trusting it.

### Frame identity and Meta

Every delivery carries a `Meta` (C++) / `shuttle_meta` (SV) with identical
content on both sides:

| Field | Meaning |
|---|---|
| `topic_name` | the concrete topic published to (never a wildcard, even for pattern subscribers) |
| `type_tag` | the message type tag |
| `topic_seq` | per-topic counter from 1, assigned by the broker, continuous across both languages and all publishers — `(topic_name, topic_seq)` is the frame's identity |
| `corr_id` | correlation id, allocated by a `Requester` and echoed by responders; 0 = uncorrelated. Rides in the header; payloads need no extra field |
| `time_fs` | simulation time at publish (femtoseconds), from `weave::now()` |

### Retained topics

`set_retained(true)` makes the broker latch the latest frame per topic and
replay it to new subscribers at registration time (asynchronously for SV
blocking entries, synchronously for everything else). There is no retained
history and no QoS levels.

### Mailbox

A **mailbox** is a subscription with a queue: "wait for the next frame"
instead of "let a callback handle each frame". It registers the nonblocking
entry (the internal callback only copies into the queue), so `publish_now`
fills a mailbox synchronously. A mailbox is an ordinary subscriber: it counts
in `subscriber_count()`, receives retained replays, and honours wildcard
patterns.

Bounded mailboxes (`SubOptions{max_pending, overflow}`) must choose
`drop_oldest`, `drop_newest`, or `reject` — `block` is rejected at
construction because a delivery callback must never block. Overflow accounting
is separate from subscription accounting: `overflowed()` counts frames dropped
from the queue, `Subscription::dropped()` counts frames that never reached the
subscriber.

### Requester / serve

Request/response sugar over the same primitives: one request topic, one
response topic, and the correlation id in the frame header. `Requester`
allocates `corr_id`, subscribes to the response topic, and demultiplexes
replies into per-`Call` queues; `serve` subscribes to the request topic,
calls your handler, and publishes the return value with the id echoed.
Request and response topics must differ (the responder publishes from inside
its request callback — the same topic would deadlock the serial chain).

`wait_all(timeout)` assumes the convention "every target subscriber replies
exactly once" (`n = subscriber_count()`); use `wait_for(n, timeout)` or
`recv(timeout)` when responders may stay silent.

### Trigger

A `Trigger` defers when a background (`serial`/`concurrent`) publish *starts*;
the snapshot is still taken at the call, so the trigger decides *when*, never
*to whom* or *what*. A deferred frame does not hold its topic's chain while
waiting — it joins at trigger time, so a delayed `publish_serial` never blocks
later frames (delivery order for such frames is join order, not publish
order).

## Terminology

| Term | Definition |
|---|---|
| topic | dotted message name; concrete on the publish side, may be a wildcard pattern on the subscribe side |
| pattern | subscription name with wildcards: `*` = one segment, `>` = terminal tail |
| type tag | string binding a topic to one message type; from `SHUTTLE_REFLECT` / SV `type_tag` |
| frame | one published message plus its `Meta`; identified by `(topic_name, topic_seq)` |
| subscriber | one registered callback; one registration = one subscriber |
| entry | the callback's capability class: blocking or nonblocking, fixed at registration |
| target set | subscribers matching a frame by name ∩ type, fixed at snapshot |
| snapshot | message copy + target set, locked at the publish call |
| chain | per-topic (`serial`) or per-subscriber (`concurrent`) FIFO of pending frames |
| terminal state | how one target's delivery ended: delivered / failed / retired / skipped / deferred |
| publication | the handle for one frame's completion and counters |
| retained | per-topic latch of the latest frame, replayed at subscribe time |
| mailbox | a nonblocking subscriber with its own bounded/unbounded queue |
| corr_id | correlation id in the frame header; request/response demultiplexing key |
| backend | `svdpi` (cross-language, under a simulator) or `systemc` (pure C++) |

## Public API — C++

All headers live under `include/shuttle/`; include the umbrella
`<shuttle/shuttle.hpp>`. Requires a weave task context for any blocking call
(`wait`, `recv`, trigger waits).

### Message declaration

```cpp
struct UartTx { std::uint32_t addr, data; };
SHUTTLE_REFLECT(UartTx, addr, data)          // type tag "UartTx" + codec
SHUTTLE_REFLECT_DERIVED(Pkt, Frame, data)    // derived: base encoded via Codec<Base>
```

The codec subset: integers, floats, `bool`, `std::string`, enums (underlying
width), `std::vector<T>`, `std::array<T,N>`, nested registered structs — up to
64 fields, encoded in declaration order, big-endian. Types without
`SHUTTLE_REFLECT` still work on pure-C++ topics (zero-copy); cross-language
delivery requires the registration.

### Publishing

```cpp
shuttle::Publisher<UartTx> pub("uart0.tx");        // binds topic ↔ type at construction

auto h0 = pub.publish_now(msg);                     // NowPolicy::defer by default
auto h1 = pub.publish_now(msg, shuttle::NowPolicy::skip);
auto h2 = pub.publish_serial(msg);                  // Trigger {} = immediate
auto h3 = pub.publish_serial(msg, shuttle::after(ev));
auto h4 = pub.publish_concurrent(msg, shuttle::after(10_us));

pub.set_retained(true);
pub.topic();                                        // the topic name
```

The message is taken by value: a prvalue argument (`publish_serial(Msg{...})`)
is elided into the frame, not copied.

### Subscribing

```cpp
auto s1 = shuttle::subscribe<UartTx>("uart0.tx",
    [](const UartTx& tx, const shuttle::Meta& m) { /* blocking entry */ });

auto s2 = shuttle::subscribe<UartTx>("uart0.*",
    shuttle::nb([](const UartTx& tx, const shuttle::Meta& m) { /* nonblocking */ }));

auto s3 = shuttle::subscribe_bytes("uart0.>",       // raw bytes, no codec
    [](const std::vector<std::byte>& b, const shuttle::Meta& m) { });
```

`Subscription` (shared semantics; the last handle's destructor unsubscribes):
`unsubscribe()` (idempotent, counters stay readable), `valid()`, `topic()`
(wildcards verbatim), `capability()`, `received()`, `dropped()`.

### Waiting on a publication

```cpp
h2.wait();                       // all targets finished (requires task context)
if (!h2.wait(1_ms)) { /* timeout */ }
h2.result();                     // wait + rethrow the first failure

h2.subscriber_count();           // targets at snapshot (excludes skipped)
h2.finished();                   // terminal states reached
h2.done();                       // finished() == subscriber_count()
h2.skipped(); h2.deferred(); h2.retired();
for (const auto& f : h2.failures()) { /* f.subscriber, f.error */ }
```

### Triggers

```cpp
shuttle::after(weave_event);        // sticky weave::Event
shuttle::after(pulse);              // weave::Pulse (edge semantics)
shuttle::after(edge);               // weave::sv::Edge, cross-language by name
shuttle::after(10_us);              // simulation delay
shuttle::after(task);               // a weave task reaches a terminal state
shuttle::after_all({t1, t2});
shuttle::after_any({t1, t2});       // must be constructed in a task context
```

### Mailbox

```cpp
shuttle::Mailbox<Resp> mb("dev.resp", {16, shuttle::OverflowPolicy::drop_oldest});
auto item = mb.recv();              // blocks; throws MailboxClosed when drained
auto maybe = mb.recv(1_ms);         // std::optional; nullopt = timed out
auto now   = mb.try_recv();         // never blocks, never throws
mb.size(); mb.closed(); mb.received(); mb.overflowed(); mb.subscription();
mb.close();                         // idempotent; queued items stay readable
shuttle::mailbox_bytes("raw.>");    // byte mailbox
```

### Request / response

```cpp
// responder (any number, either language)
shuttle::serve<Req, Resp>("dev.req", "dev.resp",
    [](const Req& r, const shuttle::Meta&) { return Resp{...}; });   // blocking entry
shuttle::serve<Req, Resp>("dev.req", "dev.resp", shuttle::nb(handler)); // nonblocking

// requester
shuttle::Requester<Req, Resp> client("dev.req", "dev.resp");
auto call = client.send_serial(Req{21});        // send_now / send_concurrent too
auto rsps = call.wait_all(1_ms);                // n = request's subscriber_count()
auto rsp  = call.recv(1_ms);                    // one reply, optional
call.wait_for(2, 1_ms); call.cancel();
call.corr_id(); call.publication(); call.queued(); call.cancelled();
client.inflight(); client.stale_responses();
```

### Introspection

```cpp
shuttle::broker::topics();                       // topics with ≥1 publication
auto info = shuttle::broker::topic_info("uart0.tx");
// info->published / delivered / dropped / retired / skipped / failed /
//      deferred / subscriber_count / type_tag / retained
```

### Exceptions

All derive from `shuttle::ShuttleError` (itself `std::runtime_error`):
`PublishError` (type-binding conflict, encode failure), `CapabilityError`
(`NowPolicy::reject`), `TypeMismatchError`, `TopicPatternError` (wildcard
misuse), `DecodeError`, `MailboxClosed`, `MailboxOverflowError`.

## Public API — SystemVerilog

Compile `sv/shuttle_pkg.sv` (after `weave_pkg.sv` and `simlog_pkg.sv`) and
`import shuttle_pkg::*;`. Constants mirror the C++ enums
(`SHUTTLE_NOW_DEFER`, `SHUTTLE_CAP_BLOCKING`, `SHUTTLE_TRIG_DELAY`, …).

### Message declaration

```systemverilog
class UartTx;
  logic [31:0] addr;
  logic [31:0] data;
  `shuttle_reflect_begin(UartTx)
    `shuttle_field(addr, shuttle_u32)
    `shuttle_field(data, shuttle_u32)
  `shuttle_reflect_end
endclass
```

The reflect macros generate `type_tag`, `pack`, `unpack`, and the codec
surface, byte-for-byte equal to the C++ layout. Codec helper classes:
`shuttle_u8/u16/u32/u64`, `shuttle_i8/i16/i32/i64`, `shuttle_f32/f64`,
`shuttle_bool`, `shuttle_string`; inheritance via `` `shuttle_base(Base) ``.
A hand-written codec follows the same convention (`{>>{...}}` streaming,
strings with a 32-bit length prefix).

### Publishing

```systemverilog
shuttle_publisher #(UartTx) pub = new("uart0.tx");

shuttle_publication h0 = pub.publish_now(tx);                    // SHUTTLE_NOW_DEFER
shuttle_publication h1 = pub.publish_now(tx, SHUTTLE_NOW_SKIP);
shuttle_publication h2 = pub.publish_serial(tx);                 // trig = null: immediate
shuttle_publication h3 = pub.publish_serial(tx, shuttle_trigger::after_delay(10_000));
shuttle_publication h4 = pub.publish_concurrent(tx, shuttle_trigger::after_edge(clk_edge));

pub.set_retained(1);
pub.reply(resp, req_meta);        // responder shorthand: echoes req_meta.corr_id
```

`publish_*` return `null` on failure (reason via `shuttle_c_last_error()`;
`serial`/`concurrent` also log an error line).

### Subscribing

```systemverilog
class Monitor implements shuttle_sink;                 // blocking entry
  virtual task invoke(input byte payload[], input shuttle_meta meta);
    UartTx tx = UartTx::unpack(payload);
  endtask
endclass

class FastMon implements shuttle_sink_nb;              // nonblocking entry
  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
  endfunction
endclass

shuttle_subscription sub  = shuttle_subscribe("uart0.tx", mon, UartTx::type_tag);
shuttle_subscription subn = shuttle_subscribe_nb("uart0.*", fast, UartTx::type_tag);
sub.unsubscribe();                // idempotent
```

### Waiting, triggers

```systemverilog
h2.wait_done();                   // blocks this process; timeout_fs = 0: forever
h2.wait_done(1_000_000);          // femtoseconds
h2.done(); h2.subscriber_count(); h2.finished();
h2.skipped(); h2.deferred(); h2.retired();
h2.first_error();                 // "" = no failure (SV has no exceptions)

shuttle_trigger::after_delay(fs);        // simulation delay
shuttle_trigger::after_pub(h);           // another publication completes
shuttle_trigger::after_edge(new("clk")); // weave edge, by name
```

### Mailbox

```systemverilog
shuttle_mailbox mb = new("dev.resp", Resp::type_tag, 16,
                         SHUTTLE_OVERFLOW_DROP_OLDEST);
bit ok; byte payload[]; shuttle_meta meta;
mb.recv(ok, payload, meta);              // blocks; timeout via third argument
if (mb.try_recv(payload, meta)) ...;     // nonblocking
mb.size(); mb.received(); mb.overflowed(); mb.closed(); mb.close();
```

### Request / response

```systemverilog
class Doubler implements shuttle_responder #(Req, Resp);
  virtual function Resp respond(input Req req, input shuttle_meta meta);
    return ...;
  endfunction
endclass

shuttle_serve #(Req, Resp) svc = shuttle_serve #(Req, Resp)::blocking(
    "dev.req", "dev.resp", doubler);     // ::nonblocking(...) for the other entry

shuttle_requester #(Req, Resp) client = new("dev.req", "dev.resp");
shuttle_call #(Resp) call = client.send_serial(req);   // send_now / send_concurrent
Resp out[$];
call.wait_all(out, 1ms);                 // also wait_for(n, out, timeout), recv(...)
call.corr_id(); call.publication(); call.cancel();
client.inflight(); client.stale_responses(); client.last_error();
```

### Object references

For SV classes that have no value semantics, `shuttle_ref_*` passes an object
across the boundary as a 4-byte internal id (tagged `$typename(T)`): a value
message carries the reference through `typedef shuttle_ref_codec #(T) alias;`
plus `` `shuttle_field(f, alias) ``, and the standalone facades are
`shuttle_ref_publisher #(T)`, `shuttle_ref_subscription #(T)`
(`::blocking` / `::nonblocking` factories over `shuttle_ref_sink` /
`shuttle_ref_sink_nb`), and `shuttle_ref_mailbox #(T)`. Lifetime verbs are
`free(obj)` and `clear()` only; ids are monotonic and never reused.

## Choosing the right primitive

| You want | Use |
|---|---|
| deliver now, on my stack, never suspend | `publish_now` + nonblocking subscribers |
| ordered delivery to mixed subscribers | `publish_serial` |
| parallel delivery, slow subscriber must not delay others | `publish_concurrent` |
| deliver later, when a condition holds | any background flavor + `Trigger` |
| "wait until N frames arrived" in my process | `Mailbox` |
| ask-and-answer with timeout | `Requester` + `serve` |
| latest value to late joiners | retained topic |
| watch a topic without knowing its type | `subscribe_bytes` / `shuttle_mailbox` |
