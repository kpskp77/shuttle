# Examples

English | [简体中文](README_zh.md)

Each example is a runnable `WEAVE_TEST` that also runs as an assertion: any wrong value, missing
frame, or timeout throws and fails the run. The expected output below is the oracle for `just
example-all` — compare against it when a run "passes" but looks wrong.

## Run

```bash
just example-sc    # systemc backend locally; the last line is "WEAVE TEST PASSED"
just example-vcs   # svdpi backend under VCS; judged by "SHUTTLE CPP DEMO PASSED"
```

On the svdpi backend, `shuttle_demo.sv` — the ordered top over the `shuttle_demo_*.sv` subject
modules — runs alongside the C++ demos in one simulator, on one broker: the `uart0.tx` that the SV
monitor subscribes to also receives the C++ publish, and the C++ subscriber receives the SV publish.
The systemc backend runs only the C++ demos, entered through `tests/shuttle_tests_main.cpp`.

## What each example exercises

| File | Language | API surface exercised |
|---|---|---|
| `shuttle_demo_publish.cpp` | C++ | publisher + typed subscribe (blocking and `nb`), all three publish flavors, retained topics, wildcard patterns, `subscribe_bytes`, introspection, multi-publisher |
| `shuttle_demo_nested.cpp` | C++ | nested message types (a reflected struct holding two of another reflected struct) |
| `shuttle_demo_inherit.cpp` | C++ | inherited message types (`SHUTTLE_REFLECT_DERIVED`, each level delegating its base subobject) |
| `shuttle_demo_namespace.cpp` | C++ | a message type declared inside a namespace, reflected through a global unqualified alias so its `type_tag` matches the SV class |
| `shuttle_demo_ref.cpp` | C++ | the wire form of a reference (`struct RefId { uint32_t object; }`) and nothing else: a forwarding peer that announces itself retained, forwards a frame untouched and waits (bounded) for the SV half to report the round trip |
| `shuttle_demo_mailbox.cpp` | C++ | mailbox as subscriber, `recv(timeout)` drain, bounded queues (`drop_oldest`/`drop_newest`/`reject`), `OverflowPolicy::block` rejected at construction, copies sharing one queue, `close()`, request/response without a correlation id |
| `shuttle_demo_requester.cpp` | C++ | `Requester`/`serve` with `corr_id` demultiplexing, concurrent in-flight calls, observer vs responder (`wait_for(n)`), responder timeout, a throwing responder surfacing as a `Failure` |
| `shuttle_demo_trigger.cpp` | C++ | `Trigger`: `after(Event)` / `after(duration)` / `after(Task)` / `after_all`, handle completion orthogonal to trigger firing |
| `shuttle_demo_edge.cpp` | C++ | failure and timeout edges: `publish_now` against a blocking entry (`CapabilityError`), the `wait(timeout)` verdict |
| `shuttle_demo_msgs.hpp` | C++ | the wire types shared by the subject files (the mirror of `shuttle_demo_msgs.sv`) |
| `shuttle_demo.cpp` | C++ (top) | the ordered call of the subject files and the only `WEAVE_TEST` of the C++ half |
| `shuttle_demo.sv` | SV | `shuttle_publisher`/`shuttle_subscription`/`shuttle_subscribe_nb`, both entries on one class, `publish_now`/`publish_serial`/`publish_concurrent`, mailbox (pure SV), multi-publisher, nested message types, inherited message classes (`` `shuttle_base ``), a namespaced message class, object references (`shuttle_ref_publisher`/`shuttle_ref_mailbox`/`shuttle_ref_subscription` over one object, echoed through the C++ interposer, resolved back to the same handle), a request/response pair carrying a reference through `shuttle_ref_codec` (`shuttle_requester`/`shuttle_serve` unchanged), a pure-SV requester with `shuttle_requester`/`shuttle_call` against a nonblocking responder (`shuttle_publisher::reply`), unsubscribe; also launches the C++ demos via `weave_run` |

The C++ side mirrors that shape: `shuttle_demo_msgs.hpp` (the wire types), one subject file per
demonstration (`shuttle_demo_publish.cpp`, `_nested.cpp`, `_inherit.cpp`, `_namespace.cpp`,
`_ref.cpp`, `_mailbox.cpp`, `_requester.cpp`, `_trigger.cpp`, `_edge.cpp`), and `shuttle_demo.cpp`
as the only `WEAVE_TEST`, calling the subject functions in order.

The SV side is one file per subject: `shuttle_demo_msgs.sv` (the wire classes), then
`shuttle_demo_publish.sv`, `shuttle_demo_nested.sv`, `shuttle_demo_inherit.sv`,
`shuttle_demo_namespace.sv`, `shuttle_demo_ref.sv`, `shuttle_demo_mailbox.sv`,
`shuttle_demo_requester.sv`. Each module holds the classes it demonstrates and runs standalone;
`shuttle_demo.sv` holds only the instantiation, the call order and the C++ launch.

## Expected output (svdpi backend)

The interleaving of SV and C++ callbacks is scheduled, not fixed — the `[monitor]` lines in each
block may appear in a different relative order, and the exact counts below depend on which demos
run. What must hold:

### Uart topics (`shuttle_demo_publish.cpp` + `shuttle_demo_publish.sv`)

```
[monitor] seq=2 addr=0x00000011 data=0x0000005a   ← SV subscriber receives the SV serial publish
[monitor] seq=4 addr=0x00000011 data=0x0000005a   ← SV subscriber receives the C++ publish (same broker)
[async]   seq=4 addr=0x00000011 data=0x0000005a   ← C++ nb subscriber receives the C++ publish
[raw] topic=uart0.tx payload=8 bytes
[broker] topic=uart0.tx type=UartTx subs=6 published=8 delivered=20
```

`seq` is the per-topic `topic_seq`, monotone across both languages: a gap means a missed frame.
`subs=6` counts every registration on `uart0.tx` alive at the snapshot, including both SV entries
of one class (two registrations, as in C++); `delivered` follows from those registrations and the
frames each of them was alive for.

### `shuttle_demo_mailbox.cpp`

```
[mailbox] publish_now filled it synchronously: size=3 received=3
[mailbox] drained 3 frame(s) (sum=6), then recv(1ms) returned empty
[mailbox] recv() got data=7 seq=4
[capped] drop_oldest: capacity=2 received=5 overflowed=3 survivors=4,5
[capped] drop_newest: capacity=2 received=2 overflowed=3 survivors=1,2
[reject] subscriber cpp sub N -> shuttle: mailbox overflowed (capacity 1)
[block] rejected at construction: ...
[alias] a copy reads the same queue (data=9, 0 left in the original)
[close] drains to 11, then recv() throws: ...
[rpc] request data=21 -> reply ch=3 data=42
```

`received` counts frames accepted into the queue (so `drop_oldest` can show `received=5` with only
2 survivors); `overflowed` counts evictions/refusals. `reject` reuses the normal `Failure` channel
— no separate accounting — and `N` in its handle is the registration-order subscriber id.

### `shuttle_demo_requester.cpp`

```
[driver] addr=0x04 corr_id=1 -> 2 resp(s), 2 target(s)
[driver]   status=0 value=0x01004
[driver]   status=1 value=0x00000
[driver] demux ok: A=0x01006 B=0x01002
[driver] observer: 2 resp(s), 3 target(s), observed=1
[driver] no responder: 0 resp(s), subscribers=0
[driver] responder cpp sub ... failed: device is not responding
```

One request fans out to both register blocks; each answers with its own verdict because `corr_id`
stays attached through the resp topic. The observer case shows why `wait_all` is wrong when
non-responders also subscribe: `wait_for(<responder count>)`.

### `shuttle_demo_trigger.cpp`

```
[alarm] code=1
[alarm] code=3
[alarm] code=2
[alarm] code=4
```

Order is not fixed — `code=2` starts only after the event fires and `code=3` after the worker task
finishes — but exactly four callbacks must run and all four `result()` calls must return.

## Working agreement

Every example file opens with a comment block explaining its scenario; the code then asserts the
documented values, so the comment cannot drift silently. When you add an example, follow the same
shape (comment → scenario → assertions) and add a row to the table above; a new subject needs a C++
file exposing `void shuttle_demo_<subject>()` (called by `shuttle_demo.cpp`), an SV module, its line
in the SV compile list of `xmake.lua` (`shuttle_examples_svdpi_vcs`) and a row in this file.
