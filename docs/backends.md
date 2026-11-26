# Backends and the DPI boundary

This document is the layer-3 surface split covering the two simulation backends
(svdpi, systemc) and the C++ ↔ SystemVerilog DPI boundary of shuttle. It answers
"why is the boundary shaped this way" and "what exactly crosses it" for people
integrating, porting, or reviewing the backend code. It is the authority on the
DPI boundary: the import/export tables, the protocol constants, and the byte
layout below are the definition. Rules a change must satisfy live in [AGENTS.md](../AGENTS.md)
(see "Guarantees" and "Pitfalls in this environment (already hit)"), positioning
and first use live in [README.md](../README.md), and broker internals — delivery
scheduling, terminal-state accounting, entry selection — live in
[architecture.md](architecture.md).

## Backend selection is switching directories, not macros

`xmake.lua` selects the backend by enumerating a directory, and nothing else:

- The `backend` option (`svdpi` by default, `systemc` the only other value)
  picks exactly one directory: `add_files("src/*.cpp")` +
  `add_files("src/broker/*.cpp")` + `add_files("src/backend/<backend>/**.cpp")`.
  Core and backend are enumerated separately so no recursive rule ever compiles
  both backends into one library.
- There are no `SHUTTLE_BACKEND_*` macros and no `#if` around a backend TU:
  the two backend directories never include each other and cannot see each
  other's symbols. Switching backend = switching directory, so every entry of
  the backend contract must have a definition in whichever directory is
  compiled (see "Adding a backend").
- The `weave` dependency is required with
  `configs = { shared = true, backend = get_config("backend") }`, so shuttle and
  weave always build against the same backend. The floor is weave 0.1.0, the first
  release under the current numbering (renumbered from the earlier 1.x line without
  a code change) and the one that carries `weave::fatal`, which the self-wait
  diagnosis reports through; svdpi additionally requires the `svdpi` package,
  systemc the `systemc` package (both shared).
- What each build compiles: `src/*.cpp` (publication, subscription,
  introspection, exception, trigger — backend-agnostic translation units) plus
  `src/broker/*.cpp` (routing, dispatch, model) plus one backend directory.
  Under svdpi that directory is `facade.cpp` + `bridge.cpp` + `dpi.cpp`; under
  systemc it is a single `bridge.cpp`, and `facade.cpp`, `dpi.cpp`, and
  `sv/shuttle_pkg.sv` do not participate in the build at all.

Running the svdpi backend against a real simulator goes through
`tests/run-eda.sh`, the only VCS runner. The two phony xmake targets
`shuttle_examples_svdpi_vcs` and `shuttle_tests_svdpi_vcs` drive it, each passing its own
testbench through `SHUTTLE_SV_TB` and its own pass marker through `SHUTTLE_PASS_MARKER`
(`SHUTTLE_SV_PKG` overrides the package for compatibility probing). The script locates
vcs via `VCS_HOME`/`PATH`, compiles
`weave_pkg.sv` + `shuttle_pkg.sv` + the testbench, runs simv with the payload
library through `-sv_root`/`-sv_lib`, and judges pass by the output marker plus
the absence of `Error-[`/`^Error` lines; without vcs on `PATH` it prints
`[svdpi-test] skipped` and exits 0. simv is always launched through
`xmake run`, because xmake's prepared runtime search path is what lets simv find
`libshuttle.so` → `libweave.so` and their simlog/spdlog dependencies.

## The svdpi backend

The svdpi backend is the only build with an external subscriber channel. It
consists of four pieces with strictly separated concerns:

| File | Concern |
| --- | --- |
| `src/backend/svdpi/dpi.cpp` | The DPI-C surface: `shuttle_c_*` imports (called by the simulator) and the SV export prototypes. Only DPI type translation and error reporting; no broker state. |
| `src/backend/svdpi/facade.cpp` | The external facade: `ext_*` (byte-frame publish, external-subscription registration) and `pub_*` (per-`pub_id` queries/waits). Broker orchestration only; no DPI knowledge. |
| `src/backend/svdpi/bridge.cpp` | The `detail::backend` implementation: `dispatch` / `dispatch_nb` / `note_retired_delivery` / `take_retired_delivery` plus the frame-pull channel. |
| `src/backend/svdpi/svdpi_internal.hpp` | Private header of the directory: `SvScopeGuard`, `FrameContext`, `g_frame`, the boundary codes, and the `ext_*`/`pub_*` declarations. |
| `sv/shuttle_pkg.sv` | The SV-side package: the DPI import/export declarations, the constants, the subscription registry, and the user API (`shuttle_publisher`, `shuttle_requester`, `shuttle_call`, `shuttle_responder`, `shuttle_serve`, `shuttle_subscription`, `shuttle_publication`, `shuttle_trigger`, `shuttle_mailbox`, `shuttle_ref_publisher` / `shuttle_ref_mailbox` / `shuttle_ref_subscription` / `shuttle_ref_sink` / `shuttle_ref_sink_nb` / `shuttle_ref_msg` / `shuttle_ref_codec`). |

Declarations and definitions are split along "who calls whom": SV calls into
`dpi.cpp`, which calls `facade.cpp` as `shuttle::detail::*`; the broker calls
`bridge.cpp` through the five-entry contract in `src/backend/backend.hpp`;
`bridge.cpp` calls back into SV through the `shuttle_sv_*` exports declared in
`sv/shuttle_pkg.sv`.

### Payloads cross as SV→C++ copies and a C++→SV reverse pull

The two directions use deliberately asymmetric mechanisms:

| Direction | Entry | How the payload crosses |
| --- | --- | --- |
| SV → C++ | `shuttle_c_publish_now` / `_serial` / `_concurrent` (import functions) | `byte payload[]` open array; C++ copies it in one `memcpy` through `svGetArrayPtr` into a `std::vector<std::byte>` (`read_payload` in dpi.cpp), falling back to per-element `svGetArrElemPtr1` when the layout is not canonical. |
| C++ → SV | `shuttle_sv_dispatch` (export task) / `shuttle_sv_dispatch_nb` (export function) | Not through arguments. C++ parks the frame's byte stream in the process-level global `g_frame` (via the RAII `FrameContext`) before calling the export; SV pulls it inside the export body with `shuttle_c_frame_len()` (allocate this many bytes) + `shuttle_c_frame_payload(b)` (fill them), so the payload crosses in one DPI call per frame and the C++ side copies it with a single `memcpy`. |

The reverse pull exists because VCS 2018.09's `svdpi.h` has no `svCreateArray`,
so C++ cannot construct an open array to pass by value; passing the payload as
an argument would also tie its lifetime to the caller's stack frame. The array
the pull fills is therefore SV-owned: `shuttle_c_frame_payload` takes the
`byte payload[]` the SV side just allocated and writes it through
`svGetArrayPtr` (a `memcpy`), falling back to `svGetArrElemPtr1` per element
when the simulator reports a non-contiguous layout — both inside one crossing,
never one crossing per word. A plain
global is safe for exactly one reason: the single-active-process contract from
[../AGENTS.md](../AGENTS.md) ("Invariants that must hold", invariant 1) —
while an export body runs, no other simulation process can run C++-side code,
so nothing can overwrite `g_frame` mid-pull. `FrameContext` is a save/restore
stack (hold the previous pointer, restore on destruction), which is what makes
nested dispatches — a subscriber that publishes to another SV subscriber
synchronously — safe.

`shuttle_frame_bytes` returns the bytes through an `output byte b[]` parameter
rather than as a function result because VCS does not accept unpacked arrays as
function return types. `frame_len` / `frame_payload` are svdpi-only entries:
they are declared in `svdpi_internal.hpp`, not in `backend/backend.hpp`, so
only code inside an export body can reach them.

### The DPI surface

Imports — provided by libshuttle.so (dpi.cpp), called from SV
(`shuttle_pkg.sv`). All are `context` imports.

| Symbol | Kind | Signature | Purpose |
| --- | --- | --- | --- |
| `shuttle_c_publish_now` | function | `(string topic, string type_tag, byte payload[], int now_policy, int unsigned corr_id = 0) → int` | Synchronous publish; returns this frame's `pub_id` (>0) or 0 on failure (see `shuttle_c_last_error`). The nonblocking part has finished before it returns. |
| `shuttle_c_publish_serial` | function | `(string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg, string trig_name, int unsigned corr_id = 0) → int` | Background publish on the topic's serial chain; returns `pub_id` or 0. |
| `shuttle_c_publish_concurrent` | function | same as `_serial` | Background publish, one task per subscriber; returns `pub_id` or 0. |
| `shuttle_c_wait_pub` | task | `(int pub_id, longint timeout_fs, output int status)` | Blocks the calling SV process until the publication completes, the deadline passes, or the wait fails; `status` is a `SHUTTLE_WAIT_*` code. `timeout_fs <= 0` means wait forever. |
| `shuttle_c_pub_done` | function | `(int pub_id) → int` | Nonzero when the publication has reached a terminal state. |
| `shuttle_c_pub_stat` | function | `(int pub_id, int kind) → int` | Publication counter query; `kind` 0–4 = target / finished / skipped-by-policy / deferred / retired; unknown `pub_id` returns −1. |
| `shuttle_c_pub_error` | function | `(int pub_id) → string` | Error message of the first failed subscriber (empty = no failure). |
| `shuttle_c_sub_retired` | function | `(int sv_sid)` | Called back by the SV registry when a dispatch finds its target already unsubscribed; lets C++ record that delivery as retired instead of delivered. |
| `shuttle_c_subscribe` | function | `(string topic, int caps, string type_tag, int sv_sid) → int` | Registers an external subscriber; `caps` is 1 (`SHUTTLE_CAP_BLOCKING`) or 2 (`SHUTTLE_CAP_NONBLOCKING`); `sv_sid` is the SV registry id, stored opaquely and returned verbatim at dispatch. Returns 0 on success. |
| `shuttle_c_unsubscribe` | function | `(int sv_sid)` | Removes the external subscriber with this handle. |
| `shuttle_c_set_retained` | function | `(string topic, bit retained)` | Enables/disables the retained latch on a topic. |
| `shuttle_c_take_corr_id` | function | `() → int` | Allocates the next request/response correlation id from the counter `shuttle::Requester` uses; never 0. Kept C++-side so an SV requester and a C++ requester cannot mint the same id for two calls on one response topic. |
| `shuttle_c_last_error` | function | `() → string` | The most recent error message on this process (the error channel for every entry that reports failure as a return code). |
| `shuttle_c_frame_len` | function | `() → int` | Frame-pull protocol: byte length of the frame parked in `g_frame`. |
| `shuttle_c_frame_payload` | function | `(output byte payload[])` | Frame-pull protocol: copies the parked frame into the array the SV side allocated (one `memcpy` when the handle is contiguous, per-element accessors otherwise). |

Exports — provided by `shuttle_pkg.sv`, called back by the C++ broker
(prototypes in bridge.cpp):

| Symbol | Kind | Signature | Purpose |
| --- | --- | --- | --- |
| `shuttle_sv_dispatch` | task | `(int sub_id, string topic_name, string type_tag, longint topic_seq, longint time_fs, int unsigned corr_id)` | Blocking entry of a delivery: runs in a background simulation process and may consume simulation time. Pulls the payload via the frame-pull protocol, then dispatches through `shuttle_registry` to the subscriber's `shuttle_sink::invoke`. |
| `shuttle_sv_dispatch_nb` | function | same arguments | Nonblocking entry: runs in the publisher's process context, must be zero-delay, lands in `shuttle_sink_nb::invoke_nb`. |

The meta fields cross as separate DPI arguments, not as packed bytes, so C++
logs and SV `$display` can align on `(topic_name, topic_seq)` directly.

### Protocol constants and the frame meta

The numeric values are part of the protocol: SV spells them as `localparam`s
and C++ compares against the same numbers (via `code_of(...)` on the C++ enums),
so the two sides cannot drift.

| Family | Constants |
| --- | --- |
| Subscription entries (`caps` of `shuttle_c_subscribe`) | `SHUTTLE_CAP_BLOCKING = 1`, `SHUTTLE_CAP_NONBLOCKING = 2` — a registration is exactly one of them; any other value is rejected. |
| `publish_now` policy (`now_policy`) | `SHUTTLE_NOW_REJECT = 0`, `SHUTTLE_NOW_SKIP = 1`, `SHUTTLE_NOW_DEFER = 2` (default: anything else falls back to defer). |
| Trigger kinds (`trig_kind`) | `SHUTTLE_TRIG_NONE = 0`, `SHUTTLE_TRIG_DELAY = 1` (`trig_arg` = femtoseconds), `SHUTTLE_TRIG_PUB = 2` (`trig_arg` = another `pub_id`), `SHUTTLE_TRIG_EDGE = 3` (`trig_name` = a `weave_edge` name; empty for every other kind). |
| Wait status (`status` of `shuttle_c_wait_pub`) | `SHUTTLE_WAIT_OK = 0`, `SHUTTLE_WAIT_TIMEOUT = 1`, `SHUTTLE_WAIT_FAILED = 2` (subscriber failure or internal error). |
| Mailbox overflow | `SHUTTLE_OVERFLOW_DROP_OLDEST = 0`, `SHUTTLE_OVERFLOW_DROP_NEWEST = 1` (no `block`/`reject` on the SV side). |
| Publication lifecycle (`pub_state`) | 0 = running, 1 = done, 2 = done with failures; −1 = unknown `pub_id` (`kUnknownPub`). |
| Publication counters (`shuttle_c_pub_stat`) | 0 = target, 1 = finished, 2 = skipped-by-policy, 3 = deferred, 4 = retired. |

The frame meta (`shuttle::Meta` / `shuttle_meta`) has five fields in the same
logical content on both sides: `topic_name`, `type_tag`, `topic_seq`,
`corr_id`, `time_fs`. Only `corr_id` is written by the publisher (0 =
uncorrelated); the broker fills the rest, and `time_fs` comes from
`weave::now()`, not from the backend.

### Payload byte layout

It is defined by `include/shuttle/codec.hpp`, mirrored by the SV message-class
convention in the `shuttle_pkg.sv` header:

- Fields are encoded in declaration order — no field names, no padding, no
  alignment; nested structs expand recursively.
- Integers and floats are big-endian (floats as the IEEE-754 bit pattern);
  enums encode as their underlying integer type, so the SV base type must have
  the same width.
- `std::string` and `std::vector<T>` carry a 32-bit big-endian length prefix
  followed by the elements in order; `std::array` has no prefix.
- SV packing must use the streaming operator `{>>{...}}`: declaration order,
  each field most-significant-byte-first — exactly the C++ `Writer` layout.
  `{<<{...}}` reverses the entire bit stream and is never compatible. A string
  field streams as `{>>{32'(s.len()), s}}` to match the length prefix.

Decoding is bounds-checked per integer (`Reader::take` throws `DecodeError`),
and length-prefixed decoders validate the declared length against the bytes
actually remaining before allocating, so a corrupt length cannot trigger a huge
pre-allocation.

### Scope fixing is needed only for our own package-level exports

Measured on VCS 2018.09: after an SV process has called an import that returns
`string` (such as `shuttle_c_last_error`), subsequent calls to package-level
exports from that same process fail with `Error-[DPI-DXFNF]` — the import call
changed the current `svScope`, making the export invisible. `SvScopeGuard`
(`svdpi_internal.hpp`) therefore switches to the scope cached by package name
(`scope_for`, one entry, no locking needed under the single-active-process
contract) before each of our exports and restores the caller's scope on exit.

The consequences:

- `dispatch` and `dispatch_nb` (bridge.cpp) construct a `SvScopeGuard` around
  the export call — these are the entries whose visibility can be polluted.
- The publish imports may indirectly drive weave (creating tasks, suspending),
  yet need no scope switch of their own: weave's DPI backend wraps
  every one of its own imports in its scope guard, so its exports resolve in
  whatever scope the caller happens to be in. Shuttle therefore never calls
  weave's exports directly and never switches scope for `weave_pkg`; the early
  `WeaveScopeGuard` / `SHUTTLE_WEAVE_SV_PACKAGE` approach was deleted precisely
  because importing weave's exports from dpi.cpp would bypass weave's own
  guard and re-couple shuttle to weave's package layout.
- Entries that never call an export need no guard: `shuttle_c_set_retained`,
  `shuttle_c_take_corr_id`, and the read-only queries (`shuttle_c_pub_done` / `_pub_stat` / `_pub_error`,
  and the frame-pull pair, which only runs inside an export body already under
  a guard).
- The package name itself can be overridden with `-DSHUTTLE_SV_PACKAGE=`; only
  shuttle's own package needs it.

### publish_now's synchronous segment can only touch nonblocking entries

DPI forbids an export **task** inside an import **function** call chain
(`Error-[DPI-INCDIC]`). Because the publish imports are functions, the
synchronous segment of `publish_now` can only invoke entries that are
themselves functions — the nonblocking ones (`shuttle_sv_dispatch_nb`). This is
enforced structurally, not by a delivery-time parameter: `dispatch_now`
partitions the target set by each registration's capability before the
synchronous loop, so blocking-entry targets never enter it. Under the `defer`
policy those targets reach `shuttle_sv_dispatch` later, from a background weave
task, where the import call chain no longer exists; under `skip` they are
counted and not delivered; under `reject` the frame is refused before anything
is built. The same constraint is why `shuttle_sv_dispatch_nb` is a function:
the compiler then enforces zero-delay on the SV side.

### The SV registry turns static export names into integer-handle dispatch

DPI export names are static — there can be no "one export function per
subscriber" — so the SV side holds a registry and the C++ side holds only
integers:

- `shuttle_registry` maps an integer id (`sv_sid`, allocated from 1 upward) to
  the callback object, in one of two associative arrays: `sinks_` (objects
  implementing `shuttle_sink`, the blocking entry) and `sinks_nb_` (objects
  implementing `shuttle_sink_nb`, the nonblocking entry). One object may
  implement both interfaces, but only registering twice makes both entries
  live — one registration is one entry, matching the C++ rule.
  `shuttle_serve_sink` is one such object: `shuttle_serve` registers exactly
  one of its entries, decided by which factory — `::blocking` or
  `::nonblocking` — was called.
- `sv_sid` ownership is SV-side: the registry allocates it, passes it to C++
  as the opaque `handle` of `shuttle_c_subscribe`, and C++ returns it verbatim
  at dispatch time. Neither side maintains a mapping. `pub_id` ownership is
  the mirror image: C++ allocates it at publish time and SV stores it in
  `shuttle_publication`.
- Dispatch is two hops: the export (`shuttle_sv_dispatch` / `_nb`) pulls the
  payload and builds the `shuttle_meta`, then `shuttle_registry::dispatch[_nb]`
  routes by id to `invoke` / `invoke_nb`.
- Unsubscribe-during-flight: `shuttle_registry::remove` deletes the sink and
  sets `retired_[id]`. A dispatch that arrives after that is dropped silently
  on the SV side — not an error — but calls `shuttle_c_sub_retired` so C++
  records the delivery as retired (a real terminal state) instead of a phantom
  delivered. An id that was never registered still gets `$error`, preserving
  visibility of genuine defects.
- `$error` on the SV side never propagates back to C++: an import that the SV
  code called successfully returns successfully. Interfaces that need a strict
  error channel must return it explicitly (which is why `shuttle_c_subscribe`
  and the publish imports answer with codes, and why `shuttle_publisher::publish_now`
  stays silent on failure while `publish_serial` / `publish_concurrent` print
  immediately — fire-and-forget async interfaces have no later moment to
  surface an error).

### wait_done suspends a plain SV process through a leaf weave wait

`shuttle_publication::wait_done()` needs to park an ordinary `initial`/`always`
process until a publication completes, and the mechanism is entirely weave's
public C++ API:

1. `shuttle_c_wait_pub` asks `pub_state(pub_id)`; an already-terminal
   publication is answered without suspending.
2. `pub_check_self_blocked(pub_id)` runs before any suspension: a frame whose
   delivery chain is held by the very process waiting for it can never
   complete, so this ends the run through `weave::fatal` — the diagnosis and the
   wait graph reach the simulator log, and the wait never returns (an exception
   would only be catchable and turn the hang into a silent one).
3. Otherwise it registers a `pub_on_complete` hook that sets a private
   `weave::Event`, and suspends the calling SV process on
   `Event::wait()` — a leaf wait that weave allows without a task identity,
   which is exactly what a plain SV process is.
4. A positive `timeout_fs` arms a separate `shuttle-pub-deadline` task that
   sleeps for the deadline and then sets the same event — a timeout is a
   cancellation, so it needs a task of its own — and whichever leg wins, the
   loser is cancelled.
5. The final `pub_state` maps to the `SHUTTLE_WAIT_*` status: still running
   means the deadline won (`TIMEOUT`), `done_with_failures` means `FAILED`
   (the subscriber failure text comes from `shuttle_c_pub_error`, an internal
   error from `shuttle_c_last_error()`).

The completion hook exists because `pub_complete` runs its `on_complete` hooks
before setting the done event, so a woken waiter never observes an
intermediate "completed but SV process not yet signalled" state. The separate
facade entry `pub_wait` (used internally by the `SHUTTLE_TRIG_PUB` trigger, not
exposed as a DPI import) answers through `done_ev` / `wait_pub_timed` inside a
weave task context instead.

### VCS 2018.09 constraints

- `small` and `edge` are reserved words (Verilog-AMS keywords) and cannot be
  identifiers; `shuttle_trigger` names its field `edge_name` accordingly, and
  error locations often point at the adjacent token.
- The `svdpi.h` of this vintage has no `svCreateArray`, which is what forced
  the reverse-pull payload protocol; VCS also rejects unpacked arrays as
  function return types (hence `output byte b[]`) and local functions inside
  packages (hence `shuttle_register` is a package function with an
  internal-marking name).
- In-package exports work on VCS (verified); if a target simulator rejects
  them, the two export entries move into a module — the protocol is unchanged.
- simv's stderr is swallowed inside the docker-shimmed vcs container: debug
  with `$display` or logs written under the repo's `build/`, not by watching
  stderr.
- The container identity-mounts `~/workspace`, so `/tmp` is invisible to the
  simulator; probe and run directories (e.g. `svdpi_work_*`) live inside the
  repo, which `tests/run-eda.sh` already arranges.
- The docker shim resolves shared libraries through the environment xmake
  prepared; running simv by hand with a self-assembled `LD_LIBRARY_PATH`, or
  copying/symlinking `libweave`/`libsimlog`/`libspdlog` next to simv, breaks
  resolution instead of fixing it. `xmake run` is the supported entry.

## The systemc backend is the broker without an external subscriber channel

`src/backend/systemc/bridge.cpp` implements the same five
`detail::backend` entries as a set of contract stubs:

- `available()` returns `false`: the build has no external subscriber channel.
- `dispatch` / `dispatch_nb` throw `std::logic_error` — unreachable in a
  correct configuration, but every entry must have a definition (switching
  backends is switching directories, not interfaces), and a throw fails fast
  where a missing definition would only fail at link time.
- `note_retired_delivery()` is a no-op and `take_retired_delivery()` is
  constantly `false`: no SV registry exists to report in-flight unsubscribes.
- Simulation time never passes through the backend, and shuttle owns no clock:
  `Meta::time_fs` and all timeout arithmetic read `weave::now()` directly
  (under systemc, weave's own port reads `sc_time_stamp()`), so no backend can
  disagree about what time it is.

The broker is unaware of which backend it runs on: pure-C++ usage is identical
under both. The asymmetry that remains is visible only in the build — the
svdpi directory (and `shuttle_pkg.sv`) does not compile under systemc, so
external-subscriber code paths have no existence there at all.

## Handle identity across the boundary is two one-way allocations

Crossing the DPI boundary never exchanges object references — only integers,
each allocated on one side and echoed back by the other:

| Handle | Allocated by | Stored by | Returned via |
| --- | --- | --- | --- |
| `sv_sid` (subscriber) | `shuttle_registry::add` (SV) | broker `Subscriber::handle` (opaque) | `shuttle_sv_dispatch[_nb]` first argument |
| `pub_id` (publication) | the broker at publish time (`PubState::id`) | `shuttle_publication::pub_id_` (SV) | every `shuttle_c_pub_*` / `shuttle_c_wait_pub` call |

The only genuinely per-process state on the C++ side of the boundary —
`g_frame`, the retired-delivery flag, the last-error string — is safe as plain
globals for the same reason the pull protocol is: the single-active-process
contract guarantees at most one simulation process is inside C++ bookkeeping
at any moment, so no locking and no `thread_local` is needed. Chain ownership
and the self-wait diagnosis built on top of it belong to the broker layer and
live in architecture.md, not here.

## Adding a backend means implementing five entries

A new backend is a new directory `src/backend/<name>/` whose `bridge.cpp`
provides the five entries declared in `src/backend/backend.hpp`:

| Entry | Contract |
| --- | --- |
| `available()` | Whether this build has an external (SV) subscriber channel. |
| `dispatch(handle, topic_name, type_tag, topic_seq, time_fs, corr_id, payload)` | Blocking delivery of `serial`/`concurrent`, called in a weave task context; `handle` is the backend-side number the subscriber received at registration, passed through without interpretation; the meta fields are forwarded verbatim so `Meta` is identical in both languages. |
| `dispatch_nb(...)` | Delivery for `publish_now`, called in the publisher's process context; must not block at all. |
| `note_retired_delivery()` / `take_retired_delivery()` | The unsubscribe feedback channel: the backend sets a flag when a dispatch finds its target already unsubscribed; the broker collects it after the call to record the delivery as retired. |

Neither the broker nor the header changes when a backend is added — unless the
new backend cannot provide delivery at all, in which case the contract itself
is the wrong shape. Time is deliberately not part of this contract: simulation
time is owned by weave, and the broker reads `weave::now()` directly.

The same directory split applies at every level: DPI-C surface, external
facade, `detail::backend` implementation, and SV package sources stay inside
the backend's own directory (or `sv/`) and never enter `backend.hpp` or the
public headers.

### svdpi is the only FFI-carrying backend, and there will never be a second one

Only svdpi has a foreign-function interface to maintain: it is the sole
backend with an external caller, so `dpi.cpp` (the DPI surface) and
`facade.cpp` (the external facade and FFI error channel) live in the svdpi
directory and the systemc build does not compile them. The price is an
explicitly accepted, permanent asymmetry: the svdpi directory sees broker
internals (`broker/broker.hpp`) because `facade.cpp` must orchestrate
preflight, envelope construction, and target-set snapshots, while the systemc
backend knows the broker only through the five-entry narrow port above. A
future backend with external subscribers would follow the svdpi arrangement —
its own FFI surface in its own directory — and the five-entry contract would
still be the only face the broker sees.
