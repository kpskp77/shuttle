# AGENTS.md

Constraints for changes in this repository. Rules only: no rationale, no mechanism descriptions, no
file maps — those live in [architecture.md](docs/architecture.md) (design decisions),
[backends.md](docs/backends.md) (backend and DPI protocol), [README](README.md) (positioning),
and in the header comment of the file a rule names.

**Rule = one imperative constraint you can check against a diff.** If a sentence explains *why*, it
belongs in `docs/architecture.md`, not here.

## The premise

Shuttle is a typed publish-subscribe broker inside an EDA simulation environment; all concurrency
orchestration is delegated to weave. **A simulation process is a coroutine, not an OS process**:
exactly one runs at a CPU instant, it suspends only at explicit weave blocking points, and it is
never preempted. Every guarantee below follows from that model — when no rule covers your case,
derive the answer from the model instead of guessing. Never design against preemptive threading:
no locks, no atomics, no threads anywhere in the broker.

## Guarantees (every change)

- Broker state (`topics` / `subs` / `pubs`) stays lock-free plain data; no `std::mutex`,
  `std::atomic`, spinlock or `std::thread` in `src/`.
- Simulation time is weave's: read `weave::now()`. No backend clock entry, no `$realtime`
  conversion, no `shuttle::now_fs()`-style forwarding wrapper.
- One registration = one subscriber = one entry, decided by the registering call (`subscribe(fn)`
  vs `subscribe(nb(fn))`; `shuttle_subscribe` vs `shuttle_subscribe_nb`; bare `serve` handler vs
  `nb(handler)`). No runtime `Capability` parameter, no `$cast`-based auto-registration of both
  entries. Both entries wanted = two registrations.
- `publish_now`'s synchronous segment invokes nonblocking entries only; blocking targets are
  partitioned out before the loop (`dispatch_now`). Delivery never picks an entry by any parameter.
- `defer` reuses `publish_serial`'s serial chain; background order on a topic equals enqueue order.
  Immediate frames hand off the chain tail inside the publish call; delayed frames hand off only
  after `wait_trigger` returns, inside the delivery task.
- Chain-tail handoff goes through `chain_join(topic, key, mine)` in both chains (topic chain and
  per-subscriber `sub_chains`); capture the tail event at enqueue, never read the chain at
  completion. `ChainLink::owner` is written only by the delivery unit (start: own task id; finish:
  −1 before `ev.set()`), never by the publisher. `Broker::remove_sub` erases the id's tail slot.
- Self-wait diagnosis (`detail::check_self_blocked`) runs before the suspension in
  `Publication::wait()` / `wait(timeout)` / SV `shuttle_c_wait_pub` — never at enqueue.
- Unsubscribe-during-flight is terminal state `retired` from both sources: C++
  (`Subscriber::removed`, checked in `deliver` before the kind dispatch) and SV (registry
  `retired_[id]` → `shuttle_c_sub_retired` → `take_retired_delivery()`). It counts into `dropped`,
  never into `delivered`, and produces no `Failure`. No backend-specific terminal states.
- Target set = name ∩ type, fixed at snapshot (`match_subs`). Type-disjoint subscribers are not
  targets: no `target_count`, no delivery unit, no `dropped`. Typed subscribers never receive
  `raw_ops` frames (`deliver` throws `PublishError`).
- `finished` advances only in `Broker::target_finished()`; every terminal path goes through it,
  including unit-level failure backfill of unattempted targets. Zero-target publishes complete
  immediately (`publish_serial` short-circuits).
- Wildcards appear in subscription patterns only. Validate names/patterns in `check_topic_name` /
  `check_pattern` before building any state; `now_preflight` validates before the policy decision.
  Read-only queries do not validate.
- Response topics (`detail::topic_mark_responses`) reject blocking-entry registrations in
  `Broker::check_response_topic` (wildcard judged by match). Replies go through `detail::pub_now`
  with `NowPolicy::defer`; never `publish_serial`. `dispatch_now` keeps the `kMaxNowDepth`
  reentrancy bound.
- `SHUTTLE_TRIG_EDGE` passes an edge name, never an event id; waits go through `Edge::wait()` /
  `Pulse::wait()`, never raw `weave_sv_wait`. No `shuttle_event` / `shuttle_c_event_*` /
  `SHUTTLE_TRIG_EVENT` resurrection.
- `Mailbox` / `Requester` stay header-only with zero broker state; the mailbox entry is
  nonblocking, bounded mailboxes reject `OverflowPolicy::block` at construction, and overflow
  accounting uses `overflowed()` vs `Subscription::dropped()` as the two existing channels. The SV
  mailbox is pure SV (`shuttle_subscribe_nb` + weave events) — no DPI entry for it.
- Object references cross the wire as a 4-byte internal id: the only user-facing surface is
  `shuttle_ref_publisher` / `shuttle_ref_mailbox` / `shuttle_ref_subscription` (+ the
  `shuttle_ref_sink` callback), tagged `$typename(T)`; the id table is internal, ids are monotonic
  and never reused, and `free(obj)` / `clear()` are the only lifetime verbs. No user-facing
  `alloc` / `get` / `free`, no DPI entry, no broker state. A reference inside a value message
  rides the ordinary codec surface (`shuttle_ref_codec #(T)` behind a typedef, used with
  `` `shuttle_field ``): no `shuttle_ref_requester` / `shuttle_ref_serve` / `shuttle_ref_mailbox`
  exists, and none is added.
- Exception classes have out-of-line definitions (a key function) — header-only exceptions break
  cross-DSO `catch` in simv with `undefined symbol`.
- SV packing is `{>>{...}}` (declaration order, MSB-first per field), matching the C++ `Writer`
  big-endian byte order; strings carry a 32-bit length prefix. Never `{<<{...}}`.
- Call `shuttle_pkg` exports only under `SvScopeGuard`; never call weave's exports directly and
  never reintroduce `WeaveScopeGuard` / `SHUTTLE_WEAVE_SV_PACKAGE` (weave ≥ 1.2.0 guards its own
  imports).
- `add_headerfiles` in `xmake.lua` stays `include/(shuttle/**.hpp)` — a narrower glob silently
  drops `detail/abi.hpp` from installed packages.
- Backend selection is the `src/backend/<backend>/` directory enumeration in `xmake.lua` only: no
  `SHUTTLE_BACKEND_*` macros, no cross-backend includes. A new backend implements the 5 entries of
  `src/backend/backend.hpp` and changes nothing else in the broker.

## Build and verify (mandatory after changes)

After **every** change run the narrowest recipe that covers it, and `just test-all` when the broker
core, dispatch, the DPI surface or a public header changed:

```bash
just example-all   # examples on both backends (systemc locally, VCS under the simulator)
just test-all      # full suites, both backends: tests/*.cpp + tests/shuttle_tests*.sv
```

- Broker core, dispatch, public headers, or DPI surface changed → `just test-all`, both backends.
- Example-only changes → `just example-all` suffices.
- Without VCS, `tests/run-eda.sh` printing `[svdpi-test] skipped` and returning 0 counts as a
  compile-only pass.
- Run simv only through `xmake run`; never add `add_rpathdirs` / `LD_LIBRARY_PATH` hacks and never
  copy/symlink `libweave`/`libsimlog`/`libspdlog` into a build directory.

## Definition of done

- The recipes above pass (or the narrower one this change requires).
- Only the files you changed were formatted (see below).
- A changed public header compiles standalone under C++17 with `-Wall -Wextra -Werror`.
- A changed English doc has its `_zh` twin updated in the same commit.
- No debug `printf`, no commented-out code, no temporary files left behind.

## Where new content goes

| Content | Location |
|---|---|
| New public API surface | `include/shuttle/<name>.hpp` + umbrella include from `shuttle.hpp` |
| New broker state or publish scheduling | `src/broker/` + a chapter in `docs/architecture.md` |
| New internal data model | `src/broker/model.hpp` only (backend-independent) |
| New DPI entry | `src/backend/svdpi/dpi.cpp` + `sv/shuttle_pkg.sv` + the tables in `docs/backends.md` |
| New backend | `src/backend/<name>/` implementing the 5 entries of `backend.hpp` + the `add_files` line in `xmake.lua` + a chapter in `docs/backends.md` |
| New SV-only API (no DPI) | `sv/shuttle_<name>.svh` + an `` `include `` line in `sv/shuttle_pkg.sv` + the API list in `docs/backends.md` |
| New example subject | `examples/shuttle_demo_<subject>.cpp` exposing `void shuttle_demo_<subject>()` (called by `examples/shuttle_demo.cpp`) and `examples/shuttle_demo_<subject>.sv` (listed in the SV compile order in `xmake.lua`) + a row in `examples/README.md` |
| New test case | `caseN` in `tests/shuttle_tests_<suite>.cpp` + the case task in `tests/shuttle_tests_<suite>.sv`; a new suite also needs its call in `tests/shuttle_tests.cpp` / `tests/shuttle_tests.sv` and the SV file in the `xmake.lua` list |
| New rule / guarantee | this file (imperative, diff-checkable); its *why* goes to `docs/architecture.md` |
| Positioning, mapping, quick start | `README.md` (+ `README_zh.md`) |

Header layering: `types` → `meta` → `codec` → `trigger` → `publisher`/`subscription` →
`publication` → `mailbox`/`requester` → `broker`, umbrella `shuttle.hpp` last. Dependencies point
downwards only.

## Naming

- Topic syntax follows NATS (`*` one segment, `>` terminal tail); async-flavored names follow
  asyncio via weave (`CancelledError`, `TimeoutError` naming already settled there). Check the
  familiar model first; do not invent.
- Subscriber handles in diagnostics are spelled `cpp sub N` / `sv sid N` (see `Failure::subscriber`).
- C++ conventions otherwise: `snake_case` functions, `PascalCase` types, `detail::` and `g_*`
  unconstrained. Build as C++17 and keep the public API complete there.
- Docs and commits in English; `_zh` twins rewrite, never translate (no calques: stacked 的, long
  attributive chains, 被-passives, 落地/对账/同构/裸抛/形状/钉住/卫兵/撕裂/第一性).

## Tests

- One `caseN` per case in the matching `tests/shuttle_tests_<suite>.cpp`, failing with
  `throw std::logic_error("caseN: ...")`; the suites are pinned by name in `docs/architecture.md`,
  so renaming a case is a doc change too.
- One suite = one subject file per language: `tests/shuttle_tests_<suite>.cpp` exposing
  `void shuttle_tests_<suite>()` and `tests/shuttle_tests_<suite>.sv` as one module with its own case
  tasks and classes; shared fixtures go to `tests/shuttle_tests_fixtures.sv`. Only
  `tests/shuttle_tests.cpp` / `tests/shuttle_tests.sv` carry the registration and the call order
  (weave orders registered tests by name, so an explicit call list is the contract).
- Cross-language suites are compiled out of the systemc build (`SHUTTLE_TESTS_XLANG` is defined only
  by the svdpi target), never skipped at run time.
- Assertion bodies stay backend-agnostic in `tests/shuttle_tests_<suite>.cpp`; anything svdpi-only
  belongs in a file excluded from the systemc target by `remove_files` in `xmake.lua` (as
  `tests/shuttle_tests_xlang.cpp` is) and must not compile into the systemc suite.
- A cross-language behavior needs coverage on both sides (C++ suite entry + SV assertion) before
  the broker code that enables it lands.

## Formatting (before committing)

Every `.cpp` / `.hpp` / `.lua` file you touch must be formatted — run the formatter on the modified
files only, not the whole tree:

```bash
clang-format -i <changed .cpp/.hpp>     # .clang-format
stylua <changed .lua>                   # .stylua.toml
```

## Docs and commits

- Follow the [documentation schema](https://gist.githubusercontent.com/kpskp77/66866026b906531fa9682c851af83a0f/raw/352e036650bbc31d774a7e62f27ad6c5b3c6bff4/documentation-schema.md): four layers, one question per file, one fact one home.
- `README.md`, `docs/*.md` and `examples/README.md` exist as English + `_zh` twins; `AGENTS.md`
  stays single-language. Change one, update the twin in the same commit.
- Commits are Conventional Commits: `type(scope): subject`, one line. Types: `feat`, `fix`,
  `docs`, `chore`, `refactor`, `test`, `build`, `perf`. Scopes: `broker`, `dispatch`, `api`,
  `svdpi`, `systemc`, `sv`, `meta`, `requester`, `mailbox`, `trigger`, `codec`, `xlang`, `test`,
  `docs`, `build`, `release`.
- One topic per commit. Do not commit unless asked.

## Pitfalls in this environment (already hit)

- `/home/penn/.synopsys/bin/vcs` is a docker shim; the container identity-mounts `~/workspace` and
  friends, so `/tmp` is invisible to the simulator — keep probe files inside the repo.
- simv's stderr is swallowed inside the container; debug with `$display` or logs written under
  `build/`.
- VCS 2018.09 treats `small` and `edge` as reserved words (Verilog-AMS keywords); the SV trigger
  field is `edge_name`, and error locations often point at the adjacent token.
- VS Code IntelliSense: `.vscode/c_cpp_properties.json` `compilerPath` must be `/usr/bin/g++`
  (plain `gcc` yields only C system headers). The container-generated `compile_commands.json`
  records `/usr/bin/g++-11`, which does not exist on the host — rewrite `arguments[0]` to the real
  host compiler, then `Developer: Reload Window` or `C/C++: Reset IntelliSense Database`.
