#pragma once

// Trace instrumentation: one cached simlog logger per module, every name under
// the "shuttle." prefix so SIMLOG_LEVEL addresses the whole tree at once (and a
// single module can still be narrowed):
//
//   SIMLOG_LEVEL="shuttle*=debug"          every module at debug
//   SIMLOG_LEVEL="shuttle.dispatch=trace"  only publish scheduling, with chain detail
//   SIMLOG_LEVEL="shuttle.sub=debug"       subscription lifecycle, both languages
//
// One module, one logger, in both languages: the SV package opens the same names
// through sv/shuttle_trace.svh (simlog keeps one native logger per name, so an SV
// line and a C++ line about the same object land in one stream, in simulation
// order). The modules are:
//
//   broker     type binding, single-target delivery, retained replay
//   sub        subscription lifecycle: registration, unsubscription, retired deliveries
//   dispatch   publish scheduling: units, chain-tail handoffs, completion, failures
//   pub        the publish entry points and Publication::wait
//   trigger    after_any races, and (SV) a trigger built from a dead edge
//   svdpi      the external facade (ext_* / pub_*), i.e. the SV side of the same operations
//   mailbox    queue overflow, receive timeouts, close
//   requester  calls going out, reply demultiplexing, stale replies, cancelled calls
//
// Level convention (the same one weave uses): debug = lifecycle and
// failure-diagnosis events you want when a run hangs or fails (subscriber
// register/remove, topic type binding, publish-unit creation, publication
// completion, recorded failures); trace = the hot bookkeeping paths (per-frame
// delivery, chain handoffs, retained replay, wait outcomes); error = a failure
// the caller would otherwise swallow (the SV half has no exceptions, so the
// sites that used to print `$error` log here; tests/run-eda.sh treats an
// `[error]` line as a failed run). The default level
// is info, so an enabled build stays silent unless SIMLOG_LEVEL says otherwise.
//
// The loggers are function-local statics of inline functions: one instance per
// module no matter how many TUs include this header (spdlog's registry dedupes
// by name anyway). spdlog checks the level before formatting, so a disabled
// line costs one level check — keep the arguments cheap (ids, views, counts),
// never build a string for a line that may not print.
//
// Layering: simlog is not a backend API — it carries its own per-backend time
// source exactly like weave carries its suspension, so core code depending on
// it does not touch the port contract (src/backend/backend.hpp stays the only
// backend door). This header is installed because Mailbox and Requester live
// entirely in installed headers; the core's modules log through it too, so the
// name tree has one definition rather than one per half.

#include <simlog/simlog.h>

#include <memory>

namespace shuttle::detail {

// Type binding, single-target delivery, retained replay.
inline std::shared_ptr<spdlog::logger> trace_broker() {
  static const auto log = simlog::get("shuttle.broker");
  return log;
}

// Subscription lifecycle, from both halves: `cpp sub N` and `sv sid N` register
// and unregister here, and so do deliveries that arrive after the subscriber is
// gone (the SV registry's terminal state is its `retired` set, C++'s is
// `Subscriber::removed`).
inline std::shared_ptr<spdlog::logger> trace_sub() {
  static const auto log = simlog::get("shuttle.sub");
  return log;
}

inline std::shared_ptr<spdlog::logger> trace_dispatch() {
  static const auto log = simlog::get("shuttle.dispatch");
  return log;
}

inline std::shared_ptr<spdlog::logger> trace_pub() {
  static const auto log = simlog::get("shuttle.pub");
  return log;
}

inline std::shared_ptr<spdlog::logger> trace_trigger() {
  static const auto log = simlog::get("shuttle.trigger");
  return log;
}

// The svdpi backend's external facade (ext_* / pub_*): the SV side of the same
// operations, so a cross-language run can be followed end to end.
inline std::shared_ptr<spdlog::logger> trace_svdpi() {
  static const auto log = simlog::get("shuttle.svdpi");
  return log;
}

// Mailbox queue-level events: capacity overflow, close, receive timeouts.
inline std::shared_ptr<spdlog::logger> trace_mailbox() {
  static const auto log = simlog::get("shuttle.mailbox");
  return log;
}

// Request/response bookkeeping: calls going out, replies demultiplexed by
// correlation id, stale replies, cancelled slots.
inline std::shared_ptr<spdlog::logger> trace_requester() {
  static const auto log = simlog::get("shuttle.requester");
  return log;
}

}  // namespace shuttle::detail
