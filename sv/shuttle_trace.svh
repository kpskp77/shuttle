`ifndef SHUTTLE_TRACE_SVH
`define SHUTTLE_TRACE_SVH

// ── Logging: the SV half writes through simlog, like the C++ half ──────
// Same library, same pattern ([time][logger][level] text), same module names
// and same SIMLOG_LEVEL rules as the broker (include/shuttle/detail/trace.hpp —
// the single definition of the name tree). One run has one tree, so one level
// spelling covers both languages and a module rule narrows both halves:
//
//   SIMLOG_LEVEL="shuttle*=debug"          everything, both languages
//   SIMLOG_LEVEL="shuttle.mailbox=trace"   the mailbox detail of both halves
//
// Sharing the name is not a collision: simlog keeps one native logger per name
// in its registry, so `new ("shuttle.pub")` here and simlog::get("shuttle.pub")
// on the C++ side hand back the same logger — an SV publisher's line and the
// C++ publish entry point's line land in one stream, in simulation order.
//
// Compile order: simlog's sv/simlog_pkg.sv must be compiled before this package
// (tests/run-eda.sh lists it first; a hand-written vcs command line has to do
// the same, see docs/backends.md). simlog_pkg carries no `timescale — like
// weave_pkg it relies on the caller's default — so the command line also needs
// `-timescale=1ps/1ps`; without it vcs rejects every later testbench that does
// carry one (ITSFM: "has `timescale but previous module(s)/package(s) do not").
// On the SVDPI backend simlog prints through vpi_printf, so `simv -l run.log`
// contains the lines with everything else.
//
// One accessor per module, opened on first use: a package-scope initializer
// would call into the DPI library during static initialization, before the
// simulator has finished loading it.
//
// Levels, same words as the C++ half: `debug` for lifecycle and failure
// diagnosis, `trace` for the hot bookkeeping paths, `error` for a failure the
// caller would otherwise swallow — a timed-out wait, a rejected fire-and-forget
// publish, a registration the broker refused. `error` sits above the default
// `info`, so such a line prints unasked, and tests/run-eda.sh treats a `[error]`
// line as a failed run, so a defect is as visible as it was with `$error`.
// `$fatal` stays where the run cannot continue (a corrupt frame length aborts
// the decode).
//
// SystemVerilog has neither variadic functions nor variadic macros, so a line's
// text is formatted at the call site with $sformatf — which happens even when
// SIMLOG_LEVEL filters the line out. There is no cheap disabled line here, so
// the sites stay on the interesting paths and the formats stay short.
typedef simlog_pkg::simlog_logger shuttle_sv_logger;

// Publish/lifecycle: a frame accepted by the broker, a rejected publish, a
// publication that finished.
shuttle_sv_logger lg_pub = null;
// Subscription registry: deliveries that arrive after unsubscribe (the SV
// terminal state is this registry's `retired` set, C++'s is
// `Subscriber::removed`). Registration and unsubscription are not logged here:
// the broker's `register_sub` / `remove_sub` lines report both halves —
// "subscriber registered: sv sid 3 pattern=... kind=external cap=blocking
// filter=..." — and one event keeps one voice.
shuttle_sv_logger lg_sub = null;
// Triggers: a trigger built from something that is not a live edge.
shuttle_sv_logger lg_trigger = null;
// Mailbox queue: capacity overflow, close. Same object, same name on both
// sides (Mailbox in C++, shuttle_mailbox in SV).
shuttle_sv_logger lg_mailbox = null;
// Request/response: calls going out, replies demultiplexed by corr_id, stale
// replies, cancelled calls, responder registration.
shuttle_sv_logger lg_requester = null;

function automatic shuttle_sv_logger trace_pub();
  if (lg_pub == null) lg_pub = new ("shuttle.pub");
  return lg_pub;
endfunction

function automatic shuttle_sv_logger trace_sub();
  if (lg_sub == null) lg_sub = new ("shuttle.sub");
  return lg_sub;
endfunction

function automatic shuttle_sv_logger trace_trigger();
  if (lg_trigger == null) lg_trigger = new ("shuttle.trigger");
  return lg_trigger;
endfunction

function automatic shuttle_sv_logger trace_mailbox();
  if (lg_mailbox == null) lg_mailbox = new ("shuttle.mailbox");
  return lg_mailbox;
endfunction

function automatic shuttle_sv_logger trace_requester();
  if (lg_requester == null) lg_requester = new ("shuttle.requester");
  return lg_requester;
endfunction

// The registration kind as a word. Spelled here instead of at the call sites
// because a SystemVerilog ternary on two string literals returns a vector of the
// longer one's width, and %s would print the padding before the shorter word.
function automatic string shuttle_entry_name(bit blocking);
  if (blocking) return "blocking";
  return "nonblocking";
endfunction

`endif  // SHUTTLE_TRACE_SVH
