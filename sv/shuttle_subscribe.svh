`ifndef SHUTTLE_SUBSCRIBE_SVH
`define SHUTTLE_SUBSCRIBE_SVH

// ── Callback interfaces: existing classes implement them to plug in, without
//    spending the single-inheritance slot ──────────────────────────────────
interface class shuttle_sink;
  // Blocking callback: the landing implementation of the blocking entry —
  // invoked by the delivery units of `serial`/`concurrent` and by the defer
  // section of `publish_now` (runs in a background simulation process, may
  // consume time)
  pure virtual task invoke(input byte payload[], input shuttle_meta meta);
endclass

interface class shuttle_sink_nb;
  // Nonblocking callback: the landing implementation of the nonblocking
  // entry — invoked synchronously by `publish_now` inside the publisher's
  // process, and also by `serial`/`concurrent` (the compiler enforces zero
  // blocking)
  pure virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
endclass

// ── Subscription registry: sub_id → callback object ─────────────────────
// DPI export names must be static, so C++ callbacks always enter via
// shuttle_sv_dispatch[_nb] and this registry dispatches them to user objects
// by sub_id.
class shuttle_registry;
  static local int             next_id_  = 1;
  static local shuttle_sink    sinks_[int];
  static local shuttle_sink_nb sinks_nb_[int];
  // Unsubscribed ids: the C++ side may still have in-flight deliveries (the
  // subscriber set was snapshotted at publish time); when such a delivery
  // arrives, drop it silently (not an SV-side error) but report it back via
  // shuttle_c_sub_retired so the frame records this subscriber's terminal
  // state as retired (instead of a phantom delivered); an id that was never
  // registered is still an error.
  static local bit             retired_[int];

  static function automatic int add(shuttle_sink s, shuttle_sink_nb snb);
    int id = next_id_++;
    if (s   != null) sinks_[id]    = s;
    if (snb != null) sinks_nb_[id] = snb;
    return id;
  endfunction

  static function automatic void remove(int id);
    sinks_.delete(id);
    sinks_nb_.delete(id);
    retired_[id] = 1;
  endfunction

  // Blocking dispatch (`serial`/`concurrent`): called by the exported task shuttle_sv_dispatch
  static task automatic dispatch(int id, byte payload[], shuttle_meta meta);
    if (!sinks_.exists(id)) begin
      // In-flight delivery after unsubscribing: dropped silently on the SV
      // side (no $error), but C++ must be told, otherwise it would record
      // this delivery as delivered (a phantom success).
      if (retired_.exists(id)) begin
        shuttle_c_sub_retired(id);
        return;
      end
      $error("shuttle: subscriber %0d does not exist or lacks the blocking entry", id);
      return;
    end
    sinks_[id].invoke(payload, meta);
  endtask

  // Nonblocking dispatch (`publish_now`): called by the exported function shuttle_sv_dispatch_nb
  static function automatic void dispatch_nb(int id, byte payload[], shuttle_meta meta);
    if (!sinks_nb_.exists(id)) begin
      if (retired_.exists(id)) begin
        shuttle_c_sub_retired(id);
        return;
      end
      $error("shuttle: subscriber %0d lacks the nonblocking entry (publish_now must not dispatch to it)", id);
      return;
    end
    sinks_nb_[id].invoke_nb(payload, meta);
  endfunction
endclass

// ── Subscription handle ────────────────────────────────────────────────
class shuttle_sub;
  local int id_;
  local bit active_;
  function new(int id);
    id_ = id;
    active_ = 1;
  endfunction
  function int id();
    return id_;
  endfunction
  // Explicit unsubscribe (idempotent); no deliveries after unsubscribing
  // (in-progress deliveries are unaffected)
  function void unsubscribe();
    if (!active_) return;
    active_ = 0;
    shuttle_c_unsubscribe(id_);
    shuttle_registry::remove(id_);
  endfunction
endclass

// ── Subscription entry points ──────────────────────────────────────────
// ── Subscription: the entry is decided by *which function is called* ───
// One-to-one with C++ subscribe(topic, fn) / subscribe(topic, nb(fn)):
//   shuttle_subscribe    → register the blocking entry (export task, may consume simulation time)
//   shuttle_subscribe_nb → register the nonblocking entry (export function,
//                            zero delay, enforced by the compiler)
// To get both entries on one object, register twice (two subscribers, two
// callbacks per frame); a non-empty type_tag is validated against the
// topic's type binding, with mismatches reported on the C++ side.
// For "two behaviors behind the same entry", create two objects — SV has no
// closures, which is equivalent to two lambdas on the C++ side.
// The entry is no longer inferred from "which interface classes the class
// implements": the old implementation's $cast probing was exactly the root
// cause of subscriber_count and `publish_now` behavior diverging between C++
// and SV for the same configuration.
// Internal: shared implementation of the two public registration functions
// (VCS 2018.09 rejects local functions inside packages, so this is not
// declared local; the naming convention marks it internal)
function automatic shuttle_sub shuttle_register(string topic, shuttle_sink s,
                                                shuttle_sink_nb snb, string type_tag,
                                                int caps);
  int                   sid = shuttle_registry::add(s, snb);
  automatic shuttle_sub sub;
  // The SV-local id goes to C++ as sv_sid and comes back verbatim at
  // dispatch time; neither side maintains a mapping.
  if (shuttle_c_subscribe(topic, caps, type_tag, sid) != 0) begin
    shuttle_registry::remove(sid);
    $error("shuttle: subscribe failed (topic=%s): %s", topic, shuttle_c_last_error());
    return null;
  end
  sub = new(sid);
  return sub;
endfunction

// blocking entry (SV: export task): ≙ C++ subscribe(topic, fn)
function automatic shuttle_sub shuttle_subscribe(string topic, shuttle_sink cb,
                                                 string type_tag = "");
  return shuttle_register(topic, cb, null, type_tag, SHUTTLE_CAP_BLOCKING);
endfunction

// nonblocking entry (SV: export function): ≙ C++ subscribe(topic, nb(fn))
function automatic shuttle_sub shuttle_subscribe_nb(string topic, shuttle_sink_nb cb,
                                                    string type_tag = "");
  return shuttle_register(topic, null, cb, type_tag, SHUTTLE_CAP_NONBLOCKING);
endfunction

`endif  // SHUTTLE_SUBSCRIBE_SVH
