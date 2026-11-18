// ============================================================================
// shuttle_pkg.sv — SV-side interface of the shuttle publish-subscribe framework
//
// Depends on: weave_pkg (../weave/sv/weave_pkg.sv, compiled by the simulation
// scripts along with this file). The DPI protocol reference is
// docs/dpi_protocol.md; the direction convention matches weave:
//   import (provided by C++): shuttle_c_*  — publish / subscribe registration /
//                                            completion wait / frame pull
//   export (provided by SV) : shuttle_sv_* — subscriber callback dispatch + sim time
//
// Frame-pull protocol (the dispatch payload does not go through DPI arguments):
//   before calling shuttle_sv_dispatch[_nb], C++ sets the current frame's byte
//   stream as process-level context; SV pulls it in chunks inside the export
//   body via shuttle_c_frame_len / shuttle_c_frame_qword (8-byte little-endian
//   chunks). Relies on the simulator's single-active-process invariant (the
//   same premise as weave).
//
// Callback entry model (corresponds to shuttle::Capability on the C++ side):
//   shuttle_sink    — blocking entry (export task, may consume simulation time),
//                     registered via shuttle_subscribe
//   shuttle_sink_nb — nonblocking entry (export function, zero delay),
//                     registered via shuttle_subscribe_nb
//   One subscriber class may implement both (multiple interface-class
//   implementations, zero intrusion into existing code); which entry is used
//   is then decided by which registration function is called — for both,
//   register twice.
//
// Message class convention (no base class to inherit; used by shuttle_pub#(T)):
//   class Txn;
//     int unsigned addr;                 // member types must belong to the codec subset (wire format in the protocol doc)
//     int unsigned data;
//     localparam string type_tag = "Txn";         // must equal the C++ SHUTTLE_REFLECT name
//     function automatic void pack(output byte q[$]);  // streaming-pack in declaration order (big-endian wire order)
//       q = {>>{addr, data}};   // ← must use {>>{...}} (1-bit slices, MSB first)
//     endfunction
//     static function automatic Txn unpack(byte b[]);
//       Txn t = new();
//       {>>{t.addr, t.data}} = b;
//       return t;
//     endfunction
//   endclass
// Note: a string field must first stream a 32-bit length prefix and then the
//       bytes, i.e. {>>{32'(s.len()), s}}, aligned with the C++ Codec.
// Wire-order hint: {>>{...}} yields "field declaration order + each field
//           MSB-first", exactly the C++ Writer's big-endian layout; {<<{...}}
//           reverses the entire bit stream — never mix them up.
// ============================================================================

package shuttle_pkg;
  // timeunit convention matches weave_pkg: it fixes the conversion basis for
  // #() delays and 1s literals (shuttle_sv_time_fs converts $realtime/1s*1e15
  // to femtoseconds).
  timeunit 1ps; timeprecision 1ps;

  // weave_pkg supplies the SV-side blocking primitive (weave_sv_wait) and the
  // event table (weave_sv_create_event / _trigger): shuttle_mailbox is
  // implemented on top of them, entirely in SV (it adds no DPI entry point).
  // weave_pkg.sv must be compiled first (tests/svdpi/run.sh and examples do).
  import weave_pkg::*;

  // ── DPI: imports (provided by libshuttle.so on the C++ side) ───────────
  // All are context imports: the implementations may call back into the
  // exports below (dispatch / time).
  import "DPI-C" context function int shuttle_c_publish_now(
      string topic, string type_tag, byte payload[], int now_policy, int unsigned corr_id = 0);
  import "DPI-C" context function int shuttle_c_publish_serial(
      string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg,
      int unsigned corr_id = 0);
  import "DPI-C" context function int shuttle_c_publish_concurrent(
      string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg,
      int unsigned corr_id = 0);
  import "DPI-C" context task shuttle_c_wait_pub(
      int pub_id, longint timeout_fs, output int status);
  import "DPI-C" context function int shuttle_c_pub_done(int pub_id);
  // Publication counter query: kind 0=expected 1=finished 2=skipped-by-policy
  // 3=deferred 4=retired
  import "DPI-C" context function int shuttle_c_pub_stat(int pub_id, int kind);
  import "DPI-C" context function string shuttle_c_pub_error(int pub_id);
  // Report back to C++ when an in-flight delivery reaches an already-unsubscribed id
  // (no error — just lets C++ record that delivery as retired)
  import "DPI-C" context function void shuttle_c_sub_retired(int sv_sid);
  import "DPI-C" context function int shuttle_c_subscribe(
      string topic, int caps, string type_tag, int sv_sid);
  import "DPI-C" context function void shuttle_c_unsubscribe(int sv_sid);
  import "DPI-C" context function void shuttle_c_set_retained(string topic, bit retained);
  import "DPI-C" context function string shuttle_c_last_error();

  // Frame-pull protocol: before calling shuttle_sv_dispatch[_nb], C++ sets the
  // current frame's byte stream as process-level context; SV pulls it in
  // chunks inside the export body via these two scalar imports (8-byte
  // little-endian chunks).
  import "DPI-C" context function int shuttle_c_frame_len();
  import "DPI-C" context function longint shuttle_c_frame_qword(int index);

  // Cross-language events: DPI entries of the C++-side event registry (used by shuttle_event)
  import "DPI-C" context function int shuttle_c_event_create();
  import "DPI-C" context function void shuttle_c_event_set(int ev_id);
  import "DPI-C" context function void shuttle_c_event_destroy(int ev_id);

  // ── DPI: exports (provided by this package, called back by the C++ broker) ──
  // Same precedent as weave: exports live inside the package (verified on
  // VCS); if a target simulator rejects in-package exports, move these two
  // entries into a module — the protocol is unchanged.
  export "DPI-C" task shuttle_sv_dispatch;
  export "DPI-C" function shuttle_sv_dispatch_nb;
  export "DPI-C" function shuttle_sv_time_fs;

  // ── Constants: strictly aligned with the C++ enums (docs/dpi_protocol.md §constants) ──
  // Subscription entries (caps argument of shuttle_c_subscribe): one
  // registration can only ever be one of them (the same object registered
  // twice = two subscribers — only then can both values be in play)
  localparam int SHUTTLE_CAP_BLOCKING    = 1;  // blocking entry: shuttle_subscribe
  localparam int SHUTTLE_CAP_NONBLOCKING = 2;  // nonblocking entry: shuttle_subscribe_nb
  // Policy for publish_now meeting blocking-entry subscribers (now_policy argument)
  localparam int SHUTTLE_NOW_REJECT = 0;  // error (deliver to no subscriber at all)
  localparam int SHUTTLE_NOW_SKIP   = 1;  // skip these subscribers: not in subscriber_count(), only in skipped()
  localparam int SHUTTLE_NOW_DEFER  = 2;  // default: hand to the topic's background serial chain, callback later
  // Trigger kinds (trig_kind argument of shuttle_c_publish_serial / _concurrent)
  localparam int SHUTTLE_TRIG_NONE  = 0;  // start immediately
  localparam int SHUTTLE_TRIG_DELAY = 1;  // trig_arg = simulation delay (femtoseconds)
  localparam int SHUTTLE_TRIG_EVENT = 2;  // trig_arg = C++-side event handle id (weave/shuttle Event)
  localparam int SHUTTLE_TRIG_PUB   = 3;  // trig_arg = the pub_id of another Publication
  // status output of shuttle_c_wait_pub
  localparam int SHUTTLE_WAIT_OK      = 0;  // all completed (not necessarily failure-free)
  localparam int SHUTTLE_WAIT_TIMEOUT = 1;
  localparam int SHUTTLE_WAIT_FAILED  = 2;  // subscriber failure(s) present (see shuttle_c_pub_error)
                                            // or an internal error (see shuttle_c_last_error)
  // Overflow policy of a bounded mailbox (mirrors shuttle::OverflowPolicy).
  // `block` and `reject` have no SV counterpart: a delivery callback must not
  // block, and SV has no exceptions to reject with.
  localparam int SHUTTLE_OVERFLOW_DROP_OLDEST = 0;
  localparam int SHUTTLE_OVERFLOW_DROP_NEWEST = 1;

  // ── Metadata: passed with each delivery, mirrors shuttle::Meta on the C++ side ──
  class shuttle_meta;
    string  topic_name;
    string  type_tag;
    longint topic_seq;
    longint time_fs;
    // Correlation id of this frame, assigned by the publisher (0 = uncorrelated).
    // Echo it when publishing a response to a request (see `shuttle_pub::publish_*`).
    int unsigned corr_id;
    function new(string topic_name, string type_tag, longint topic_seq, longint time_fs,
                 int unsigned corr_id = 0);
      this.topic_name = topic_name;
      this.type_tag   = type_tag;
      this.topic_seq  = topic_seq;
      this.time_fs    = time_fs;
      this.corr_id    = corr_id;
    endfunction
  endclass

  // ── Frame pull: byte stream of the C++ dispatch context → SV dynamic array ──
  // Protocol: shuttle_c_frame_qword(i) returns the i-th 8-byte chunk (little
  // endian); concatenating them yields the complete frame.
  // Relies on the single-active-process invariant: callable only inside the
  // shuttle_sv_dispatch[_nb] body.
  function automatic void shuttle_frame_bytes(output byte b[]);
    int n = shuttle_c_frame_len();
    b = new [n];
    for (int base = 0; base < n; base += 8) begin
      longint qw = shuttle_c_frame_qword(base / 8);
      for (int j = 0; j < 8 && base + j < n; j++)
        b[base + j] = byte'(qw[j * 8 +: 8]);
    end
  endfunction

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
    static local int            next_id_  = 1;
    static local shuttle_sink    sinks_[int];
    static local shuttle_sink_nb sinks_nb_[int];
    // Unsubscribed ids: the C++ side may still have in-flight deliveries (the
    // subscriber set was snapshotted at publish time); when such a delivery
    // arrives, drop it silently (not an SV-side error) but report it back via
    // shuttle_c_sub_retired so the frame records this subscriber's terminal
    // state as retired (instead of a phantom delivered); an id that was never
    // registered is still an error.
    static local bit            retired_[int];

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

  // ── Mailbox: a subscription with a queue (mirror of shuttle::Mailbox<T>) ──
  // "Wait for the next frame" instead of "let a callback handle each frame".
  // A mailbox **is one subscriber**: it registers through shuttle_subscribe_nb
  // (nonblocking entry — pushing into the queue never blocks), so it joins the
  // target set, counts in shuttle_handle::subscriber_count() and receives
  // retained replays exactly like any other subscription.
  //
  // Whole implementation is pure SV: the queue is an SV queue and the wakeup
  // goes through weave_pkg's event primitives (weave_sv_wait / weave_sv_trigger),
  // so a mailbox needs no DPI entry point at all.
  //
  // Ordering: queue order = callback invocation order (serial = join order,
  // concurrent = completion order, now = registration order).
  // Capacity: capacity == 0 is unbounded; a bounded mailbox drops the oldest or
  // the newest item and counts it in overflowed() (no blocking, no exceptions).
  class shuttle_mb_item;
    byte         data[];
    shuttle_meta meta;
    function new(input byte d[], input shuttle_meta m);
      data = d;  // dynamic array assignment copies
      meta = m;
    endfunction
  endclass

  // The queue, the counters and the wakeup carrier. This is also the landing
  // implementation of the nonblocking entry: pushing never blocks, so it is a
  // legal export function body.
  class shuttle_mailbox_sink implements shuttle_sink_nb;
    local int             ev_;  // weave event id used as the wakeup carrier
    local shuttle_mb_item q[$];
    local int             capacity_;
    local int             overflow_;
    local bit             closed_;
    local longint         received_;
    local longint         overflowed_;

    function new(int capacity, int overflow);
      capacity_ = capacity;
      overflow_ = overflow;
      closed_   = 0;
      ev_       = weave_sv_create_event();
    endfunction

    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      push(payload, meta);
    endfunction

    function void push(input byte payload[], input shuttle_meta meta);
      if (closed_) return;  // closed: no new frames (queued ones stay readable)
      if (capacity_ > 0 && q.size() >= capacity_) begin
        if (overflow_ == SHUTTLE_OVERFLOW_DROP_NEWEST) begin
          overflowed_++;
          return;
        end
        q.pop_front();  // DROP_OLDEST (and the default)
        overflowed_++;
      end
      begin
        shuttle_mb_item it = new(payload, meta);
        q.push_back(it);
      end
      received_++;
      weave_sv_trigger(ev_);  // schedules waiters, never runs them inline
    endfunction

    function bit try_recv(output byte payload[], output shuttle_meta meta);
      shuttle_mb_item it;
      if (q.size() == 0) begin
        payload = {};
        return 0;
      end
      it      = q.pop_front();
      payload = it.data;
      meta    = it.meta;
      return 1;
    endfunction

    // Blocking receive: suspends the calling process until a frame arrives, the
    // timeout expires (timeout == 0 means wait forever), or the mailbox is
    // closed and drained. ok == 0 reports the two latter cases.
    task automatic recv(output bit ok, output byte payload[], output shuttle_meta meta,
                        input time timeout = 0);
      int             status;
      bit             has_deadline;
      time            deadline;
      longint         ticks;
      shuttle_mb_item it;
      ok           = 0;
      payload      = {};
      has_deadline = (timeout > 0);
      if (has_deadline) deadline = $time + timeout;
      forever begin
        if (q.size() > 0) begin
          it      = q.pop_front();
          payload = it.data;
          meta    = it.meta;
          ok      = 1;
          return;
        end
        if (closed_) return;  // drained and closed: nothing will ever arrive
        // The deadline is checked here (not from weave_sv_wait's status) because
        // the event's "triggered in this slot" flag makes status ambiguous when
        // a push and the timeout land in the same time slot.
        if (has_deadline) begin
          if ($time >= deadline) return;
          ticks = longint'(deadline - $time);  // package timeunit is 1ps: 1 tick == 1ps
        end else begin
          ticks = -1;  // no timeout leg: wait forever
        end
        weave_sv_wait(ev_, ticks, -1, status);
      end
    endtask

    function int size();
      return q.size();
    endfunction

    function longint received();
      return received_;
    endfunction

    function longint overflowed();
      return overflowed_;
    endfunction

    function bit closed();
      return closed_;
    endfunction

    function void close();
      if (closed_) return;
      closed_ = 1;
      weave_sv_trigger(ev_);
    endfunction
  endclass

  class shuttle_mailbox;
    local shuttle_sub         sub_;   // nonblocking registration
    local shuttle_mailbox_sink sink_; // owns the queue; must outlive the registration

    function new(string pattern, string type_tag = "", int capacity = 0,
                 int overflow = SHUTTLE_OVERFLOW_DROP_OLDEST);
      sink_ = new(capacity, overflow);
      sub_  = shuttle_subscribe_nb(pattern, sink_, type_tag);
    endfunction

    function bit valid();
      return sub_ != null;
    endfunction

    function bit try_recv(output byte payload[], output shuttle_meta meta);
      return sink_.try_recv(payload, meta);
    endfunction

    task automatic recv(output bit ok, output byte payload[], output shuttle_meta meta,
                        input time timeout = 0);
      sink_.recv(ok, payload, meta, timeout);
    endtask

    function int size();
      return sink_.size();
    endfunction

    function longint received();
      return sink_.received();
    endfunction

    function longint overflowed();
      return sink_.overflowed();
    endfunction

    function bit closed();
      return sink_.closed();
    endfunction

    // Idempotent: unsubscribe and wake every waiter. Queued frames stay readable.
    function void close();
      if (sub_ != null) sub_.unsubscribe();
      sink_.close();
    endfunction
  endclass

  // ── Publication completion handle (corresponds to shuttle::Publication) ──
  // SV has no exceptions: subscriber failures surface via first_error(), and
  // the user decides how to handle them.
  class shuttle_handle;
    local int pub_id_;
    function new(int pub_id);
      pub_id_ = pub_id;
    endfunction
    function int id();
      return pub_id_;
    endfunction
    function bit valid();
      return pub_id_ > 0;
    endfunction
    function bit done();
      return valid() && shuttle_c_pub_done(pub_id_) != 0;
    endfunction
    // Block until all subscriber callbacks complete; timeout_fs = 0 means
    // wait forever (unit: femtoseconds)
    task wait_done(longint timeout_fs = 0);
      int status;
      if (!valid()) return;
      shuttle_c_wait_pub(pub_id_, timeout_fs, status);
      if (status == SHUTTLE_WAIT_TIMEOUT)
        $error("shuttle: wait on Publication %0d timed out", pub_id_);
      else if (status == SHUTTLE_WAIT_FAILED)
        $error("shuttle: Publication %0d has subscriber failure(s): %s", pub_id_, first_error());
    endtask
    // ── Visibility counters (same names and semantics as C++ Publication) ──
    // Target subscriber count of this frame (snapshot at publish time, including the deferred part)
    function int subscriber_count();
      return valid() ? shuttle_c_pub_stat(pub_id_, 0) : 0;
    endfunction
    // Subscribers finished (successfully or not)
    function int finished();
      return valid() ? shuttle_c_pub_stat(pub_id_, 1) : 0;
    endfunction
    // Subscribers skipped by policy, to whom this frame will not be delivered
    function int skipped();
      return valid() ? shuttle_c_pub_stat(pub_id_, 2) : 0;
    endfunction
    // Subscribers deferred to the background serial chain, delivered later
    function int deferred();
      return valid() ? shuttle_c_pub_stat(pub_id_, 3) : 0;
    endfunction
    // Subscribers unsubscribed after the snapshot, never entered the callback (already counted in finished)
    function int retired();
      return valid() ? shuttle_c_pub_stat(pub_id_, 4) : 0;
    endfunction
    // Error message of the first failed subscriber (empty = no failure)
    function string first_error();
      if (!valid()) return "";
      return shuttle_c_pub_error(pub_id_);
    endfunction
  endclass

  // ── Cross-language event handle (the C++-side registry owns the underlying
  //    object; the C++ side waits on it with weave::Event) ──────────────────
  // The underlying object resides in the C++-side registry (held by C++; SV
  // only holds the id): the SV-side set() wakes TRIG_EVENT delivery units
  // waiting on it; it can also serve as the trigger condition of
  // shuttle_trigger::after_event(id) (below).
  // There is no public C++ entry that waits by id: C++ code uses its own
  // weave::Event + after().
  class shuttle_event;
    local int id_;
    local bit active_;
    function new();
      id_ = shuttle_c_event_create();
      active_ = (id_ > 0);
      if (!active_) $error("shuttle: failed to create event: %s", shuttle_c_last_error());
    endfunction
    function int id();
      return id_;
    endfunction
    function bit valid();
      return active_;
    endfunction
    // Wake all waiters (Trigger / TRIG_EVENT tasks on the C++ side waiting on this event are released)
    function void set();
      if (active_) shuttle_c_event_set(id_);
    endfunction
    // Explicit release (may be omitted at end of simulation; dies with the process)
    function void close();
      if (!active_) return;
      active_ = 0;
      shuttle_c_event_destroy(id_);
    endfunction
  endclass

  // ── Deferred trigger (corresponds to shuttle::Trigger) ─────────────────
  // Snapshot semantics match C++: the subscriber set and the message copy are
  // locked in at the moment of the publish call; the trigger only decides when
  // the background callback process starts.
  class shuttle_trigger;
    int     kind = SHUTTLE_TRIG_NONE;
    longint arg  = 0;
    static function shuttle_trigger none();
      shuttle_trigger t = new();
      return t;
    endfunction
    // Simulation delay (femtoseconds)
    static function shuttle_trigger after_delay(longint fs);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_DELAY;
      t.arg  = fs;
      return t;
    endfunction
    // Event handle id (created only on the SV side; the C++ side waits by id,
    // see detail::event_wait_id). Pass an explicit id for a shuttle_event:
    // after_event(ev.id()) (a class handle and an int support only a single
    // implicit conversion; VCS rejects the two as overloads)
    static function shuttle_trigger after_event(int ev_id);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_EVENT;
      t.arg  = ev_id;
      return t;
    endfunction
    // Another publication completes (success or failure both count as done)
    static function shuttle_trigger after_pub(shuttle_handle h);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_PUB;
      t.arg  = h.id();
      return t;
    endfunction
  endclass

  // ── Typed publisher ────────────────────────────────────────────────────
  // The type parameter T must satisfy the message-class convention at the top
  // of this file (pack() + localparam type_tag).
  class shuttle_pub #(type T = byte);
    local string topic_;
    function new(string topic);
      topic_ = topic;
    endfunction

    // Queue → dynamic array (DPI open-array parameters require dynamic arrays;
    // VCS does not support unpacked arrays as function return types)
    local function automatic void q2a(const ref byte q[$], output byte a[]);
      a = new [q.size()];
      foreach (q[i]) a[i] = q[i];
    endfunction

    // ── Publish trio: names correspond 1:1 to publish_now / publish_serial /
    //    publish_concurrent on the C++ side; the scheduling flavor is in the
    //    function name — there is no mode argument ──────────────────────────

    // Message → dynamic array needed by the DPI open array (pack() produces a
    // queue; see the message-class convention at the top of this file)
    local function void pack_payload(T msg, output byte payload[]);
      byte q[$];
      msg.pack(q);
      q2a(q, payload);
    endfunction

    // Trigger → (trig_kind, trig_arg) scalars; null means start immediately
    local function void trig_scalars(shuttle_trigger trig, output int kind, output longint arg);
      kind = SHUTTLE_TRIG_NONE;
      arg  = 0;
      if (trig != null) begin
        kind = trig.kind;
        arg  = trig.arg;
      end
    endfunction

    // pub_id → handle; returns null on failure. loud=1 additionally prints $error.
    // `publish_now` stays silent (under the reject policy, failure is the
    // expected outcome; the reason goes through shuttle_c_last_error());
    // `serial`/`concurrent` print because they are fire-and-forget async
    // interfaces where errors must be visible immediately.
    local function shuttle_handle wrap(int pid, string what, bit loud);
      if (pid <= 0) begin
        if (loud)
          $error("shuttle: %s publish failed (topic=%s): %s", what, topic_, shuttle_c_last_error());
        return null;
      end
      begin
        shuttle_handle h = new(pid);
        return h;
      end
    endfunction

    // `publish_now`: synchronous callbacks on the publisher's stack, never
    // suspending (invokes all subscribers with a nonblocking entry).
    // policy: how to handle targets whose entry is blocking (cannot be called
    //         on the caller's stack, i.e. the shuttle_sink invoke task);
    //         defaults to SHUTTLE_NOW_DEFER (hand to the background serial
    //         chain, callback later).
    //         — one registration has exactly one entry; the policy only sees
    //         that entry and is independent of whether the topic also has
    //         nonblocking subscribers.
    // Returns this frame's handle (same type as `serial`/`concurrent`): the
    // nonblocking part has fully finished when it returns, skipped targets are
    // in skipped(), and deferred targets are still on the background chain —
    // done()/wait_done() cover that part.
    // Returns null on failure (see shuttle_c_last_error()).
    function shuttle_handle publish_now(T msg, int policy = SHUTTLE_NOW_DEFER);
      byte payload[];
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_now(topic_, T::type_tag, payload, policy, 0), "now", 1'b0);
    endfunction

    // `publish_serial`: background single-chain, in-order callbacks
    // (topic-level FIFO); returns the completion handle immediately.
    function shuttle_handle publish_serial(T msg, shuttle_trigger trig = null);
      byte payload[];
      int kind;
      longint arg;
      trig_scalars(trig, kind, arg);
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_serial(topic_, T::type_tag, payload, kind, arg, 0),
                  "serial", 1'b1);
    endfunction

    // `publish_concurrent`: background, one task per subscriber, concurrent
    // callbacks; returns the completion handle immediately.
    function shuttle_handle publish_concurrent(T msg, shuttle_trigger trig = null);
      byte payload[];
      int kind;
      longint arg;
      trig_scalars(trig, kind, arg);
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_concurrent(topic_, T::type_tag, payload, kind, arg, 0),
                  "concurrent", 1'b1);
    endfunction

    // Responder shorthand: publish `msg` as the answer to `request`. The frame inherits
    // request.corr_id, which is what lets the C++ side route it to its Call. This is the SV
    // counterpart of serve() and the *only* place correlation is visible; publish_now /
    // publish_serial / publish_concurrent stay unaware of it (they leave corr_id 0), so a
    // publisher never has to think about correlation.
    //
    // It goes out through publish_now so that a nonblocking C++ requester has the answer on
    // its stack before this call returns (no task switch). `defer` keeps it correct even if
    // a blocking subscriber slipped onto the response topic before it was marked.
    function shuttle_handle reply(T msg, shuttle_meta request);
      byte payload[];
      pack_payload(msg, payload);
      return wrap(
          shuttle_c_publish_now(topic_, T::type_tag, payload, SHUTTLE_NOW_DEFER, request.corr_id),
          "reply", 1'b1);
    endfunction

    // retained (latched): once enabled, the broker caches the latest frame
    // and new subscribers receive it immediately
    function void set_retained(bit r);
      shuttle_c_set_retained(topic_, r);
    endfunction

  endclass

  // ── DPI export implementations: callback entries of the C++ broker ─────
  // Dispatch of `serial`/`concurrent`: called by the C++ weave task; this task
  // runs in the SV process hosting that weave task and may legally consume
  // simulation time. The payload is obtained via the frame-pull protocol (see
  // the header of this file).
  task automatic shuttle_sv_dispatch(input int sub_id, input string topic_name,
                                     input string type_tag, input longint topic_seq,
                                     input longint time_fs, input int unsigned corr_id);
    shuttle_meta m = new(topic_name, type_tag, topic_seq, time_fs, corr_id);
    byte payload[];
    shuttle_frame_bytes(payload);
    shuttle_registry::dispatch(sub_id, payload, m);
  endtask

  // Dispatch of `publish_now`: runs in the publisher's process context and
  // must be zero-delay (the compiler enforces this for a function). The
  // payload is obtained via the frame-pull protocol.
  function automatic void shuttle_sv_dispatch_nb(input int sub_id, input string topic_name,
                                                 input string type_tag, input longint topic_seq,
                                                 input longint time_fs,
                                                 input int unsigned corr_id);
    shuttle_meta m = new(topic_name, type_tag, topic_seq, time_fs, corr_id);
    byte payload[];
    shuttle_frame_bytes(payload);
    shuttle_registry::dispatch_nb(sub_id, payload, m);
  endfunction

  // Simulation time source (implementation of shuttle::now_fs() on the C++
  // side). $realtime is in this package's timeunit; converted to femtoseconds.
  // Note: real has only a 53-bit mantissa, so precision degrades at very large
  // simulation times — a production implementation should switch to $time ×
  // femtoseconds-per-tick (aligned with the tick convention of weave_pkg).
  function automatic longint shuttle_sv_time_fs();
    return longint'($realtime / 1s * 1e15);
  endfunction

endpackage
