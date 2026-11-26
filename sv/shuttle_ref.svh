`ifndef SHUTTLE_REF_SVH
`define SHUTTLE_REF_SVH

// ── Reference messaging: objects travel by reference, the frame carries an id ──
// A broker frame is a value with declared fields, so a class holding a queue, a nested
// handle or an interface can never be a payload. This file is the sanctioned way to send
// one anyway, without learning a new mechanism: the payload is a 4-byte id and the type tag
// defaults to $typename(T), so the frame is an ordinary value frame — name ∩ type targeting, retained
// replay, triggers, serial/concurrent/now ordering and cross-language routing all behave
// exactly as for a reflected message class.
//
//   class FObj;  // the object: any class, no reflection needed
//     int v; int hist[$];
//   endclass
//
//   shuttle_ref_publisher #(FObj) pub = new("demo.obj");
//   shuttle_publication h = pub.publish_serial(obj);      // same trio as shuttle_publisher
//   shuttle_ref_mailbox #(FObj) box = new("demo.obj");    // same shape as shuttle_mailbox
//   automatic FObj got = null;
//   if (box.try_recv(got, meta)) ...                      // got === obj
//
// The id table below is internal — users never see an id. Only the owning side resolves;
// a peer that just forwards the frame treats the payload as data.
//
// Request/response needs no reference flavour either: shuttle_requester and shuttle_serve carry
// any reflected message, and a message may embed a reference next to its data through
// shuttle_ref_codec (typedef + `shuttle_field`, see the codec section below).
//
// Constraints that follow from "the id is a table index":
//   · T must be the *same* class type on every side that names it (declare it in a package
//     or in a file both sides include): the table is per-T. The default tag is $typename(T),
//     which carries the declaring scope ("my_module.FObj"), so a *cross-language* peer passes
//     the mirror's spelling explicitly — the last argument, exactly like shuttle_mailbox's
//     type_tag — and a C++ peer reflects that name on a struct with one u32 field.
//   · the table holds a strong reference. The publisher drops an object with free(obj)
//     (or clear()) once no in-flight or retained frame carries it; a frame whose id was
//     freed is a loud $error at delivery, never a silent alias onto another object.
//   · ids are allocated monotonically and never reused.
//
// One class, several reference types. SV has no overloading, so a class cannot implement
// shuttle_ref_sink #(A) and shuttle_ref_sink #(B) at once — both declare on_msg, and the compiler
// reports the second as "identifier previously declared". Three shapes work instead; pick by what
// the receiver needs:
//   · an open-ended type set: implement the untyped shuttle_sink / shuttle_sink_nb once and
//     dispatch on meta.type_tag, one line per arm through shuttle_ref_msg #(T)::unpack(payload) —
//     the mirror of T::unpack(payload) on the value path;
//   · a type hierarchy: one shuttle_ref_sink #(Base), $cast down inside on_msg (the encoded tag is
//     then the base's name, so a subscriber filtering on a derived name does not match);
//   · queue semantics: one shuttle_ref_mailbox #(T) per type, drained by one service loop — no
//     adapter class at all.
// Untyped callbacks that want to stay fully static use one 4-line adapter class per type (each
// implementing shuttle_ref_sink #(T) and forwarding to a plain method of the owner): distinct
// classes never share a method name, so nothing collides.

// ── Internal id table (per #(T) specialization) ────────────────────────
// by_id_ is the id → object map; ids_/objs_ is the reverse cache that keeps a re-published
// object on one id. A queue with a linear scan, not an associative array, because VCS 2018.09
// does not accept a handle as an associative-array index.
class shuttle_ref_table #(type T);
  static T            by_id_[int unsigned];
  static int unsigned next_ = 1;
  static T            objs_[$];
  static int unsigned ids_[$];

  static function int unsigned id_of(input T obj);
    if (obj == null) begin
      $error("shuttle: null handle published (tag \"%s\")", $typename(T));
      return 0;
    end
    foreach (objs_[i])
      if (objs_[i] == obj) return ids_[i];
    begin
      int unsigned id = next_++;
      by_id_[id] = obj;
      objs_.push_back(obj);
      ids_.push_back(id);
      return id;
    end
  endfunction

  static function T of_id(input int unsigned id);
    if (id == 0) return null;  // malformed frame: shuttle_ref_unpack already spoke
    if (by_id_.exists(id)) return by_id_[id];
    $error("shuttle: frame carries unknown object id %0d (tag \"%s\") — freed, or sent by another type",
           id, $typename(T));
    return null;
  endfunction

  // Quiet probe, for callers that must not trip the loud path.
  static function bit known(input int unsigned id);
    return by_id_.exists(id);
  endfunction

  static function bit free_obj(input T obj);
    foreach (objs_[i]) begin
      if (objs_[i] == obj) begin
        by_id_.delete(ids_[i]);
        objs_.delete(i);
        ids_.delete(i);
        return 1;
      end
    end
    return 0;
  endfunction
endclass

// ── Encoded form: one u32 through the standard codec ──────────────────
// The reference is encoded by shuttle_u32, i.e. exactly the codec a reflected `logic [31:0]`
// field uses, so the bytes are the C++ Reader/Writer's big-endian u32 with no second byte
// layout to keep in sync. A malformed frame (cannot happen through the broker) is reported
// once here and yields id 0, which no table entry ever has.
function automatic void shuttle_ref_pack(input int unsigned id, output byte payload[]);
  byte q[$];
  shuttle_u32::pack_obj(q, id);
  shuttle_q2a(q, payload);
endfunction

function automatic int unsigned shuttle_ref_unpack(input byte payload[]);
  int          pos = 0;
  int unsigned id;
  if (payload.size() != 4) begin
    $error("shuttle: a reference frame must be 4 bytes, got %0d", payload.size());
    return 0;
  end
  shuttle_u32::unpack_into(payload, pos, id);
  return id;
endfunction

// ── Whole-frame resolve: payload → handle ─────────────────────────────
// For a sink that receives several reference types. A typed sink cannot be implemented twice by
// one class (see the header), so such a receiver implements shuttle_sink_nb once and dispatches
// on meta.type_tag — this is the one-liner per arm, and no id reaches the caller:
//
//   virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
//     if (meta.type_tag == "FObj") last_f = shuttle_ref_msg #(FObj)::unpack(payload);
//     else if (meta.type_tag == "SObj") last_s = shuttle_ref_msg #(SObj)::unpack(payload);
//   endfunction
class shuttle_ref_msg #(type T);
  static function T unpack(input byte payload[]);
    return shuttle_ref_table #(T)::of_id(shuttle_ref_unpack(payload));
  endfunction
endclass

// ── Codec: a reference as a field of an ordinary message ───────────────
// A value message may embed references; `shuttle_field` takes a codec *name*, so a
// parameterized codec arrives through a typedef:
//
//   typedef shuttle_ref_codec #(FObj) FObj_link;
//   class AskObj;
//     FObj         obj;      // 4 bytes in the encoding, resolved through the table
//     logic [31:0] mode;
//     `shuttle_reflect_begin(AskObj)
//       `shuttle_field(obj, FObj_link)
//       `shuttle_field(mode, shuttle_u32)
//     `shuttle_reflect_end
//   endclass
//
// This is why the reference path has no requester of its own: request/response, serve,
// mailbox and publisher already carry any reflected message, and a message may carry an
// object next to its data. Vector fields and nested messages compose the same way (wrap the
// codec in the usual shim, see sv/shuttle_codec.svh).
class shuttle_ref_codec #(type T);
  static function void pack_obj(ref byte q[$], input T v);
    if (v == null) begin
      $error("shuttle: packing a null reference (tag \"%s\")", $typename(T));
      shuttle_u32::pack_obj(q, 0);
      return;
    end
    shuttle_u32::pack_obj(q, shuttle_ref_table #(T)::id_of(v));
  endfunction

  static function void unpack_into(const ref byte b[], ref int pos, output T v);
    int unsigned id;
    shuttle_u32::unpack_into(b, pos, id);
    v = shuttle_ref_table #(T)::of_id(id);  // null + $error on an id nobody holds
  endfunction
endclass

// ── Callback interface ─────────────────────────────────────────────────
// on_msg is a function: resolving a handle cannot consume simulation time (same rule as
// shuttle_responder::respond). A receiver that needs time queues the handle in its own
// shuttle_ref_mailbox or task.
interface class shuttle_ref_sink #(type T);
  pure virtual function void on_msg(input T value, input shuttle_meta meta);
endclass

// Landing implementation of both entries: the blocking task and the nonblocking function
// resolve the id and call on_msg. Which one is registered is decided by the factory used
// (::blocking / ::nonblocking), never by what this class implements.
class shuttle_ref_cb #(type T) implements shuttle_sink, shuttle_sink_nb;
  local shuttle_ref_sink #(T) sink_;
  function new(shuttle_ref_sink #(T) sink);
    sink_ = sink;
  endfunction
  virtual task invoke(input byte payload[], input shuttle_meta meta);
    T obj = shuttle_ref_msg #(T)::unpack(payload);
    if (obj != null) sink_.on_msg(obj, meta);  // null = the table already reported it
  endtask
  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
    T obj = shuttle_ref_msg #(T)::unpack(payload);
    if (obj != null) sink_.on_msg(obj, meta);
  endfunction
endclass

// ── Publisher: the same trio as shuttle_publisher, taking an object handle ──
class shuttle_ref_publisher #(type T);
  local string topic_;
  local string tag_;
  local T      seen_[$];  // objects this publisher handed out ids for (for free/clear)

  // type_tag: empty means $typename(T) (which includes the declaring scope); pass the mirror's
  // spelling when a peer in another language, or a value-typed subscriber, must match it.
  function new(string topic, string type_tag = "");
    topic_ = topic;
    tag_   = (type_tag == "") ? $typename(T) : type_tag;
  endfunction

  local function string tag();
    return tag_;
  endfunction

  local function int unsigned remember(input T obj);
    int unsigned id = shuttle_ref_table #(T)::id_of(obj);
    if (id == 0) return 0;
    foreach (seen_[i])
      if (seen_[i] == obj) return id;
    seen_.push_back(obj);
    return id;
  endfunction

  local function shuttle_publication wrap(int pid, string what, bit loud);
    if (pid <= 0) begin
      if (loud) $error("shuttle: %s publish failed (topic=%s): %s", what, topic_, shuttle_c_last_error());
      return null;
    end
    begin
      shuttle_publication h = new(pid);
      return h;
    end
  endfunction

  function shuttle_publication publish_now(T obj, int policy = SHUTTLE_NOW_DEFER);
    byte payload[];
    int unsigned id = remember(obj);
    if (id == 0) return null;
    shuttle_ref_pack(id, payload);
    return wrap(shuttle_c_publish_now(topic_, tag(), payload, policy, 0), "now", 1'b0);
  endfunction

  function shuttle_publication publish_serial(T obj, shuttle_trigger trig = null);
    byte payload[];
    int unsigned id = remember(obj);
    int kind;
    longint arg;
    string name;
    if (id == 0) return null;
    shuttle_ref_pack(id, payload);
    shuttle_trig_scalars(trig, kind, arg, name);
    return wrap(shuttle_c_publish_serial(topic_, tag(), payload, kind, arg, name, 0), "serial", 1'b1);
  endfunction

  function shuttle_publication publish_concurrent(T obj, shuttle_trigger trig = null);
    byte payload[];
    int unsigned id = remember(obj);
    int kind;
    longint arg;
    string name;
    if (id == 0) return null;
    shuttle_ref_pack(id, payload);
    shuttle_trig_scalars(trig, kind, arg, name);
    return wrap(shuttle_c_publish_concurrent(topic_, tag(), payload, kind, arg, name, 0), "concurrent",
                1'b1);
  endfunction

  function void set_retained(bit r);
    shuttle_c_set_retained(topic_, r);
  endfunction

  // Drop the id bound to obj: call once no in-flight or retained frame carries it. Frames still
  // in flight after this are a loud error at delivery (never a silent alias). `free` (not
  // `release`) because VCS 2018.09 reserves the latter.
  function void free(T obj);
    void'(shuttle_ref_table #(T)::free_obj(obj));
    foreach (seen_[i])
      if (seen_[i] == obj) begin
        seen_.delete(i);
        break;
      end
  endfunction

  // Drop every id this publisher handed out (teardown).
  function void clear();
    foreach (seen_[i]) void'(shuttle_ref_table #(T)::free_obj(seen_[i]));
    seen_.delete();
  endfunction
endclass

// ── Callback subscription: entry chosen by the factory called ──────────
// ::blocking mirrors shuttle_subscribe (the callback may run on the serial chain, i.e. inside
// a background delivery task); ::nonblocking mirrors shuttle_subscribe_nb (invoked inline,
// also by publish_now).
class shuttle_ref_subscription #(type T);
  local shuttle_subscription sub_;

  static function shuttle_ref_subscription #(T) blocking(string topic, shuttle_ref_sink #(T) sink,
                                                         string type_tag = "");
    return make(topic, sink, type_tag, 1'b1);
  endfunction

  static function shuttle_ref_subscription #(T) nonblocking(string topic,
                                                            shuttle_ref_sink #(T) sink,
                                                            string type_tag = "");
    return make(topic, sink, type_tag, 1'b0);
  endfunction

  function bit valid();
    return sub_ != null;
  endfunction

  function int id();
    return (sub_ == null) ? 0 : sub_.id();
  endfunction

  function void unsubscribe();
    if (sub_ != null) sub_.unsubscribe();
  endfunction

  local static function shuttle_ref_subscription #(T) make(string topic, shuttle_ref_sink #(T) sink,
                                                           string type_tag, bit blocking_entry);
    shuttle_ref_subscription #(T) sub = new ();
    shuttle_ref_cb #(T) cb;
    if (sink == null) begin
      $error("shuttle: reference subscription without a sink (topic=%s)", topic);
      return sub;
    end
    cb = new (sink);
    if (blocking_entry)
      sub.sub_ = shuttle_subscribe(topic, cb, (type_tag == "") ? $typename(T) : type_tag);
    else
      sub.sub_ = shuttle_subscribe_nb(topic, cb, (type_tag == "") ? $typename(T) : type_tag);
    return sub;
  endfunction
endclass

// ── Mailbox: the shuttle_mailbox shape, queueing resolved handles ──────
// The landing implementation is the nonblocking entry (pushing never blocks); retained
// frames are replayed into the queue at construction, resolved at that moment.
class shuttle_ref_mailbox_sink #(type T) implements shuttle_sink_nb;
  local int                  ev_;
  local T                    q[$];
  local shuttle_meta         qm[$];
  local int                  capacity_;
  local int                  overflow_;
  local bit                  closed_;
  local longint              received_;
  local longint              overflowed_;
  local longint              unresolved_;

  function new(int capacity, int overflow);
    capacity_ = capacity;
    overflow_ = overflow;
    closed_   = 0;
    ev_       = weave_sv_create_event();
  endfunction

  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
    int unsigned id = shuttle_ref_unpack(payload);
    if (id == 0) begin
      unresolved_++;  // malformed frame: shuttle_ref_unpack already spoke
      return;
    end
    if (!shuttle_ref_table #(T)::known(id)) begin
      unresolved_++;
      $error("shuttle: reference mailbox got an unknown object id %0d (tag \"%s\")", id, $typename(T));
      return;
    end
    if (closed_) return;
    if (capacity_ > 0 && q.size() >= capacity_) begin
      if (overflow_ == SHUTTLE_OVERFLOW_DROP_NEWEST) begin
        overflowed_++;
        return;
      end
      void'(q.pop_front());
      void'(qm.pop_front());
      overflowed_++;
    end
    q.push_back(shuttle_ref_table #(T)::of_id(id));
    qm.push_back(meta);
    received_++;
    weave_sv_trigger(ev_);
  endfunction

  function bit try_recv(output T value, output shuttle_meta meta);
    if (q.size() == 0) begin
      value = null;
      return 0;
    end
    value = q.pop_front();
    meta  = qm.pop_front();
    return 1;
  endfunction

  task automatic recv(output bit ok, output T value, output shuttle_meta meta, input time timeout = 0);
    int     status;
    bit     has_deadline;
    time    deadline;
    longint ticks;
    ok           = 0;
    value        = null;
    has_deadline = (timeout > 0);
    if (has_deadline) deadline = $time + timeout;
    forever begin
      if (q.size() > 0) begin
        value = q.pop_front();
        meta  = qm.pop_front();
        ok    = 1;
        return;
      end
      if (closed_) return;
      if (has_deadline) begin
        if ($time >= deadline) return;
        ticks = longint'(deadline - $time);
      end else begin
        ticks = -1;
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

  function longint unresolved();
    return unresolved_;
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

class shuttle_ref_mailbox #(type T);
  local shuttle_subscription          sub_;
  local shuttle_ref_mailbox_sink #(T) sink_;

  function new(string topic, string type_tag = "", int capacity = 0,
               int overflow = SHUTTLE_OVERFLOW_DROP_OLDEST);
    sink_ = new (capacity, overflow);
    sub_  = shuttle_subscribe_nb(topic, sink_, (type_tag == "") ? $typename(T) : type_tag);
  endfunction

  function bit valid();
    return sub_ != null;
  endfunction

  function bit try_recv(output T value, output shuttle_meta meta);
    return sink_.try_recv(value, meta);
  endfunction

  task automatic recv(output bit ok, output T value, output shuttle_meta meta,
                      input time timeout = 0);
    sink_.recv(ok, value, meta, timeout);
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

  function longint unresolved();
    return sink_.unresolved();
  endfunction

  function bit closed();
    return sink_.closed();
  endfunction

  function void close();
    if (sub_ != null) sub_.unsubscribe();
    sink_.close();
  endfunction
endclass

`endif  // SHUTTLE_REF_SVH
