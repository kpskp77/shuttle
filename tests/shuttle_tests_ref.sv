// ============================================================================
// shuttle_tests_ref.sv — objects crossing by reference through the broker
//
// The cases only touch the public surface: shuttle_ref_publisher (trio + retained + free),
// shuttle_ref_mailbox and shuttle_ref_subscription, plus ride-along in a reflected message
// (shuttle_ref_codec through the ordinary shuttle_requester / shuttle_serve). No id ever appears
// on this side — the table that maps it is internal. The object class FObj carries a queue and a
// nested handle, i.e. the things the wire fundamentally cannot hold, so resolving to the same
// handle is the only way a receiver can see them at all.
//
// case_ref_xlang pairs with tests/shuttle_tests_ref.cpp, whose FObj struct mirrors the 4-byte
// reference and routes it back untouched without resolving it.
// ============================================================================

module shuttle_tests_ref;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import shuttle_tests_runner::*;

  // The object that crosses by reference: a plain class, no reflection, free to hold anything.
  class FObj;
    int          v;
    string       s;
    FObj         inner;
    int unsigned hist[$];
    function new(int v_ = 0, string s_ = "");
      v = v_;
      s = s_;
    endfunction
  endclass

  // Callback form: on_msg receives the resolved handle, so a later mutation is visible (the
  // frame carried a reference, not a copy).
  class FWatch implements shuttle_ref_sink #(FObj);
    int          n;
    FObj         last;
    function new();
      n    = 0;
      last = null;
    endfunction
    virtual function void on_msg(input FObj value, input shuttle_meta meta);
      n++;
      last = value;
    endfunction
  endclass

  task automatic case_ref_basic();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.basic");
    automatic shuttle_ref_mailbox #(FObj) box = new ("ref.basic");
    automatic shuttle_meta meta;
    automatic FObj obj = null;
    automatic FObj got = null;
    automatic shuttle_publication h = null;
    obj = new (7, "seven");
    h   = pub.publish_serial(obj);
    check(h != null, "ref.basic_published");
    if (h != null) h.wait_done();
    check(box.size() == 1, "ref.basic_frame");
    check(box.try_recv(got, meta), "ref.basic_recv");
    check(got == obj, "ref.same_handle");
    check(got.v == 7 && got.s == "seven", "ref.fields");
    pub.free(obj);
  endtask

  task automatic case_ref_mutation();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.mutate");
    automatic FWatch watch = new ();
    automatic shuttle_ref_subscription #(FObj) sub = null;
    automatic FObj obj = null;
    automatic shuttle_publication h = null;
    sub = shuttle_ref_subscription #(FObj)::blocking("ref.mutate", watch);
    check(sub.valid(), "ref.mutate_subscribed");
    obj = new (1, "");
    h   = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    check(watch.n == 1 && watch.last == obj, "ref.mutate_delivered");
    // Same handle on the receiver side: what the sender does after the publish is visible.
    obj.hist.push_back(42);
    obj.v = 2;
    check(watch.last.hist.size() == 1 && watch.last.hist[0] == 42, "ref.shared_queue");
    check(watch.last.v == 2, "ref.shared_field");
    sub.unsubscribe();
    pub.free(obj);
  endtask

  task automatic case_ref_modes();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.modes");
    automatic shuttle_ref_mailbox #(FObj) box = new ("ref.modes");
    automatic shuttle_meta meta;
    automatic FObj obj = null;
    automatic FObj got = null;
    automatic shuttle_publication h = null;
    obj = new (3, "");
    // publish_now reaches the mailbox's nonblocking entry inline; publish_concurrent goes
    // through the background chain. Both carry the same reference.
    check(pub.publish_now(obj) != null, "ref.modes_now");
    h = pub.publish_concurrent(obj);
    if (h != null) h.wait_done();
    check(box.size() == 2, "ref.modes_frames");
    check(box.try_recv(got, meta) && got == obj, "ref.modes_same_handle");
    check(box.try_recv(got, meta) && got == obj, "ref.modes_same_handle_2");
    pub.free(obj);
  endtask

  task automatic case_ref_callbacks();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.callbacks");
    automatic FWatch blocking_watch = new ();
    automatic FWatch nb_watch = new ();
    automatic shuttle_ref_subscription #(FObj) blocking_sub = null;
    automatic shuttle_ref_subscription #(FObj) nb_sub = null;
    automatic FObj obj = null;
    automatic shuttle_publication h = null;
    blocking_sub = shuttle_ref_subscription #(FObj)::blocking("ref.callbacks", blocking_watch);
    nb_sub = shuttle_ref_subscription #(FObj)::nonblocking("ref.callbacks", nb_watch);
    check(blocking_sub.valid() && nb_sub.valid(), "ref.callbacks_subscribed");
    obj = new (5, "five");
    // publish_now reaches nonblocking entries only; SKIP (not the default DEFER, which would
    // still land the frame on the blocking subscriber through the background chain).
    void'(pub.publish_now(obj, SHUTTLE_NOW_SKIP));
    check(nb_watch.n == 1, "ref.callbacks_nb_inline");
    check(blocking_watch.n == 0, "ref.callbacks_blocking_skipped");
    // serial reaches both: nonblocking inline, blocking on the delivery task.
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    check(nb_watch.n == 2 && blocking_watch.n == 1, "ref.callbacks_serial_both");
    check(blocking_watch.last == obj && nb_watch.last == obj, "ref.callbacks_handles");
    // Unsubscribing stops delivery; the other registration is untouched.
    blocking_sub.unsubscribe();
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    check(blocking_watch.n == 1 && nb_watch.n == 3, "ref.callbacks_unsubscribed");
    nb_sub.unsubscribe();
    pub.free(obj);
  endtask

  // One receiver, two reference types. SV has no overloading, so FMulti cannot implement
  // shuttle_ref_sink #(FObj) and shuttle_ref_sink #(SObj) at once; it implements the untyped
  // nonblocking entry once and resolves each arm through shuttle_ref_msg #(T)::unpack — the
  // multi-type shape the ref header documents.
  class SObj;
    int v;
    function new(int v_ = 0);
      v = v_;
    endfunction
  endclass

  class FMulti implements shuttle_sink_nb;
    int  f_n;
    int  s_n;
    FObj last_f;
    SObj last_s;
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      if (meta.type_tag == "FObj") begin
        last_f = shuttle_ref_msg #(FObj)::unpack(payload);
        f_n++;
      end else if (meta.type_tag == "SObj") begin
        last_s = shuttle_ref_msg #(SObj)::unpack(payload);
        s_n++;
      end
    endfunction
  endclass

  task automatic case_ref_multi();
    automatic FMulti                     multi = new ();
    automatic shuttle_ref_publisher #(FObj) fpub = new ("ref.multi.f", "FObj");
    automatic shuttle_ref_publisher #(SObj) spub = new ("ref.multi.s", "SObj");
    automatic FObj                       fobj = new (13, "thirteen");
    automatic SObj                       sobj = new (14);
    automatic shuttle_subscription        fsub = null;
    automatic shuttle_subscription        ssub = null;
    automatic shuttle_publication         h = null;
    // The same sink object, two registrations: whichever tag the frame carries picks the arm.
    fsub = shuttle_subscribe_nb("ref.multi.f", multi, "FObj");
    ssub = shuttle_subscribe_nb("ref.multi.s", multi, "SObj");
    check(fsub != null && ssub != null, "ref.multi_subscribed");
    h = fpub.publish_serial(fobj);
    if (h != null) h.wait_done();
    h = spub.publish_serial(sobj);
    if (h != null) h.wait_done();
    check(multi.f_n == 1 && multi.last_f == fobj, "ref.multi_first_type");
    check(multi.s_n == 1 && multi.last_s == sobj, "ref.multi_second_type");
    // Nothing was dropped into the other arm.
    check(multi.f_n + multi.s_n == 2, "ref.multi_no_extra_delivery");
    fsub.unsubscribe();
    ssub.unsubscribe();
    fpub.free(fobj);
    spub.free(sobj);
  endtask

  task automatic case_ref_retained();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.retained");
    automatic shuttle_ref_mailbox #(FObj) box = null;
    automatic shuttle_meta meta;
    automatic FObj obj = null;
    automatic FObj got = null;
    automatic shuttle_publication h = null;
    obj = new (9, "nine");
    obj.hist.push_back(1);
    pub.set_retained(1);
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    // A subscriber that arrives after the publish still resolves: the retained frame carries
    // the reference, and the table lookup happens at replay time.
    box = new ("ref.retained");
    check(box.size() == 1, "ref.retained_replayed");
    check(box.try_recv(got, meta), "ref.retained_recv");
    check(got == obj && got.hist.size() == 1, "ref.retained_same_handle");
    pub.free(obj);
  endtask

  task automatic case_ref_lifetime();
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.lifetime");
    automatic shuttle_ref_mailbox #(FObj) box = new ("ref.lifetime");
    automatic shuttle_meta meta;
    automatic FObj obj = null;
    automatic FObj other = null;
    automatic FObj got = null;
    automatic shuttle_publication h = null;
    obj = new (1, "one");
    other = new (2, "two");
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    // free drops the reference binding; publishing the same object again binds a fresh one
    // (ids are never reused, so a stale in-flight frame can never alias another object).
    pub.free(obj);
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    h = pub.publish_serial(other);
    if (h != null) h.wait_done();
    check(box.size() == 3, "ref.lifetime_frames");
    check(box.try_recv(got, meta) && got == obj, "ref.lifetime_first");
    check(box.try_recv(got, meta) && got == obj, "ref.lifetime_after_free");
    check(box.try_recv(got, meta) && got == other, "ref.lifetime_other");
    check(box.unresolved() == 0, "ref.lifetime_no_unresolved");
    // clear() drops every binding this publisher handed out; the next publish rebinds.
    pub.clear();
    h = pub.publish_serial(other);
    if (h != null) h.wait_done();
    check(box.try_recv(got, meta) && got == other, "ref.lifetime_after_clear");
    pub.clear();
  endtask

  // Reference inside a value message: request/response uses the *existing* shuttle_requester and
  // shuttle_serve — a reflected message may carry an object next to its data, so no reference
  // flavour of either class is needed. `shuttle_field` takes a codec name, hence the typedef.
  typedef shuttle_ref_codec #(FObj) FObj_link;

  class AskObj;
    FObj         obj;
    logic [31:0] mode;
    `shuttle_reflect_begin(AskObj)
      `shuttle_field(obj, FObj_link)
      `shuttle_field(mode, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class RspVal;
    logic [31:0] mode;
    int          v;
    `shuttle_reflect_begin(RspVal)
      `shuttle_field(mode, shuttle_u32)
      `shuttle_field(v, shuttle_i32)
    `shuttle_reflect_end
  endclass

  class FObjResponder implements shuttle_responder #(AskObj, RspVal);
    virtual function RspVal respond(input AskObj req, input shuttle_meta meta);
      RspVal r = new ();
      // Reading the object here is possible only because the frame carried a reference.
      r.mode = req.mode;
      r.v    = (req.obj != null) ? req.obj.v : -1;
      return r;
    endfunction
  endclass

  task automatic case_ref_requester();
    automatic FObjResponder                       hdl = new ();
    automatic shuttle_serve #(AskObj, RspVal)     svc = shuttle_serve #(AskObj, RspVal)::nonblocking(
        "ref.rq.req", "ref.rq.resp", hdl);
    automatic shuttle_requester #(AskObj, RspVal) rq = new ("ref.rq.req", "ref.rq.resp");
    automatic FObj                                obj = new (41, "forty-one");
    automatic AskObj                              req = new ();
    automatic shuttle_call #(RspVal)              call;
    automatic RspVal                              rsp;
    automatic bit                                 ok = 0;
    check(rq.valid() && svc.valid(), "ref.rq.valid");
    req.obj  = obj;
    req.mode = 32'd7;
    call     = rq.send_now(req);
    check(call != null, "ref.rq.call");
    if (call != null) call.recv(ok, rsp, 5000 * 10us);
    check(ok && rsp.mode == 32'd7, "ref.rq_reply");
    check(rsp.v == 41, "ref.rq_object_resolved");
    check(rq.stale_responses() == 0, "ref.rq_no_stale");
  endtask

  // Cross-language: the C++ peer sees a plain u32 and routes it back untouched; only the SV side
  // resolves it back to the handle.
  task automatic case_ref_xlang();
    // Cross-language: the tag must be the C++ mirror's spelling, not the default
    // $typename(FObj) == "shuttle_tests_ref.FObj".
    automatic shuttle_ref_publisher #(FObj) pub = new ("ref.echo.req", "FObj");
    automatic shuttle_ref_mailbox #(FObj) box = new ("ref.echo.resp", "FObj");
    automatic shuttle_meta meta;
    automatic FObj obj = null;
    automatic FObj got = null;
    automatic shuttle_publication h = null;
    automatic integer i = 0;
    xlang_barrier("case11");
    obj = new (11, "eleven");
    h   = pub.publish_serial(obj);
    if (h != null) h.wait_done();
    while (box.size() == 0 && i < 100_000) begin
      #(10us);
      i++;
    end
    check(box.size() == 1, "ref.xlang_echo_frame");
    check(box.try_recv(got, meta), "ref.xlang_recv");
    check(got == obj, "ref.xlang_same_handle");
    check(got.v == 11 && got.s == "eleven", "ref.xlang_fields");
    pub.free(obj);
    g_xlang_done++;
  endtask

  task automatic run_cases();
    case_ref_basic();
    case_ref_mutation();
    case_ref_modes();
    case_ref_callbacks();
    case_ref_multi();
    case_ref_retained();
    case_ref_lifetime();
    case_ref_requester();
  endtask

endmodule
