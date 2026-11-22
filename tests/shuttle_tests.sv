// ============================================================================
// shuttle_tests.sv — SV-side test suite of shuttle (on a real simulator, VCS)
//
// Compile order: weave_pkg.sv (../weave/sv) → shuttle_pkg.sv (../../sv) → this file.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
//
// Composition:
//   · sv_cases       — all in-SV cases (`now`/`serial`/`concurrent`, triggers, retained,
//                       multiple publishers, unsubscribe, entry routing, seq semantics, snapshot semantics);
//   · case_xlang     — cross-language round trip, paired with tests/xlang_tests.cpp;
//   · case_xlang_mixed — one topic whose target set mixes an SV blocking entry with C++ ones
//                       (task identity must survive the nested frame; weave v1.2.0);
//   · run_cpp_tests  — launches the C++-side tests (WEAVE_TEST in tests/*.cpp) via weave_run;
//                       both sides share one broker, so they see each other.
//
// Verdict: SV case failures accumulate into g_fail; a C++-side failure makes weave_run return a non-zero status.
// "SHUTTLE TESTS PASSED" is printed only when both sides pass (run.sh keys on this marker),
// otherwise $fatal makes simv exit non-zero.
//
// Note: this file never calls $error — negative cases are judged by return values; every assertion,
// pass or fail, is only logged via $display, with a final catch-all $fatal, so run.sh's log matching is not disturbed.
// ============================================================================

module shuttle_tests;
  timeunit 1ns;
  timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipe (case_event)

  int unsigned g_pass = 0;
  int unsigned g_fail = 0;
  bit          g_sv_done = 0;

  // ── Message classes: counterpart of `struct XMsg { uint32_t addr; uint32_t data; }` on the C++ side ──
  // All message classes use the generated codec (sv/shuttle_reflect.svh three-piece macros), which is
  // byte-for-byte the C++ Writer's big-endian wire order (verified on VCS; see the
  // "Simulator constraints" section of docs/backends.md).
  class XMsg;
    int unsigned addr;
    int unsigned data;
    `shuttle_reflect_begin(XMsg)
      `shuttle_field(addr, shuttle_u32)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // Another type: only used by the subscribe-before-bind tag-conflict case (any type_tag different from XMsg)
  class YMsg;
    int unsigned addr;
    `shuttle_reflect_begin(YMsg)
      `shuttle_field(addr, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // ── Cross-language request/response payloads (mirror XRqReq/XRqResp in tests/xlang_tests.cpp) ──
  // One uint32 field each; type_tag must equal the C++ struct name so that type-matched
  // delivery works across languages. The correlation id is NOT part of the payload: it travels in
  // the frame header (shuttle_meta::corr_id), so these classes hold only domain data.
  class XRqReq;
    int unsigned op;
    // The macro generates both pack and unpack; this class only ever decodes C++-side requests.
    `shuttle_reflect_begin(XRqReq)
      `shuttle_field(op, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class XRqResp;
    int unsigned data;
    // Only ever published from SV; the C++ requester decodes it.
    `shuttle_reflect_begin(XRqResp)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // Responder: answers op*2+1 and hands the request's frame header to `pub.reply`, which echoes
  // meta.corr_id for us — the response never mentions the id itself.
  class SvRqSink implements shuttle_sink;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XRqReq                 req  = XRqReq::unpack(payload);
      XRqResp                resp = new();
      shuttle_pub #(XRqResp) pub = new("rq.xlang.resp");
      resp.data = req.op * 2 + 1;
      pub.reply(resp, meta);
    endtask
  endclass

  // ── Implements both entries: shuttle_subscribe registers the blocking entry, shuttle_subscribe_nb the
  //    nonblocking entry (isomorphic to C++ subscribe(fn) / subscribe(nb(fn))) ─────────
  class Rec implements shuttle_sink, shuttle_sink_nb;
    int     blk_count = 0;
    int     nb_count  = 0;
    int     order[$];   // received addr sequence (ordering assertion)
    longint seqs[$];    // received seq sequence (monotonicity assertion)
    XMsg    last;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      blk_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.topic_seq);
      #(1ns);  // consuming simulation time inside a blocking callback is a legal usage
    endtask

    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      nb_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.topic_seq);
    endfunction
  endclass

  // ── Subscriber whose entry is blocking: `publish_now` should error or skip ─────────────────────
  class BlkRec implements shuttle_sink;
    int count = 0;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      count++;
      #(1ns);
    endtask
  endclass

  // ── Subscriber with only a nonblocking entry (invoke_nb): `serial`/`concurrent` must still deliver (via the nonblocking entry) ──
  class NbRec implements shuttle_sink_nb;
    int  count = 0;
    XMsg last;
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      count++;
      last = m;
    endfunction
  endclass

  task automatic check(bit cond, string name);
    if (cond) begin
      g_pass++;
      $display("[PASS] sv::%s", name);
    end else begin
      g_fail++;
      $display("[FAIL] sv::%s", name);
    end
  endtask

  // ── `publish_now`: synchronous callback inside the publisher process, never suspends ───────────────────────────
  task automatic case_now_sync();
    automatic Rec                 rec = new();
    // `now` only delivers to nonblocking entries → register via shuttle_subscribe_nb
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.now", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.now");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h101;
    m.data = 32'h202;
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "now_sync.handle");
    check(h.subscriber_count() == 1, "now_sync.count");
    check(h.done() != 0, "now_sync.done_on_return");  // the nonblocking part is done when it returns
    check(rec.nb_count == 1, "now_sync.nb_hit");
    check(rec.blk_count == 0, "now_sync.blocking_not_hit");
    check(rec.last.addr == 32'h101 && rec.last.data == 32'h202, "now_sync.payload");
  endtask

  // ── `publish_now`: blocking-entry subscribers → reject errors / skip skips (the default is defer) ─────
  task automatic case_now_blocking();
    automatic BlkRec              rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.nowblk", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.nowblk");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 2;
    h = pub.publish_now(m, SHUTTLE_NOW_REJECT);
    check(h == null, "now_blk.reject_returns_null");
    check(rec.count == 0, "now_blk.no_partial_delivery");
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "now_blk.skip_ok");
    check(h.skipped() == 1 && h.deferred() == 0, "now_blk.skip_counted");
    check(h.done() != 0, "now_blk.skip_completes");
    check(rec.count == 0, "now_blk.skip_skips");
  endtask

  // ── `publish_serial`: background single-process, in-order callbacks, per-topic FIFO ──────────────────────────
  task automatic case_publish_serial();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.seq", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.seq");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    for (int i = 1; i <= 3; i++) begin
      m.addr = i;
      m.data = i * 16;
      h = pub.publish_serial(m);
      h.wait_done();
    end
    check(rec.blk_count == 3, "serial.all_arrived");
    // this registration declared only the blocking entry → only invoke runs; invoke_nb is never called
    check(rec.nb_count == 0, "serial.blocking_entry_only");
    check(rec.order.size() == 3 && rec.order[0] == 1 && rec.order[1] == 2 && rec.order[2] == 3,
          "serial.fifo");
    check(rec.seqs.size() == 3 && rec.seqs[0] < rec.seqs[1] && rec.seqs[1] < rec.seqs[2],
          "serial.seq_monotonic");
  endtask

  // ── `publish_concurrent`: one process per subscriber, concurrent callbacks; complete only after all arrive ──────────────────
  task automatic case_publish_concurrent();
    automatic Rec                 a   = new();
    automatic Rec                 b   = new();
    automatic shuttle_sub         sa  = shuttle_subscribe("sv.t.par", a, "XMsg");
    automatic shuttle_sub         sb  = shuttle_subscribe("sv.t.par", b, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.par");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h300;
    m.data = 32'h301;
    h = pub.publish_concurrent(m);
    check(h.done() == 0, "concurrent.immediate_return");  // returned immediately, not yet complete
    h.wait_done();
    check(a.blk_count == 1, "concurrent.sub_a");
    check(b.blk_count == 1, "concurrent.sub_b");
    check(h.done() != 0, "concurrent.done_after_wait");
    check(h.first_error() == "", "concurrent.no_failure");
    check(a.nb_count == 0 && b.nb_count == 0, "concurrent.blocking_entry_only");
  endtask

  // ── Subscribers with a nonblocking entry: `serial`/`concurrent` deliver to them in full as well (entry rule: that registration
  //    has only this one entry, and delivery uses it) ──
  task automatic case_nb_in_background();
    automatic NbRec               rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.nbbg", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.nbbg");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h55;
    m.data = 32'h66;
    h = pub.publish_serial(m);
    h.wait_done();
    check(rec.count == 1, "nb_bg.serial_hits_nonblocking_entry");
    check(h.subscriber_count() == 1, "nb_bg.counted_as_target");
    h = pub.publish_concurrent(m);
    h.wait_done();
    check(rec.count == 2, "nb_bg.concurrent_hits_nonblocking_entry");
  endtask

  // ── The entry is decided by the registration function: registering the same object twice = two subscribers = two callbacks per frame ──────
  // This is C++ subscribe(fn) + subscribe(nb(fn)): two subscribers, two different entries.
  task automatic case_dual_registration();
    automatic Rec                 rec  = new();
    automatic shuttle_sub         st   = shuttle_subscribe   ("sv.t.dual", rec, "XMsg");  // blocking entry
    automatic shuttle_sub         snb  = shuttle_subscribe_nb("sv.t.dual", rec, "XMsg");  // nonblocking entry
    automatic shuttle_pub #(XMsg) pub  = new("sv.t.dual");
    automatic XMsg                m    = new();
    automatic shuttle_handle      h;
    m.addr = 32'h77;
    m.data = 32'h88;

    // `serial`: two subscribers, one call per entry
    h = pub.publish_serial(m);
    h.wait_done();
    check(h.subscriber_count() == 2, "dual_registration.counted_twice");
    check(rec.blk_count == 1, "dual_registration.blocking_entry_once");
    check(rec.nb_count == 1, "dual_registration.nonblocking_entry_once");

    // `now`: only the one with a nonblocking entry is invoked synchronously; the blocking-entry one is handled per policy
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "dual_registration.nonblocking_handle");
    check(h.skipped() == 1, "dual_registration.skip_counted");
    check(rec.nb_count == 2, "dual_registration.nonblocking_sync_second");
    check(rec.blk_count == 1, "dual_registration.blocking_entry_not_synced");
    h = pub.publish_now(m);                                          // default: defer
    check(h != null, "dual_registration.defer_handle");
    check(h.deferred() == 1 && h.done() == 0, "dual_registration.defer_pending");
    h.wait_done();                                                   // wait for the background chain to finish
    check(rec.blk_count == 2, "dual_registration.defer_reaches_blocking_entry");
    check(h.done() != 0, "dual_registration.defer_done_after_wait");
    st.unsubscribe();
    snb.unsubscribe();
  endtask

  // ── Deferred trigger: no delivery before the condition holds, and the Publication stays incomplete ───────────────
  task automatic case_trigger();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.trig", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.trig");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 7;
    m.data = 8;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(200_000_000));  // 200ns
    check(rec.blk_count == 0, "trigger.not_before_delay");
    check(h.done() == 0, "trigger.not_done_before_delay");
    h.wait_done();
    check(rec.blk_count == 1, "trigger.delivered_after_delay");
    check(rec.last.addr == 7, "trigger.payload");
  endtask

  // ── retained: replay the latest frame when a new subscriber registers ──────────────────────────────
  task automatic case_retained();
    automatic Rec                 first = new();
    automatic shuttle_sub         s1    = shuttle_subscribe("sv.t.ret", first, "XMsg");
    automatic shuttle_pub #(XMsg) pub   = new("sv.t.ret");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_sub         s2;
    pub.set_retained(1);
    m.addr = 32'hACE;
    m.data = 32'h1;
    h = pub.publish_serial(m);
    h.wait_done();
    check(first.blk_count == 1, "retained.first_sub");

    late = new();
    s2   = shuttle_subscribe("sv.t.ret", late, "XMsg");
    // the late subscriber's blocking callback is replayed via a background process, so the scheduler must advance: publish one more frame and wait for it
    for (int i = 0; i < 5 && late.blk_count == 0; i++) begin
      h = pub.publish_serial(m);
      h.wait_done();
    end
    check(late.blk_count >= 1, "retained.catchup_to_late_sub");
    check(late.last.addr == 32'hACE, "retained.catchup_payload");

    pub.set_retained(0);
    late = null;
    s2   = null;
  endtask

  // ── Multiple publishers on one topic: both deliver, seq is globally monotonic within the topic ─────────────
  task automatic case_multi_pub();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.multi", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pa  = new("sv.t.multi");
    automatic shuttle_pub #(XMsg) pb  = new("sv.t.multi");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'hA;
    m.data = 0;
    h = pa.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "multi_pub.handle");
    m.addr = 32'hB;
    h = pb.publish_now(m, SHUTTLE_NOW_SKIP);
    check(rec.nb_count == 2, "multi_pub.both_delivered");
    check(rec.order.size() == 2 && rec.order[0] == 32'hA && rec.order[1] == 32'hB,
          "multi_pub.now_order");
    check(rec.seqs.size() == 2 && rec.seqs[0] < rec.seqs[1], "multi_pub.seq_global");
  endtask

  // ── Unsubscribe (idempotent): nothing is received after unsubscribing ──────────────────────────────────────
  // case_retired: in-flight delivery after unsubscribing — the SV registry drops it silently, and the C++ side must record it as retired (not delivered)
  task automatic case_retired();
    automatic Rec                 a   = new();
    automatic Rec                 b   = new();
    automatic shuttle_sub         sa  = shuttle_subscribe("sv.t.retired", a, "XMsg");
    automatic shuttle_sub         sb  = shuttle_subscribe("sv.t.retired", b, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.retired");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 9;
    m.data = 8'h3c;
    h = pub.publish_serial(m);  // the snapshot already includes a and b; the callbacks run later, on the background chain
    sa.unsubscribe();           // unsubscribe before delivery: a's in-flight delivery will be dropped by the registry
    h.wait_done();
    check(b.blk_count == 1, "retired.others_unaffected");
    check(h.finished() == h.subscriber_count(), "retired.counted_as_terminal");
    check(h.retired() == 1, "retired.counted_as_retired");
    check(h.first_error() == "", "retired.not_an_error");
  endtask

  // ── Subscribe before binding: SV registers with only a tag (unlike a C++ concrete-topic subscription, which immediately topic_declares);
  //    the topic's first binding happens at its first publication. The C++ side must then run the reverse check, turning a "wrong tag" into
  //    one explicit error, instead of letting it silently fall out of the target set on every subsequent frame — the same mistake must not
  //    sometimes error and sometimes never error, depending on registration order.
  task automatic case_bind_conflict();
    automatic Rec                 rec   = new();
    automatic Rec                 other = new();
    automatic shuttle_sub         subx  = shuttle_subscribe_nb("sv.t.late", rec, "XMsg");
    automatic shuttle_sub         suby  = shuttle_subscribe_nb("sv.t.late2", other, "YMsg");
    automatic shuttle_pub #(XMsg) pubx  = new("sv.t.late");
    automatic shuttle_pub #(YMsg) puby  = new("sv.t.late");
    automatic shuttle_pub #(XMsg) pubx2 = new("sv.t.late2");
    automatic XMsg                mx    = new();
    automatic XMsg                mx2   = new();
    automatic YMsg                my    = new();
    automatic shuttle_handle      h;
    mx.addr  = 5;
    mx.data  = 6;
    mx2.addr = 7;
    my.addr  = 8;
    // the very first binding is the wrong type: the publish is rejected (returns null), the frame is never published
    h = puby.publish_now(my, SHUTTLE_NOW_SKIP);
    check(h == null, "bind_conflict.rejected");
    check(shuttle_c_last_error() != "", "bind_conflict.has_error_text");
    check(rec.nb_count == 0, "bind_conflict.no_callback");
    // the failed attempt changed no state: the correct type can still bind and deliver
    h = pubx.publish_now(mx, SHUTTLE_NOW_SKIP);
    check(h != null, "bind_conflict.retry_ok");
    check(rec.nb_count == 1 && rec.last.addr == 32'd5, "bind_conflict.retry_delivered");
    // the reverse direction: subscriber declares YMsg, publisher carries XMsg — also rejected at first binding
    h = pubx2.publish_now(mx2, SHUTTLE_NOW_SKIP);
    check(h == null, "bind_conflict.rejected_reverse");
    check(other.nb_count == 0, "bind_conflict.reverse_no_callback");
  endtask

  // ── Topic names must not contain wildcards (* / > belong to subscription patterns only) ────────────────────────────────
  // Only the publish side is tested: `publish_now` fails silently (the reason goes through shuttle_c_last_error) instead of
  // $error-ing like serial/concurrent; subscription-pattern syntax is covered by case14 of the C++ broker_tests.
  task automatic case_bad_topic();
    automatic shuttle_pub #(XMsg) bad = new("sv.t.bad.*");
    automatic shuttle_pub #(XMsg) sib = new("sv.t.bad.ok");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 2;
    // wildcard topic: rejected, no handle, reason recorded (no phantom topic may be created, and no broadcast-like fanout)
    h = bad.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h == null, "badtopic.no_handle");
    check(shuttle_c_last_error() != "", "badtopic.has_error_text");
    // a legal sibling name at the same level is unaffected
    h = sib.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "badtopic.sibling_ok");
  endtask

  task automatic case_unsubscribe();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.unsub", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.unsub");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 1;
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(rec.nb_count == 1, "unsub.before");
    sub.unsubscribe();
    sub.unsubscribe();  // idempotent
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h.subscriber_count() == 0 && h.done() != 0, "unsub.empty_pub_completes");
    check(rec.nb_count == 1, "unsub.after");
  endtask

  // ── Snapshot semantics of `publish_serial`: subscribers added after the publish call do not join this frame ───────────────────
  task automatic case_snapshot();
    automatic Rec                 first = new();
    automatic shuttle_sub         s1    = shuttle_subscribe("sv.t.snap", first, "XMsg");
    automatic shuttle_pub #(XMsg) pub   = new("sv.t.snap");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_sub         s2;
    m.addr = 32'h401;
    m.data = 0;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(100_000_000));  // starts after 100ns
    late = new();
    s2   = shuttle_subscribe("sv.t.snap", late, "XMsg");  // a new subscriber registered before the trigger fires
    h.wait_done();
    check(first.blk_count == 1, "snapshot.first_received");
    check(late.blk_count == 0, "snapshot.late_sub_not_target");
  endtask

  // ── Cross-language request/response: C++ Requester side in requester_tests.cpp case11 ─────────
  // The ready broadcast is retained, so the C++ side may subscribe before or after it is published.
  task automatic case_rq_xlang();
    automatic SvRqSink            sink  = new();
    automatic shuttle_sub         sub   = shuttle_subscribe("rq.xlang.req", sink, "XRqReq");
    automatic shuttle_pub #(XMsg) ready = new("rq.xlang.ready");
    automatic XMsg                note  = new();
    note.addr = 32'h1;
    note.data = 32'h0;
    ready.set_retained(1);
    void'(ready.publish_serial(note));
    check(sub.id() != 0, "rq_xlang.responder_registered");
  endtask

  // ── Cross-language round trip: C++ side in tests/xlang_tests.cpp ────────────────────
  task automatic case_xlang();
    automatic Rec                 rec       = new();
    automatic shuttle_sub         sub       = shuttle_subscribe("xlang.cpp2sv", rec, "XMsg");
    automatic shuttle_pub #(XMsg) ready_pub = new("xlang.sv_ready");
    automatic shuttle_pub #(XMsg) to_cpp    = new("xlang.sv2cpp");
    automatic XMsg                m         = new();
    automatic shuttle_handle      h;
    automatic bit                 ok;

    // 1) ready broadcast (retained): the C++ side receives it whether it subscribes before or after
    ready_pub.set_retained(1);
    m.addr = 32'h1;
    m.data = 32'h0;
    h = ready_pub.publish_serial(m);
    h.wait_done();

    // 2) wait until the C++ side has sent all 3 frames (10us steps, up to 50ms of simulation time)
    for (int i = 0; i < 5000 && rec.blk_count < 3; i++) #(10us);

    check(rec.blk_count == 3, "xlang.cpp2sv.count");
    check(rec.order.size() == 3 && rec.order[0] == 1 && rec.order[1] == 2 && rec.order[2] == 3,
          "xlang.cpp2sv.fifo");
    if (rec.last != null) check(rec.last.data == 32'h300, "xlang.cpp2sv.payload");
    check(rec.seqs.size() == 3 && rec.seqs[0] < rec.seqs[1] && rec.seqs[1] < rec.seqs[2],
          "xlang.cpp2sv.seq_monotonic");

    // 3) send 3 frames back; wait_done returning means the C++-side callbacks have finished
    for (int i = 0; i < 3; i++) begin
      m.addr = 32'h51 + i;
      m.data = 32'hA0 + i;
      h = to_cpp.publish_serial(m);
      h.wait_done();
      ok = (h.first_error() == "");
      check(ok, "xlang.sv2cpp.no_failure");
    end
  endtask

  // A sink that publishes from inside its own callback and then suspends. That nested publish
  // forces the simulator to run a C++ frame in the middle of the outer delivery; with weave <= 1.1.0
  // the outer frame came back with no current task, so the next C++ subscriber that blocked threw
  // "weave: blocking primitive outside task context" (../weave/bugs.md; fixed by keying the task
  // identity per simulation process in weave v1.2.0).
  class MixedSink implements shuttle_sink;
    int                 count = 0;
    XMsg                last;
    shuttle_pub #(XMsg) nested;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      count++;
      last = m;
      if (nested != null) void'(nested.publish_serial(m));
      #(1ns);
    endtask
  endclass

  // Cross-language mixed target set (C++ side: case6 in tests/xlang_tests.cpp). The SV sink
  // registers before the C++ subscriber does, so the frame's target list is
  // [SV blocking entry, C++ blocking subscriber] - the ordering that used to break the identity.
  task automatic case_xlang_mixed();
    automatic MixedSink           sink  = new();
    automatic shuttle_pub #(XMsg) ready = new("xlang.mixed.ready");
    automatic XMsg                note  = new();
    automatic shuttle_handle      h;
    automatic shuttle_sub         sub;

    sink.nested = new("xlang.mixed.mon");
    sub         = shuttle_subscribe("xlang.mixed.tx", sink, "XMsg");
    check(sub.id() != 0, "xlang.mixed.sink_registered");

    // ready broadcast (retained): the C++ side subscribes after this, so it becomes the second and
    // third target of the frame it publishes below
    ready.set_retained(1);
    note.addr = 32'h1;
    note.data = 32'h0;
    h = ready.publish_serial(note);
    h.wait_done();
    check(h.done() && h.first_error() == "", "xlang.mixed.ready_no_failure");

    // the frame itself comes from the C++ side (0xA1/0xB2); bounded so a broken handshake fails loudly
    for (int i = 0; i < 20000 && sink.count == 0; i++) #(10us);
    check(sink.count == 1, "xlang.mixed.sink_delivered");
    if (sink.last != null) begin
      check(sink.last.addr == 32'hA1, "xlang.mixed.sink_payload_addr");
      check(sink.last.data == 32'hB2, "xlang.mixed.sink_payload_data");
    end
  endtask

  // A wait_pub deadline: the timeout leg fires, the frame stays pending and completes later.
  // Exercised through shuttle_c_wait_pub directly because shuttle_handle::wait_done() turns a
  // timeout into $error by design (this file never calls $error).
  task automatic case_wait_timeout();
    automatic shuttle_pub #(XMsg) pub = new("sv.t.timeout");
    automatic Rec                 rec = new();
    automatic XMsg                m   = new();
    automatic int                 st  = -1;
    automatic shuttle_sub         sub;
    automatic shuttle_handle      h;

    sub = shuttle_subscribe("sv.t.timeout", rec, "XMsg");
    check(sub.id() != 0, "wait.timeout_subscribed");
    m.addr = 32'hA1;
    m.data = 32'hB2;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(10_000_000));  // the frame is due at 10 ns
    shuttle_c_wait_pub(h.id(), 1_000_000, st);                            // give up after 1 ns
    check(st == SHUTTLE_WAIT_TIMEOUT, "wait.timeout_fires");
    check(!h.done(), "wait.timeout_leaves_the_frame_pending");
    shuttle_c_wait_pub(h.id(), 0, st);                                    // no deadline: wait it out
    check(st == SHUTTLE_WAIT_OK && h.done(), "wait.completes_after_the_deadline");
    check(rec.blk_count == 1, "wait.deadline_delivered_once");
  endtask

  // ── Subscriber that publishes on its own topic and waits: diagnosed, not hung ──────────────────
  // The frame it publishes is queued behind the very delivery that is asking for it, so it can
  // never start. The C++ wait entry detects this (detail::check_self_blocked) and answers
  // SHUTTLE_WAIT_FAILED with shuttle_c_last_error() set. `wait_done()` is avoided here because this
  // file must not emit $error.
  class SelfWaitSink implements shuttle_sink;
    int             blk_count   = 0;
    int             wait_status = -1;
    string          wait_error  = "";
    shuttle_handle  inner;
    shuttle_pub #(XMsg) pub;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      XMsg echo = new();
      blk_count++;
      if (m.addr != 32'hD1) return;
      echo.addr = 32'hD2;
      echo.data = 32'h0;
      inner = pub.publish_serial(echo);          // same topic: queued behind this delivery
      shuttle_c_wait_pub(inner.id(), 0, wait_status);
      wait_error = shuttle_c_last_error();
    endtask
  endclass

  // ── Deferred publish on an external edge: arm a weave_edge and publish from its waiter ──────────
  // There is no event trigger any more, so this *is* the documented recipe: the edge stays on the
  // SV side (it never crosses the DPI boundary) and the frame is published when the edge fires.
  // Note the edge semantics it inherits from weave: a fire() with no waiter is lost, so the waiter
  // has to be armed before the fire.
  task automatic case_event();
    automatic weave_edge          ev  = new("sv.t.ev.edge");
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.ev", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.ev");
    automatic XMsg                m   = new();
    m.addr = 32'hE0;
    m.data = 32'h0;
    fork
      begin
        ev.wait_edge();
        pub.publish_serial(m);
      end
    join_none
    #(10ns);
    check(rec.blk_count == 0, "event.not_before_fire");
    ev.fire();
    for (int i = 0; i < 1000 && rec.blk_count == 0; i++) #(1ns);
    check(rec.blk_count == 1, "event.delivered_after_fire");
    check(rec.last.addr == 32'hE0, "event.payload");
    ev.fire();  // nobody is armed now: a fire with no waiter is lost, it does not latch
    #(100ns);
    check(rec.blk_count == 1, "event.fire_without_waiter_lost");
    sub.unsubscribe();
  endtask

  // after_edge: the same weave_edge the C++ side reaches through weave::sv::Edge, but with *trigger*
  // semantics — the snapshot (subscribers + payload) is taken at the publish call, not at fire time,
  // which is exactly what the fork recipe above cannot give. Only the edge *name* crosses the DPI
  // boundary (C++ finds it in weave's own table), and an edge is not a latch: a fire that lands
  // before the delivery process registered its wait is lost.
  task automatic case_edge_trigger();
    automatic weave_edge          ev    = new("sv.t.ev.trig");
    automatic Rec                 early = new();
    automatic Rec                 late  = new();
    automatic shuttle_sub         sub   = shuttle_subscribe("sv.t.ev.trig", early, "XMsg");
    automatic shuttle_pub #(XMsg) pub   = new("sv.t.ev.trig");
    automatic XMsg                m     = new();
    automatic XMsg                m2    = new();
    automatic shuttle_handle      h;
    automatic shuttle_handle      h2;
    automatic shuttle_sub         later;
    m.addr  = 32'hA1;
    m2.addr = 32'hA2;
    h = pub.publish_serial(m, shuttle_trigger::after_edge(ev));
    // subscribed *after* the publish: the snapshot was already taken, so this is not a target
    later = shuttle_subscribe("sv.t.ev.trig", late, "XMsg");
    #(10ns);
    check(early.blk_count == 0, "edge_trigger.not_before_fire");
    check(!h.done(), "edge_trigger.pending_before_fire");
    ev.fire();
    h.wait_done();
    check(early.blk_count == 1, "edge_trigger.delivered_after_fire");
    check(early.last.addr == 32'hA1, "edge_trigger.payload");
    check(late.blk_count == 0, "edge_trigger.snapshot_at_publish");
    check(h.finished() == 1, "edge_trigger.target_count");
    check(h.first_error() == "", "edge_trigger.no_failure");

    // no suspension between the publish and the fire, so the delivery process has not registered
    // its wait yet: the edge is lost (unlike after_pub / a sticky event)
    h2 = pub.publish_serial(m2, shuttle_trigger::after_edge(ev));
    ev.fire();
    #(10ns);
    check(!h2.done(), "edge_trigger.lost_edge_does_not_latch");
    ev.fire();
    h2.wait_done();
    check(early.blk_count == 2, "edge_trigger.second_fire_delivers");
    check(early.last.addr == 32'hA2, "edge_trigger.second_payload");
    later.unsubscribe();
    sub.unsubscribe();
  endtask

  // ── self-wait: a callback that publishes on its own topic and waits is diagnosed, not hung ──────
  task automatic case_self_wait();
    automatic SelfWaitSink        sink = new();
    automatic shuttle_pub #(XMsg) pub  = new("sv.t.selfwait");
    automatic XMsg                m    = new();
    automatic shuttle_handle      h;
    automatic shuttle_sub         sub  = shuttle_subscribe("sv.t.selfwait", sink, "XMsg");
    sink.pub = pub;
    m.addr = 32'hD1;
    m.data = 32'h0;
    h = pub.publish_serial(m);
    h.wait_done();                             // the callback itself never suspends: it got a status
    for (int i = 0; i < 1000 && sink.blk_count < 2; i++) #(1us);
    check(sink.wait_status == SHUTTLE_WAIT_FAILED, "selfwait.diagnosed");
    // The diagnosis carries its own text through shuttle_c_last_error. Compared by indexing instead
    // of substr(): VCS 2018.09 reads substr(a, b) as "chars a..b inclusive", not as (start, length).
    begin
      automatic string tail = "";
      for (int i = sink.wait_error.len() - 22; i < sink.wait_error.len(); i++)
        tail = {tail, sink.wait_error[i]};
      check(tail == "waited for the result)", "selfwait.message");
    end
    check(sink.blk_count == 2, "selfwait.inner_frame_delivered");
    sub.unsubscribe();
  endtask

  // ── Mailbox: subscription + queue, pure SV (weave_pkg's event primitives only) ──────────────────
  task automatic case_mailbox();
    automatic shuttle_mailbox     box    = new("sv.t.mbox");
    automatic shuttle_pub #(XMsg) pub    = new("sv.t.mbox");
    automatic XMsg                m      = new();
    automatic byte                payload[];
    automatic shuttle_meta        meta;
    automatic bit                 ok;
    automatic XMsg                got;
    automatic shuttle_handle      h;
    automatic shuttle_mailbox     cap_box = new("sv.t.mboxcap", "", 2, SHUTTLE_OVERFLOW_DROP_NEWEST);
    automatic shuttle_pub #(XMsg) pub2  = new("sv.t.mboxcap");

    // nonblocking fill: publish_now delivers to the nonblocking entry synchronously
    m.addr = 32'h701;
    m.data = 32'h702;
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h.subscriber_count() == 1, "mbox.is_target");  // a mailbox is an ordinary subscriber
    check(box.size() == 1, "mbox.now_fills_synchronously");

    check(box.try_recv(payload, meta) != 0, "mbox.try_recv_ok");
    got = XMsg::unpack(payload);
    check(got.addr == 32'h701 && got.data == 32'h702, "mbox.payload");
    check(meta.topic_name == "sv.t.mbox", "mbox.meta_topic");
    check(box.try_recv(payload, meta) == 0, "mbox.try_recv_empty");
    check(box.received() == 1, "mbox.received_count");

    // blocking receive with a timeout: nothing published → ok == 0, and no frame is lost
    box.recv(ok, payload, meta, 10_000_000);  // 10ns
    check(ok == 0, "mbox.timeout");
    h = pub.publish_serial(m);
    h.wait_done();
    box.recv(ok, payload, meta, 1_000_000);  // 1us
    check(ok == 1, "mbox.serial_fills");

    // bounded mailbox: drop_newest keeps the queue at capacity and counts the losses
    for (int i = 1; i <= 4; ++i) begin
      m.addr = i;
      h = pub2.publish_now(m, SHUTTLE_NOW_SKIP);
    end
    check(cap_box.size() == 2, "mbox.capacity_bound");
    check(cap_box.received() == 2, "mbox.capacity_received");
    check(cap_box.overflowed() == 2, "mbox.capacity_overflowed");
    cap_box.close();

    // close: the queued frame is still readable, then recv reports ok == 0 without hanging
    box.close();
    check(box.closed() != 0, "mbox.closed_flag");
    box.recv(ok, payload, meta, 0);
    check(ok == 0, "mbox.recv_after_close");
    check(box.try_recv(payload, meta) == 0, "mbox.try_recv_after_close");
  endtask

  // ── SV-side case driver: local cases first, then the cross-language round trip ──────────────────────
  initial begin : sv_cases
    // The request/response responder is a service: register it (and broadcast readiness) before
    // anything else, otherwise the C++ Requester test would wait for it while this block waits for
    // the C++ xlang tests — a circular wait resolved only by a timeout.
    case_rq_xlang();
    case_now_sync();
    case_now_blocking();
    case_publish_serial();
    case_publish_concurrent();
    case_wait_timeout();
    case_nb_in_background();
    case_dual_registration();
    case_trigger();
    case_retained();
    case_multi_pub();
    case_unsubscribe();
    case_retired();
    case_bind_conflict();
    case_bad_topic();
    case_snapshot();
    case_event();
    case_edge_trigger();
    case_self_wait();
    case_mailbox();
    case_xlang();
    case_xlang_mixed();
    g_sv_done = 1;
    $display("[sv] %0d passed, %0d failed", g_pass, g_fail);
  end

  // ── Launch and final verdict of the C++-side tests ────────────────────────────────────────
  initial begin : run_cpp_tests
    automatic int status;
    weave_pkg::weave_run(status);
    if (status != 0) $fatal(1, "SHUTTLE CPP TESTS FAILED (status=%0d)", status);
    wait (g_sv_done);
    if (g_fail != 0) $fatal(1, "SHUTTLE SV TESTS FAILED (%0d failures)", g_fail);
    $display("SHUTTLE TESTS PASSED (%0d sv cases, cpp ok)", g_pass);
    $finish;
  end

endmodule
