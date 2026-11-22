// ============================================================================
// shuttle_tests_publish.sv — publish_now / serial / concurrent, entry routing, retained, snapshot
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_publish;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

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

  // ── `publish_now`: synchronous callback inside the publisher process, never suspends ───────────────────────────
  task automatic case_now_sync();
    automatic Rec                 rec = new();
    // `now` only delivers to nonblocking entries → register via shuttle_subscribe_nb
    automatic shuttle_subscription         sub = shuttle_subscribe_nb("sv.t.now", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.now");
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
    automatic shuttle_subscription         sub = shuttle_subscribe("sv.t.nowblk", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.nowblk");
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
    automatic shuttle_subscription         sub = shuttle_subscribe("sv.t.seq", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.seq");
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
    automatic shuttle_subscription         sa  = shuttle_subscribe("sv.t.par", a, "XMsg");
    automatic shuttle_subscription         sb  = shuttle_subscribe("sv.t.par", b, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.par");
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
    automatic shuttle_subscription         sub = shuttle_subscribe_nb("sv.t.nbbg", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.nbbg");
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

  // ── retained: replay the latest frame when a new subscriber registers ──────────────────────────────
  task automatic case_retained();
    automatic Rec                 first = new();
    automatic shuttle_subscription         s1    = shuttle_subscribe("sv.t.ret", first, "XMsg");
    automatic shuttle_publisher #(XMsg) pub   = new("sv.t.ret");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_subscription         s2;
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
    automatic shuttle_subscription         sub = shuttle_subscribe_nb("sv.t.multi", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pa  = new("sv.t.multi");
    automatic shuttle_publisher #(XMsg) pb  = new("sv.t.multi");
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

  // ── Snapshot semantics of `publish_serial`: subscribers added after the publish call do not join this frame ───────────────────
  task automatic case_snapshot();
    automatic Rec                 first = new();
    automatic shuttle_subscription         s1    = shuttle_subscribe("sv.t.snap", first, "XMsg");
    automatic shuttle_publisher #(XMsg) pub   = new("sv.t.snap");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_subscription         s2;
    m.addr = 32'h401;
    m.data = 0;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(100_000_000));  // starts after 100ns
    late = new();
    s2   = shuttle_subscribe("sv.t.snap", late, "XMsg");  // a new subscriber registered before the trigger fires
    h.wait_done();
    check(first.blk_count == 1, "snapshot.first_received");
    check(late.blk_count == 0, "snapshot.late_sub_not_target");
  endtask

endmodule
