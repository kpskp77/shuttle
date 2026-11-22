// ============================================================================
// shuttle_tests_mailbox.sv — mailbox: queue, deadline, bounded overflow, close
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_mailbox;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

  // ── Mailbox: subscription + queue, pure SV (weave_pkg's event primitives only) ──────────────────
  task automatic case_mailbox();
    automatic shuttle_mailbox     box    = new("sv.t.mbox");
    automatic shuttle_publisher #(XMsg) pub    = new("sv.t.mbox");
    automatic XMsg                m      = new();
    automatic byte                payload[];
    automatic shuttle_meta        meta;
    automatic bit                 ok;
    automatic XMsg                got;
    automatic shuttle_handle      h;
    automatic shuttle_mailbox     cap_box = new("sv.t.mboxcap", "", 2, SHUTTLE_OVERFLOW_DROP_NEWEST);
    automatic shuttle_publisher #(XMsg) pub2  = new("sv.t.mboxcap");

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

endmodule
