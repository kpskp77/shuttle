// ============================================================================
// shuttle_tests_trigger.sv — deferred/concurrent triggers, timeouts, edges
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_trigger;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

  // ── Deferred trigger: no delivery before the condition holds, and the Publication stays incomplete ───────────────
  task automatic case_trigger();
    automatic Rec                 rec = new();
    automatic shuttle_subscription         sub = shuttle_subscribe("sv.t.trig", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.trig");
    automatic XMsg                m   = new();
    automatic shuttle_publication      h;
    m.addr = 7;
    m.data = 8;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(200_000_000));  // 200ns
    check(rec.blk_count == 0, "trigger.not_before_delay");
    check(h.done() == 0, "trigger.not_done_before_delay");
    h.wait_done();
    check(rec.blk_count == 1, "trigger.delivered_after_delay");
    check(rec.last.addr == 7, "trigger.payload");
  endtask

  // A wait_pub deadline: the timeout leg fires, the frame stays pending and completes later.
  // Exercised through shuttle_c_wait_pub directly because shuttle_publication::wait_done() turns a
  // timeout into $error by design (this file never calls $error).
  task automatic case_wait_timeout();
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.timeout");
    automatic Rec                 rec = new();
    automatic XMsg                m   = new();
    automatic int                 st  = -1;
    automatic shuttle_subscription         sub;
    automatic shuttle_publication      h;

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

  // ── Deferred publish on an external edge: arm a weave_edge and publish from its waiter ──────────
  // There is no event trigger any more, so this *is* the documented recipe: the edge stays on the
  // SV side (it never crosses the DPI boundary) and the frame is published when the edge fires.
  // Note the edge semantics it inherits from weave: a fire() with no waiter is lost, so the waiter
  // has to be armed before the fire.
  task automatic case_event();
    automatic weave_edge          ev  = new("sv.t.ev.edge");
    automatic Rec                 rec = new();
    automatic shuttle_subscription         sub = shuttle_subscribe("sv.t.ev", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.ev");
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
    automatic shuttle_subscription         sub   = shuttle_subscribe("sv.t.ev.trig", early, "XMsg");
    automatic shuttle_publisher #(XMsg) pub   = new("sv.t.ev.trig");
    automatic XMsg                m     = new();
    automatic XMsg                m2    = new();
    automatic shuttle_publication      h;
    automatic shuttle_publication      h2;
    automatic shuttle_subscription         later;
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

endmodule
