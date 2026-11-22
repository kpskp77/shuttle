// ============================================================================
// shuttle_tests_broker.sv — registrations, unsubscribe, retired, bind conflict, topic-name checks
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_broker;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

  // ── The entry is decided by the registration function: registering the same object twice = two subscribers = two callbacks per frame ──────
  // This is C++ subscribe(fn) + subscribe(nb(fn)): two subscribers, two different entries.
  task automatic case_dual_registration();
    automatic Rec                 rec  = new();
    automatic shuttle_subscription         st   = shuttle_subscribe   ("sv.t.dual", rec, "XMsg");  // blocking entry
    automatic shuttle_subscription         snb  = shuttle_subscribe_nb("sv.t.dual", rec, "XMsg");  // nonblocking entry
    automatic shuttle_publisher #(XMsg) pub  = new("sv.t.dual");
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

  // ── Unsubscribe (idempotent): nothing is received after unsubscribing ──────────────────────────────────────
  // case_retired: in-flight delivery after unsubscribing — the SV registry drops it silently, and the C++ side must record it as retired (not delivered)
  task automatic case_retired();
    automatic Rec                 a   = new();
    automatic Rec                 b   = new();
    automatic shuttle_subscription         sa  = shuttle_subscribe("sv.t.retired", a, "XMsg");
    automatic shuttle_subscription         sb  = shuttle_subscribe("sv.t.retired", b, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.retired");
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
    automatic shuttle_subscription         subx  = shuttle_subscribe_nb("sv.t.late", rec, "XMsg");
    automatic shuttle_subscription         suby  = shuttle_subscribe_nb("sv.t.late2", other, "YMsg");
    automatic shuttle_publisher #(XMsg) pubx  = new("sv.t.late");
    automatic shuttle_publisher #(YMsg) puby  = new("sv.t.late");
    automatic shuttle_publisher #(XMsg) pubx2 = new("sv.t.late2");
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
  // $error-ing like serial/concurrent; subscription-pattern syntax is covered by case14 of the C++ shuttle_tests_broker.
  task automatic case_bad_topic();
    automatic shuttle_publisher #(XMsg) bad = new("sv.t.bad.*");
    automatic shuttle_publisher #(XMsg) sib = new("sv.t.bad.ok");
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
    automatic shuttle_subscription         sub = shuttle_subscribe_nb("sv.t.unsub", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) pub = new("sv.t.unsub");
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

endmodule
