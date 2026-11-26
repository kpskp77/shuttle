// ============================================================================
// shuttle_tests_requester.sv — SV requester against SV and C++ responders, and the responder service the C++
// requester test (tests/shuttle_tests_requester.cpp case11) calls
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_requester;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

  // ── Responder logic behind shuttle_serve: the service owns the response publisher and the
  //    corr_id echo (through pub.reply), so the responder only computes the answer ──────────
  class SvRqPlusOne implements shuttle_responder #(XRqReq, XRqResp);
    virtual function XRqResp respond(input XRqReq req, input shuttle_meta meta);
      XRqResp resp = new();
      resp.data = req.op * 2 + 1;
      return resp;
    endfunction
  endclass

  // ── Same doubling used by the SV-local cases ──
  class SvRqDouble implements shuttle_responder #(XRqReq, XRqResp);
    virtual function XRqResp respond(input XRqReq req, input shuttle_meta meta);
      XRqResp resp = new();
      resp.data = req.op * 2;
      return resp;
    endfunction
  endclass

  // ── Cross-language request/response: the responder service the C++ requester case (case5 in
  //    tests/shuttle_tests_xlang.cpp) calls ─────────
  // Subscribe, announce readiness (retained — the C++ side may subscribe before or after it is
  // published), then rendezvous with the C++ case through xlang_barrier. The task runs in the
  // forked cross-language group (see shuttle_tests.sv), so its position in the SV case list
  // carries no timing meaning.
  task automatic case_rq_xlang();
    automatic SvRqPlusOne                    hdl = new();
    automatic shuttle_serve #(XRqReq, XRqResp) svc = shuttle_serve #(XRqReq, XRqResp)::blocking(
        "rq.xlang.req", "rq.xlang.resp", hdl);
    automatic shuttle_publisher #(XMsg) ready = new("rq.xlang.ready");
    automatic XMsg                note  = new();
    note.addr = 32'h1;
    note.data = 32'h0;
    ready.set_retained(1);
    void'(ready.publish_serial(note));
    check(svc.valid(), "rq_xlang.responder_registered");
    xlang_barrier("case5");
    g_xlang_done++;
  endtask

  // ── SV-side request/response with both ends in SV: the requester allocates its corr_id through
  //    shuttle_c_take_corr_id (the counter C++ shares), an nb responder answers inside the publish
  //    call, and the reply is routed back by that id ─────────────────────────────────
  task automatic case_sv_requester();
    automatic SvRqDouble                           hdl = new();
    automatic shuttle_serve #(XRqReq, XRqResp)     svc = shuttle_serve #(XRqReq, XRqResp)::nonblocking(
        "sv.rq.req", "sv.rq.resp", hdl);
    automatic shuttle_requester #(XRqReq, XRqResp) rq   = new ("sv.rq.req", "sv.rq.resp");
    automatic XRqReq                               req  = new();
    automatic XRqResp                              resp;
    automatic XRqResp                              all[$];
    automatic shuttle_call #(XRqResp)              call;
    automatic shuttle_requester #(XRqReq, XRqResp) same;
    automatic shuttle_requester #(XRqReq, XRqResp) nameless;
    automatic shuttle_serve #(XRqReq, XRqResp)     same_svc;
    automatic bit                                  ok;
    automatic time                                 t0;

    check(rq.valid(), "svrq.valid");
    check(svc.valid() && svc.subscription().id() != 0, "svrq.responder_registered");
    check(rq.req_topic() == "sv.rq.req" && rq.resp_topic() == "sv.rq.resp", "svrq.topics");
    req.op = 21;

    // send_now + an nb responder: the round trip happens on this stack, no simulation time spent
    t0   = $time;
    call = rq.send_now(req);
    check(call != null, "svrq.send_now_call");
    if (call != null) begin
      check(call.corr_id() != 0, "svrq.corr_id_allocated");
      check(call.queued() == 1, "svrq.inline_reply");
      check($time == t0, "svrq.no_sim_time");
      check(call.publication().subscriber_count() == 1 && call.publication().done() != 0,
            "svrq.request_done");
      call.recv(ok, resp, 1_000_000);
      check(ok == 1, "svrq.recv_ok");
      if (resp != null) check(resp.data == 42, "svrq.reply_value");
      check(call.queued() == 0, "svrq.reply_taken");
    end

    // serial flavor: the request travels the background chain, so the reply arrives later and the
    // requester suspends on the call's own event
    call = rq.send_serial(req);
    check(call != null, "svrq.send_serial_call");
    if (call != null) begin
      check(call.queued() == 0, "svrq.serial_not_inline");
      call.wait_all(all, 1_000_000);
      check(all.size() == 1, "svrq.serial_reply_count");
      if (all.size() == 1) check(all[0].data == 42, "svrq.serial_reply_value");
    end

    // a cancelled call stops accepting replies: the answer that still arrives is counted as stale,
    // and a pending recv returns ok == 0 instead of waiting for its deadline
    call = rq.send_serial(req);
    check(call != null, "svrq.cancel_call");
    if (call != null) begin
      call.cancel();
      call.publication().wait_done();  // every target has been served by now
      check(call.cancelled() != 0, "svrq.cancelled");
      check(rq.stale_responses() == 1, "svrq.stale_counted");
      check(call.queued() == 0, "svrq.cancelled_queue_dropped");
      call.recv(ok, resp, 1_000_000);
      check(ok == 0, "svrq.cancelled_recv_not_ok");
    end

    // the next send prunes the cancelled slot: the table stays bounded by the calls still in flight
    call = rq.send_now(req);
    check(call != null && rq.inflight() == 3, "svrq.pruned_table");

    // both failure modes are reported through valid()/last_error(), never by $error (which would
    // fail the run) and never silently
    same     = new ("sv.rq.same", "sv.rq.same");
    nameless = new ("", "sv.rq.nameless.resp");
    check(!same.valid() && same.last_error() != "", "svrq.same_topic_invalid");
    check(!nameless.valid() && nameless.last_error() != "", "svrq.empty_topic_invalid");

    // serve validates its topics before registering and reports the same way
    same_svc = shuttle_serve #(XRqReq, XRqResp)::nonblocking("sv.rq.same", "sv.rq.same", hdl);
    check(!same_svc.valid() && same_svc.last_error() != "", "svrq.serve_same_topic_invalid");
  endtask

  // ── Cross-language request/response, SV requester side: the C++ responder is case9 of
  //    tests/shuttle_tests_xlang.cpp (serve() with an nb handler plus a retained ready frame) ─────────────
  task automatic case_rq_sv_requester();
    automatic shuttle_mailbox                      ready = new ("xlang.rqsv.ready", "XMsg");
    automatic shuttle_requester #(XRqReq, XRqResp) rq = new ("xlang.rqsv.req", "xlang.rqsv.resp");
    automatic XRqReq                  req = new();
    automatic XRqResp                 all[$];
    automatic shuttle_call #(XRqResp) call;

    check(rq.valid(), "rqsv.valid");

    // Rendezvous with C++ case10: its serve() responder is registered before the C++ barrier,
    // so once both sides pass, the request below always has its target.
    xlang_barrier("case10");

    // The C++ responder also announces itself with a retained frame; the wait cannot span other
    // cases anymore, but stays bounded so a broken handshake fails loudly instead of hanging.
    for (int i = 0; i < 100_000 && ready.size() == 0; i++) #(10us);  // up to 1s
    check(ready.size() == 1, "rqsv.ready_seen");

    req.op = 21;
    call   = rq.send_serial(req);
    check(call != null, "rqsv.send_call");
    if (call != null) begin
      call.wait_all(all, 10_000_000);  // 10ms
      check(all.size() == 1, "rqsv.reply_count");
      if (all.size() == 1) check(all[0].data == 42, "rqsv.reply_value");
      check(rq.stale_responses() == 0, "rqsv.no_stale");
    end
    g_xlang_done++;
  endtask

endmodule
