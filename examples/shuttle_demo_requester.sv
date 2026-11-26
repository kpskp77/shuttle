// ============================================================================
// shuttle_demo_requester.sv — request/response with a correlation id
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_demo_msgs.sv → this file.
// Counterpart of the C++ shuttle::Requester in shuttle_demo_requester.cpp. The id comes from
// shuttle_c_take_corr_id() — the same counter C++ draws from — and every reply is routed back by
// it, so two in-flight calls cannot mix up their answers. Replies go out with `skip`, so a
// blocking observer on the response topic never sees them; watch replies with a nonblocking
// entry and tell them apart from hand-published frames by meta.corr_id (nonzero means a reply).
// ============================================================================

module shuttle_demo_requester;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;
  import shuttle_demo_msgs::*;

  // ── Responder: answers in a nonblocking entry; shuttle_serve owns the response publisher and
  //    echoes meta.corr_id through pub.reply — the answer never mentions the id ──
  class UartEcho implements shuttle_responder #(UartTx, UartTx);
    virtual function UartTx respond(input UartTx req, input shuttle_meta meta);
      UartTx resp = new();
      resp.addr = req.addr;
      resp.data = req.data + 1;
      return resp;
    endfunction
  endclass

  task automatic run();
    automatic UartEcho                            hdl = new();
    automatic shuttle_serve #(UartTx, UartTx)     svc = shuttle_serve #(UartTx, UartTx)::nonblocking(
        "uart0.rpc.req", "uart0.rpc.resp", hdl);
    automatic shuttle_requester #(UartTx, UartTx) rq   = new ("uart0.rpc.req", "uart0.rpc.resp");
    automatic UartTx                              each[$];
    automatic shuttle_call #(UartTx)              call;
    automatic UartTx                              answer;
    automatic bit                                 got_ok;
    automatic UartTx                              tx = new();

    if (!rq.valid()) $error("requester: %0s", rq.last_error());
    if (!svc.valid()) $error("responder: %0s", svc.last_error());
    tx.addr = 32'h40;
    tx.data = 32'h1000;

    // send_now + a nonblocking responder: the whole round trip runs on this stack, so the answer is
    // already queued when send_now returns
    call = rq.send_now(tx);
    if (call == null || call.queued() != 1)
      $error("requester: the nonblocking responder must answer inside send_now");
    call.recv(got_ok, answer, 1_000_000);  // 1us
    if (got_ok != 1 || answer == null || answer.data != 32'h1001)
      $error("requester: wrong answer");

    // serial flavor: the request travels the background chain, so the call waits on its own event;
    // wait_all expects one answer per target of the request frame
    call = rq.send_serial(tx);
    call.wait_all(each, 1_000_000);  // 1us
    if (each.size() != 1 || each[0].data != 32'h1001) $error("requester: serial answer");
    $display("[rpc] corr_id=%0d answer=0x%08h", call.corr_id(), each[0].data);
    svc.subscription().unsubscribe();
  endtask

endmodule
