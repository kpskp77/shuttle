// ============================================================================
// shuttle_demo_mailbox.sv — a subscription with a queue attached ("wait for the next frame")
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_demo_msgs.sv → this file.
// Pure SV (no DPI entry): shuttle_subscribe_nb + weave_pkg's event primitives. The callback only
// copies the frame into the queue, so publish_now fills it synchronously — the same contract as
// the C++ shuttle::mailbox<UartTx> in shuttle_demo_mailbox.cpp.
// ============================================================================

module shuttle_demo_mailbox;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;
  import shuttle_demo_msgs::*;

  task automatic run();
    automatic shuttle_mailbox              mbox = new("mbox.demo");
    automatic shuttle_publisher #(UartTx)  mpub = new("mbox.demo");
    automatic shuttle_mailbox              cap  = new("mbox.cap", "", 2, SHUTTLE_OVERFLOW_DROP_NEWEST);
    automatic shuttle_publisher #(UartTx)  cpub = new("mbox.cap");
    automatic UartTx                       tx   = new();
    automatic byte                         payload[];
    automatic shuttle_meta                 meta;
    automatic UartTx                       got;
    automatic shuttle_publication               hm;
    automatic bit                          got_ok;

    // A mailbox is an ordinary subscriber: it is a target of the publication (subscriber_count),
    // and its queue is filled inline while publish_now runs.
    tx.addr = 32'h20; tx.data = 32'h01;
    hm = mpub.publish_now(tx, SHUTTLE_NOW_SKIP);
    if (hm.subscriber_count() != 1 || mbox.size() != 1)
      $error("mailbox: publish_now did not fill the queue synchronously");
    if (!mbox.try_recv(payload, meta))
      $error("mailbox: try_recv found nothing");
    got = UartTx::unpack(payload);
    $display("[mbox] addr=0x%08h data=0x%08h topic=%0s received=%0d", got.addr, got.data, meta.topic_name,
             mbox.received());

    // Nothing is pending: recv with a deadline reports ok == 0 instead of hanging, and no frame is lost.
    mbox.recv(got_ok, payload, meta, 1_000_000);  // 1us
    if (got_ok != 0) $error("mailbox: recv on an empty mailbox must time out");

    // Bounded mailbox: drop_newest keeps the queue at capacity and counts the refusals,
    // separately from the frames that actually reached the subscriber.
    for (int i = 1; i <= 4; ++i) begin
      tx.addr = 32'h30 + i;
      hm = cpub.publish_now(tx, SHUTTLE_NOW_SKIP);
    end
    $display("[mbox] capacity=2 received=%0d overflowed=%0d", cap.received(), cap.overflowed());
    if (cap.size() != 2 || cap.received() != 2 || cap.overflowed() != 2)
      $error("mailbox: bounded-mailbox accounting is wrong");

    // close(): queued frames stay readable (drop_newest kept the first two), then recv reports
    // ok == 0 once the queue is drained — no exception channel in SV, the output flag is the signal.
    cap.close();
    if (cap.try_recv(payload, meta)) got = UartTx::unpack(payload);
    if (got.addr != 32'h31) $error("mailbox: queued frame must survive close()");
    cap.recv(got_ok, payload, meta, 0);  // second queued frame is still delivered
    cap.recv(got_ok, payload, meta, 0);  // now drained and closed
    if (got_ok != 0) $error("mailbox: recv after close must report ok == 0");
  endtask

endmodule
