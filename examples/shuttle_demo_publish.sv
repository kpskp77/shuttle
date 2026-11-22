// ============================================================================
// shuttle_demo_publish.sv — one subscriber class, both entries, all three publish flavors
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_demo_msgs.sv → this file.
// Driven by shuttle_demo.sv in a fixed order; nothing here depends on another demo module.
// ============================================================================

module shuttle_demo_publish;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;
  import shuttle_demo_msgs::*;

  // ── Subscriber: multiple interface-class implementations, no single-inheritance slot spent ────────────────────
  // implements shuttle_sink     → blocking entry (export task), registered via shuttle_subscribe:
  //                              invoked by the delivery units of `serial`/`concurrent` and the defer section of `publish_now`;
  // implements shuttle_sink_nb  → nonblocking entry (export function), registered via shuttle_subscribe_nb:
  //                              invoked synchronously on the publisher's stack by `publish_now`, and also by `serial`/`concurrent`.
  // To have both entries, register twice (isomorphic to C++ subscribe(fn) + subscribe(nb(fn))).
  class Monitor implements shuttle_sink, shuttle_sink_nb;
    UartTx last;
    int    nb_count = 0;

    // Blocking callback: runs in a broker background simulation process; consuming simulation time is allowed
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartTx tx = UartTx::unpack(payload);
      $display("[monitor] seq=%0d addr=0x%08h data=0x%08h", meta.topic_seq, tx.addr, tx.data);
      #(10ns);
    endtask

    // Nonblocking callback: `publish_now` executes it synchronously inside the publisher's process; a function is compiler-enforced zero-delay
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      last = UartTx::unpack(payload);
      nb_count++;
    endfunction
  endclass

  Monitor              mon;
  shuttle_subscription sub;     // blocking entry
  shuttle_subscription sub_nb;  // nonblocking entry

  task automatic run();
    automatic shuttle_publisher #(UartTx) pub = new("uart0.tx");
    automatic shuttle_handle h;
    automatic UartTx       tx  = new();

    mon = new();
    // The type_tag is validated against the topic's type binding at subscribe time; if a C++ Publisher<UartTx> has already bound it, they must match
    sub    = shuttle_subscribe   ("uart0.tx", mon, UartTx::type_tag);
    sub_nb = shuttle_subscribe_nb("uart0.tx", mon, UartTx::type_tag);

    // ── `publish_now`: synchronous callbacks on the publisher's stack; the publishing process never suspends ───────────────────
    // The registration with a nonblocking entry is invoked synchronously (invoke_nb); the one with a blocking entry is handled per policy —
    // here we skip it with skip; omitting the policy defaults to defer (hand to the background serial chain, invoke called later).
    tx.addr = 32'h10; tx.data = 32'hA5;
    begin
      automatic shuttle_handle hn = pub.publish_now(tx, SHUTTLE_NOW_SKIP);
      if (hn == null) $error("publish_now failed: %s", shuttle_c_last_error());
      else if (hn.skipped() != 1)
        $error("publish_now should skip 1 blocking-entry subscriber (got %0d)", hn.skipped());
    end
    if (mon.last.addr !== 32'h10)
      $error("publish_now did not deliver synchronously");

    // ── `publish_serial`: background single-process, in-order callbacks; returns the handle immediately ─────────────────────────
    tx.addr = 32'h11; tx.data = 32'h5A;
    h = pub.publish_serial(tx);
    // Wait when you need completion semantics: suspends this process via weave_sv_wait; callable directly from a native process
    h.wait_done();

    // ── `publish_concurrent`: background, one process per subscriber, concurrent callbacks + deferred trigger ────────────────────
    // The subscriber callbacks start only after 100ns = 1e8 fs; the subscriber set and message are locked in at the moment of the call
    tx.addr = 32'h12; tx.data = 32'h3C;
    h = pub.publish_concurrent(tx, shuttle_trigger::after_delay(100_000_000));
    h.wait_done();
    if (h.first_error() != "")
      $error("subscriber failure(s) present: %s", h.first_error());

    // ── Other trigger forms ──────────────────────────────────────────────────
    // shuttle_trigger::after_pub(h)       —— follow another Publication to completion
    // shuttle_trigger::after_edge(e)      —— wait for a weave edge (clock, reset, handshake):
    //   automatic weave_edge e = new("uart0.mon.edge");
    //   h = pub.publish_serial(tx, shuttle_trigger::after_edge(e));  // snapshot taken here
    //   ...; e.fire();                                               // callbacks start now
    // Only the edge *name* crosses into C++ (weave's own name table resolves it), and an edge is
    // not a latch: a fire() with no armed waiter is lost, exactly like `-> ev`.
    // Publishing immediately is equivalent to shuttle_trigger::none() or omitting the trig argument.
    // If you do not need the trigger snapshot, the cheaper recipe is to publish from the waiter —
    //   fork begin e.wait_edge(); pub.publish_serial(tx); end join_none

    // ── retained (latched): once enabled, a new subscriber receives the latest frame replayed at registration time (here we only flip the switch) ────
    pub.set_retained(1);

    // ── One-to-many: multiple publisher instances share the same topic and subscriber set ──────────────────
    // (Same as C++: multiple shuttle_publisher #(UartTx) instances all bind the "uart0.tx" type tag,
    //  each publishes independently, and every subscriber independently receives every published frame)
    begin
      automatic shuttle_publisher #(UartTx) pub_b = new("uart0.tx");
      tx.addr = 32'h15; tx.data = 32'h55;
      begin
        automatic shuttle_handle hn = pub_b.publish_now(tx, SHUTTLE_NOW_SKIP);
        if (hn == null) $error("multi-publisher publish_now failed: %s", shuttle_c_last_error());
      end
      if (mon.last.addr !== 32'h15)
        $error("multi-publisher did not deliver");
    end
  endtask

  // ── Unsubscribe (idempotent) ──────────────────────────────────────────────────────
  // Called by the top after every module has run: the monitor must stay registered while the C++
  // demos share the topic, exactly as before the split.
  task automatic unsubscribe();
    sub_nb.unsubscribe();
    sub.unsubscribe();
  endtask

endmodule
