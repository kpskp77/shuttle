// ============================================================================
// shuttle_demo.sv — SV-side user-interface demo of shuttle
//
// Compile order: weave_pkg.sv (../weave/sv) → shuttle_pkg.sv (../../sv) → this file.
// Run: the simulator loads libshuttle_examples_svdpi.so via -sv_lib (which brings in
//       libshuttle.so / libweave.so indirectly); weave_run also starts the C++-side registered
//       demos (WEAVE_TEST, see ../): Publishing/subscribing on both sides interoperates on the same broker:
//       the "uart0.tx" this file subscribes to also receives messages published by the C++ basic_demo, and vice versa.
// ============================================================================

module shuttle_demo;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;

  // ── Message class: counterpart of `struct UartTx { uint32_t addr; uint32_t data }` on the C++ side ──
  // The three-piece macros (sv/shuttle_reflect.svh) generate the whole message-class convention:
  //   · localparam string type_tag = "UartTx" — must equal the C++ struct name, validated at publish time;
  //   · pack()/unpack() byte-for-byte equal to the C++ Writer's big-endian wire order;
  //   · each shuttle_field is a (field, codec) pair; the codec name carries the width/encoding
  //     contract while the field keeps whatever SV type you prefer (see sv/shuttle_codec.svh).
  class UartTx;
    int unsigned addr;
    int unsigned data;
    `shuttle_reflect_begin(UartTx)
      `shuttle_field(addr, shuttle_u32)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // ── Nested message class: counterpart of `struct UartBurst` in basic_demo.cpp ──
  // `shuttle_field(first, UartTx)` makes the nested class itself the field codec: the macro
  // generates UartTx::pack_obj(q, o.first) / UartTx::unpack_into(b, pos, o.first) — the same
  // recursion Codec<UartBurst> → Codec<UartTx> runs on the C++ side, inline, no length prefix.
  // The frame is published retained, so this subscriber receives it no matter which side of the
  // DPI boundary subscribes first.
  class UartBurst;
    byte   count;
    UartTx first = new();
    UartTx last  = new();
    `shuttle_reflect_begin(UartBurst)
      `shuttle_field(count, shuttle_u8)
      `shuttle_field(first, UartTx)
      `shuttle_field(last, UartTx)
    `shuttle_reflect_end
  endclass

  class BurstRec implements shuttle_sink;
    int       count = 0;
    UartBurst last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartBurst b = UartBurst::unpack(payload);
      count++;
      last = b;
    endtask
  endclass

  // ── Inherited message classes: counterpart of UartHeader/UartFrame/UartTagged in basic_demo.cpp ──
  // `shuttle_base(T) encodes the direct base subobject through T's own codec, so each level lists
  // only its own fields — the SV twin of SHUTTLE_REFLECT_DERIVED. Wire order is base first, then
  // own fields, which is exactly the flat listing; here the inherited fields are written once.
  class UartHeader;
    int unsigned addr;
    `shuttle_reflect_begin(UartHeader)
      `shuttle_field(addr, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class UartFrame extends UartHeader;
    int unsigned data;
    `shuttle_reflect_begin(UartFrame)
      `shuttle_base(UartHeader)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class UartTagged extends UartFrame;
    byte flags;
    `shuttle_reflect_begin(UartTagged)
      `shuttle_base(UartFrame)
      `shuttle_field(flags, shuttle_u8)
    `shuttle_reflect_end
  endclass

  class TagRec implements shuttle_sink;
    int        count = 0;
    UartTagged last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartTagged t = UartTagged::unpack(payload);
      count++;
      last = t;
    endtask
  endclass

  // ── Responder for the requester section: answers in its nonblocking entry and hands the request's
  //    frame header to `pub.reply`, which echoes meta.corr_id — the answer never mentions the id ──
  class UartEchoNb implements shuttle_sink_nb;
    shuttle_pub #(UartTx) pub;
    function new(string resp_topic);
      pub = new (resp_topic);
    endfunction
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      UartTx req  = UartTx::unpack(payload);
      UartTx resp = new();
      resp.addr = req.addr;
      resp.data = req.data + 1;
      void'(pub.reply(resp, meta));
    endfunction
  endclass

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

  initial begin : demo
    automatic Monitor      mon = new();
    // The type_tag is validated against the topic's type binding at subscribe time; if a C++ Publisher<UartTx> has already bound it, they must match
    automatic shuttle_sub  sub  = shuttle_subscribe   ("uart0.tx", mon, UartTx::type_tag);
    automatic shuttle_sub  subn = shuttle_subscribe_nb("uart0.tx", mon, UartTx::type_tag);
    automatic shuttle_pub #(UartTx) pub = new("uart0.tx");
    automatic shuttle_handle h;
    automatic UartTx       tx  = new();

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
    // (Same as C++: multiple shuttle_pub#(UartTx) instances all bind the "uart0.tx" type tag,
    //  each publishes independently, and every subscriber independently receives every published frame)
    begin
      automatic shuttle_pub #(UartTx) pub_b = new("uart0.tx");
      tx.addr = 32'h15; tx.data = 32'h55;
      begin
        automatic shuttle_handle hn = pub_b.publish_now(tx, SHUTTLE_NOW_SKIP);
        if (hn == null) $error("multi-publisher publish_now failed: %s", shuttle_c_last_error());
      end
      if (mon.last.addr !== 32'h15)
        $error("multi-publisher did not deliver");
    end

    // ── Nested message types: decode the retained UartBurst published by basic_demo.cpp ──
    // Both entries in one scope: the sink decodes with the generated nested codec; the assertions
    // run silently — a wrong nested field fails the demo through $error, like everywhere above.
    begin
      automatic BurstRec              brec = new();
      automatic shuttle_sub           bsub = shuttle_subscribe("uart0.burst", brec, UartBurst::type_tag);
      // The C++ demo publishes retained, but stay bounded: a broken handshake must fail loudly,
      // not hang the demo.
      for (int i = 0; i < 5000 && brec.count < 1; i++) #(10us);
      if (brec.count != 1) $error("nested burst was not delivered");
      if (brec.last != null) begin
        if (brec.last.count !== 2) $error("nested burst: count field wrong");
        if (brec.last.first.addr !== 32'h21 || brec.last.first.data !== 32'hAA)
          $error("nested burst: first transaction wrong");
        if (brec.last.last.addr !== 32'h22 || brec.last.last.data !== 32'hBB)
          $error("nested burst: last transaction wrong");
      end
    end

    // ── Inherited message classes: decode the retained UartTagged published by basic_demo.cpp ──
    // The frame crosses the DPI boundary in the base-first order both macros produce; the inherited
    // field names never appear in the derived class, only `shuttle_base.
    begin
      automatic TagRec            trec = new();
      automatic shuttle_sub       tsub = shuttle_subscribe("uart0.tag", trec, UartTagged::type_tag);
      for (int i = 0; i < 5000 && trec.count < 1; i++) #(10us);
      if (trec.count != 1) $error("inherited message was not delivered");
      if (trec.last != null) begin
        if (trec.last.addr !== 32'h30) $error("inherited message: base field wrong");
        if (trec.last.data !== 32'hCAFE) $error("inherited message: middle field wrong");
        if (trec.last.flags !== 8'h05) $error("inherited message: own field wrong");
      end
    end

    // ── Mailbox: a subscription with a queue attached ("wait for the next frame") ─────────────────
    // Pure SV (no DPI entry): shuttle_subscribe_nb + weave_pkg's event primitives. The callback only
    // copies the frame into the queue, so publish_now fills it synchronously — the same contract as
    // the C++ shuttle::mailbox<UartTx> in ../cpp/mailbox_demo.cpp.
    begin
      automatic shuttle_mailbox         mbox = new("mbox.demo");
      automatic shuttle_pub #(UartTx)   mpub = new("mbox.demo");
      automatic shuttle_mailbox         cap  = new("mbox.cap", "", 2, SHUTTLE_OVERFLOW_DROP_NEWEST);
      automatic shuttle_pub #(UartTx)   cpub = new("mbox.cap");
      automatic byte                    payload[];
      automatic shuttle_meta            meta;
      automatic UartTx                  got;
      automatic shuttle_handle          hm;
      automatic bit                     got_ok;

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
    end

    // ── Requester: request/response with a correlation id ("ask the peer and wait for its answer") ──────
    // Counterpart of the C++ shuttle::Requester in ../cpp/request_response_demo.cpp. The id comes from
    // shuttle_c_take_corr_id() — the same counter C++ draws from — and every reply is routed back by it,
    // so two in-flight calls cannot mix up their answers. The response topic is claimed as a response
    // channel first, exactly like the C++ side, which is why a blocking observer there would be refused.
    begin
      automatic UartEchoNb                          svc  = new ("uart0.rpc.resp");
      automatic shuttle_sub                         ssub = shuttle_subscribe_nb("uart0.rpc.req", svc,
                                                                              "UartTx");
      automatic shuttle_requester #(UartTx, UartTx) rq   = new ("uart0.rpc.req", "uart0.rpc.resp");
      automatic UartTx                              each[$];
      automatic shuttle_call #(UartTx)              call;
      automatic UartTx                              answer;
      automatic bit                                 got_ok;

      if (!rq.valid()) $error("requester: %0s", rq.last_error());
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
      ssub.unsubscribe();
    end

    // ── Unsubscribe (idempotent) ──────────────────────────────────────────────────────
    subn.unsubscribe();
    sub.unsubscribe();
  end

  // ── Unified launch entry of the C++-side demos ────────────────────────────────────────────
  // The C++ demos are registered as WEAVE_TESTs (see *.cpp) and executed serially by the weave root task.
  initial begin : run_cpp_demos
    automatic int status;
    weave_pkg::weave_run(status);
    if (status != 0) $fatal(1, "SHUTTLE DEMO FAILED (status=%0d)", status);
    $display("SHUTTLE DEMO PASSED");
  end

endmodule
