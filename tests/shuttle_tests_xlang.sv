// ============================================================================
// shuttle_tests_xlang.sv — cross-language round trips paired with tests/shuttle_tests_xlang.cpp
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

module shuttle_tests_xlang;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;
  import weave_pkg::*;  // weave_edge for the deferred-publish recipes
  import shuttle_tests_fixtures::*;
  import shuttle_tests_runner::*;

  // ── Inheritance sink: decodes the C++-side XChain (negative middle field included) ──
  class InhRec implements shuttle_sink;
    int    count = 0;
    XChain last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XChain c = XChain::unpack(payload);
      count++;
      last = c;
    endtask
  endclass

  // ── Nested-type sink: decodes the C++-side XTrack (negative coordinates included) ──
  class NestRec implements shuttle_sink;
    int    count = 0;
    XTrack last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XTrack t = XTrack::unpack(payload);
      count++;
      last = t;
    endtask
  endclass

  // ── Cross-language round trip: C++ side in tests/shuttle_tests_xlang.cpp ────────────────────
  task automatic case_xlang();
    automatic Rec                 rec       = new();
    automatic shuttle_subscription         sub       = shuttle_subscribe("xlang.cpp2sv", rec, "XMsg");
    automatic shuttle_publisher #(XMsg) ready_pub = new("xlang.sv_ready");
    automatic shuttle_publisher #(XMsg) to_cpp    = new("xlang.sv2cpp");
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
    shuttle_publisher #(XMsg) nested;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      count++;
      last = m;
      if (nested != null) void'(nested.publish_serial(m));
      #(1ns);
    endtask
  endclass

  // Cross-language mixed target set (C++ side: case6 in tests/shuttle_tests_xlang.cpp). The SV sink
  // registers before the C++ subscriber does, so the frame's target list is
  // [SV blocking entry, C++ blocking subscriber] - the ordering that used to break the identity.
  task automatic case_xlang_mixed();
    automatic MixedSink           sink  = new();
    automatic shuttle_publisher #(XMsg) ready = new("xlang.mixed.ready");
    automatic XMsg                note  = new();
    automatic shuttle_handle      h;
    automatic shuttle_subscription         sub;

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

  // ── Cross-language nested message types: C++ side in tests/shuttle_tests_xlang.cpp case7 ─────────
  // The C++ publisher sends one XTrack with negative coordinates; we decode it with the generated
  // nested codec, verify every field, and answer with another XTrack so the C++ side exercises
  // Codec<XTrack> recursing Codec<XPoint> on the way back. Handshake: we subscribe first and
  // announce readiness with a retained frame, so the C++ publish always has its target.
  task automatic case_xlang_nested();
    automatic NestRec               rec       = new();
    automatic shuttle_subscription           sub       = shuttle_subscribe("xlang.nest.cpp2sv", rec, "XTrack");
    automatic shuttle_publisher #(XTrack) ready_pub = new("xlang.nest.ready");
    automatic shuttle_publisher #(XTrack) to_cpp    = new("xlang.nest.sv2cpp");
    automatic XTrack                m         = new();
    automatic shuttle_handle        h;
    automatic bit                   ok;

    check(sub.id() != 0, "xlang.nest.subscribed");

    // ready broadcast (retained): the C++ side subscribes after this and gets the frame replayed
    ready_pub.set_retained(1);
    m.id = 1;
    h = ready_pub.publish_serial(m);
    h.wait_done();
    ok = (h.first_error() == "");
    check(ok, "xlang.nest.ready_no_failure");

    // wait for the C++-side nested frame; bounded so a broken handshake fails loudly
    for (int i = 0; i < 5000 && rec.count < 1; i++) #(10us);
    check(rec.count == 1, "xlang.nest.count");
    if (rec.last != null) begin
      check(rec.last.id == 32'hC0FFEE01, "xlang.nest.id");
      check(rec.last.origin.x == -100 && rec.last.origin.y == 200, "xlang.nest.origin");
      check(rec.last.dest.x == 300 && rec.last.dest.y == -400, "xlang.nest.dest");
    end

    // answer with another nested frame; wait_done returning means the C++ callback has finished
    m = new();
    m.id = 2;
    m.origin.x = 1;
    m.origin.y = -2;
    m.dest.x = -3;
    m.dest.y = 4;
    h = to_cpp.publish_serial(m);
    h.wait_done();
    ok = (h.first_error() == "");
    check(ok, "xlang.nest.no_failure");
  endtask

  // ── Inheritance chain, both directions (mirror case8 of tests/shuttle_tests_xlang.cpp) ──
  // The C++ side registers XChain with SHUTTLE_REFLECT_DERIVED, so its wire form is the base
  // encoding followed by the own fields; the SV classes reach the same layout through
  // `shuttle_base. Decoding here proves both delegations agree byte for byte, and the frame going
  // back proves the SV pack order is what Codec<XChain> expects.
  task automatic case_xlang_inherit();
    automatic InhRec               rec       = new();
    automatic shuttle_subscription          sub       = shuttle_subscribe("xlang.inherit.cpp2sv", rec, "XChain");
    automatic shuttle_publisher #(XChain) ready_pub = new("xlang.inherit.ready");
    automatic shuttle_publisher #(XChain) to_cpp    = new("xlang.inherit.sv2cpp");
    automatic XChain               m         = new();
    automatic shuttle_handle       h;
    automatic bit                  ok;

    check(sub.id() != 0, "xlang.inherit.subscribed");

    // ready broadcast (retained): the C++ side subscribes after this and gets the frame replayed
    ready_pub.set_retained(1);
    m.seq   = 1;
    m.level = 0;
    m.flag  = 0;
    h = ready_pub.publish_serial(m);
    h.wait_done();
    ok = (h.first_error() == "");
    check(ok, "xlang.inherit.ready_no_failure");

    // wait for the C++-side frame; bounded so a broken handshake fails loudly
    for (int i = 0; i < 5000 && rec.count < 1; i++) #(10us);
    check(rec.count == 1, "xlang.inherit.count");
    if (rec.last != null) begin
      check(rec.last.seq == 32'hC0FFEE02, "xlang.inherit.seq");
      check(rec.last.level == -7, "xlang.inherit.level");
      check(rec.last.flag == 8'h5A, "xlang.inherit.flag");
    end

    // answer with another chain; wait_done returning means the C++ callback has finished
    m = new();
    m.seq   = 2;
    m.level = -3;
    m.flag  = 8'hA5;
    h = to_cpp.publish_serial(m);
    h.wait_done();
    ok = (h.first_error() == "");
    check(ok, "xlang.inherit.no_failure");
  endtask

endmodule
