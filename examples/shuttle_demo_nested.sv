// ============================================================================
// shuttle_demo_nested.sv — nested message type across the DPI boundary
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_demo_msgs.sv → this file.
// Decodes the retained UartBurst the C++ shuttle_demo_publish publishes on "uart0.burst".
// ============================================================================

module shuttle_demo_nested;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;
  import shuttle_demo_msgs::*;

  class BurstRec implements shuttle_sink;
    int       count = 0;
    UartBurst last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartBurst b = UartBurst::unpack(payload);
      count++;
      last = b;
    endtask
  endclass

  // ── Nested message types: decode the retained UartBurst published by shuttle_demo_publish.cpp ──
  // The sink decodes with the generated nested codec; the assertions run silently — a wrong
  // nested field fails the demo through $error, like everywhere else.
  task automatic run();
    automatic BurstRec              brec = new();
    automatic shuttle_subscription  bsub = shuttle_subscribe("uart0.burst", brec, UartBurst::type_tag);
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
  endtask

endmodule
