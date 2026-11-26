// ============================================================================
// shuttle_demo_namespace.sv — a message class whose C++ twin is declared in a namespace
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → this file.
// The C++ type is `demo_hw::UartCfg` in examples/shuttle_demo_namespace.cpp, reflected through
// the global alias `UartCfg`: SHUTTLE_REFLECT must run at global scope and its first argument
// doubles as the type tag, so "UartCfg" (the alias spelling) is the type_tag below. The C++ publish
// is retained, so this subscriber sees the frame even when it registers after the publish.
// ============================================================================

module shuttle_demo_namespace;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;

  // ── Counterpart of `struct demo_hw::UartCfg { uint32_t baud; uint8_t mode; }` ──
  class UartCfg;
    int unsigned baud;
    byte         mode;
    `shuttle_reflect_begin(UartCfg)
      `shuttle_field(baud, shuttle_u32)
      `shuttle_field(mode, shuttle_u8)
    `shuttle_reflect_end
  endclass

  class CfgRec implements shuttle_sink;
    int     count = 0;
    UartCfg last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartCfg c = UartCfg::unpack(payload);
      count++;
      last = c;
    endtask
  endclass

  // ── Decode the retained UartCfg published by shuttle_demo_namespace.cpp ──
  // The assertions run silently — a wrong field fails the demo through $error, like everywhere else.
  task automatic run();
    automatic CfgRec                crec = new();
    automatic shuttle_subscription  csub = shuttle_subscribe("cfg.ns.uart", crec, UartCfg::type_tag);
    // The C++ demo publishes retained, but stay bounded: a broken handshake must fail loudly.
    for (int i = 0; i < 5000 && crec.count < 1; i++) #(10us);
    if (crec.count != 1) $error("namespaced message was not delivered");
    if (crec.last != null) begin
      if (crec.last.baud !== 32'd115200) $error("namespaced message: baud wrong");
      if (crec.last.mode !== 8'd3) $error("namespaced message: mode wrong");
    end
  endtask

endmodule
