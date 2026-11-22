// ============================================================================
// shuttle_demo_inherit.sv — inherited message classes across the DPI boundary
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_demo_msgs.sv → this file.
// Decodes the retained UartTagged the C++ shuttle_demo_publish publishes on "uart0.tag".
// ============================================================================

module shuttle_demo_inherit;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;
  import shuttle_demo_msgs::*;

  class TagRec implements shuttle_sink;
    int        count = 0;
    UartTagged last;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartTagged t = UartTagged::unpack(payload);
      count++;
      last = t;
    endtask
  endclass

  // ── Inherited message classes: decode the retained UartTagged published by shuttle_demo_publish.cpp ──
  // The frame crosses the DPI boundary in the base-first order both macros produce; the inherited
  // field names never appear in the derived class, only `shuttle_base.
  task automatic run();
    automatic TagRec               trec = new();
    automatic shuttle_subscription tsub = shuttle_subscribe("uart0.tag", trec, UartTagged::type_tag);
    for (int i = 0; i < 5000 && trec.count < 1; i++) #(10us);
    if (trec.count != 1) $error("inherited message was not delivered");
    if (trec.last != null) begin
      if (trec.last.addr !== 32'h30) $error("inherited message: base field wrong");
      if (trec.last.data !== 32'hCAFE) $error("inherited message: middle field wrong");
      if (trec.last.flags !== 8'h05) $error("inherited message: own field wrong");
    end
  endtask

endmodule
