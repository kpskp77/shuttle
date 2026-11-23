// ============================================================================
// shuttle_demo.sv — SV-side demo of shuttle: the top that runs the subject modules in order
//
// Compile order: weave_pkg.sv (../weave/sv) → shuttle_pkg.sv (../../sv) → shuttle_demo_msgs.sv →
//                shuttle_demo_publish.sv / _nested.sv / _inherit.sv / _namespace.sv / _ref.sv /
//                _mailbox.sv / _requester.sv → this file (vcs is handed the list in this order by
//                xmake.lua).
// Run: the simulator loads libshuttle_examples_svdpi.so via -sv_lib (which brings in
//       libshuttle.so / libweave.so indirectly); weave_run also starts the C++-side registered
//       demos (WEAVE_TEST, see ../): publishing/subscribing on both sides interoperates on the same
//       broker — the "uart0.tx" the publish module subscribes to also receives messages published by
//       the C++ shuttle_demo_publish, and vice versa.
//
// This file holds no classes and no assertions: each module owns the classes it demonstrates and
// runs standalone, the order below is the only coupling between them.
// ============================================================================

module shuttle_demo;
  timeunit 1ns; timeprecision 1ps;

  shuttle_demo_publish   m_publish();
  shuttle_demo_nested    m_nested();
  shuttle_demo_inherit   m_inherit();
  shuttle_demo_namespace m_namespace();
  shuttle_demo_ref       m_ref();
  shuttle_demo_mailbox   m_mailbox();
  shuttle_demo_requester m_requester();

  initial begin : demo
    m_publish.run();      // both entries, publish_now / serial / concurrent, retained, multi-publisher
    m_nested.run();       // retained UartBurst from the C++ side
    m_inherit.run();      // retained UartTagged from the C++ side
    m_namespace.run();    // retained UartCfg whose C++ type lives in namespace demo_wire
    m_ref.run();          // object references: published here, echoed by the C++ mirror, resolved here,
                          // and carried inside a request/response through shuttle_ref_codec
    m_mailbox.run();      // pure-SV mailbox, bounded queue, close()
    m_requester.run();    // request/response with a correlation id
    // Unsubscribing last keeps the monitor registered while the C++ demos share "uart0.tx", which is
    // what the subscriber count in shuttle_demo_publish's introspection expects.
    m_publish.unsubscribe();
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
