// ============================================================================
// shuttle_tests_runner.sv — pass/fail counters and the check() helper shared by the suites
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

package shuttle_tests_runner;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;

  int unsigned g_pass = 0;
  int unsigned g_fail = 0;
  // set by each forked cross-language case at completion; shuttle_tests.sv waits on this instead
  // of `wait fork` (weave's C++-spawned delivery processes would otherwise be collected as
  // children of the driver process and never let it return)
  int unsigned g_xlang_done = 0;
  bit          g_sv_done = 0;

  // Handshake marker payload for xlang_barrier (mirror of struct XHs in tests/
  // shuttle_tests_xlang.cpp): one byte, no fields anyone asserts on.
  class XHs;
    byte v;
    `shuttle_reflect_begin(XHs)
      `shuttle_field(v, shuttle_u8)
    `shuttle_reflect_end
  endclass

  // Per-case rendezvous between the two languages. This side publishes a retained marker on
  // "xlang.hs.sv.<name>" and waits (bounded — the bound is a watchdog, not the synchronization)
  // for the C++ marker on "xlang.hs.cpp.<name>"; the C++ side does the mirror image. The retained
  // replay makes the pairing order-free, so a new cross-language case needs exactly one
  // xlang_barrier("<case>") call on each side and no call-list timing to reason about.
  task automatic xlang_barrier(input string name);
    automatic shuttle_publisher #(XHs) mine = new({"xlang.hs.sv.", name});
    automatic shuttle_mailbox peer = new({"xlang.hs.cpp.", name}, "XHs");
    automatic XHs m = new();
    automatic bit seen = 0;
    mine.set_retained(1);
    void'(mine.publish_serial(m));
    for (int i = 0; i < 100_000 && !seen; i++) begin
      seen = (peer.size() != 0);
      if (!seen) #(10us);
    end
    check(seen, {"xlang.hs.", name, ".sv_seen_cpp"});
  endtask

  task automatic check(bit cond, string name);
    if (cond) begin
      g_pass++;
      $display("[PASS] sv::%s", name);
    end else begin
      g_fail++;
      $display("[FAIL] sv::%s", name);
    end
  endtask

endpackage
