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
  bit          g_sv_done = 0;

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
