// ============================================================================
// shuttle_tests.sv — the top of the SV-side test suite of shuttle (on a real simulator, VCS)
//
// Compile order: weave_pkg.sv (../weave/sv) → shuttle_pkg.sv (../../sv) → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → shuttle_tests_publish/broker/trigger/mailbox/xlang/
//                requester.sv → this file (vcs is handed the list in this order by xmake.lua).
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
//
// Composition:
//   · every suite module owns its classes and its case tasks, and runs standalone;
//   · sv_cases below is the only coupling: local cases run in call order (one broker, one timeline);
//     the cross-language cases are forked into their own processes and rendezvous with their C++
//     peers through xlang_barrier, so their order is free;
//   · run_cpp_tests launches the C++-side tests (WEAVE_TEST in tests/*.cpp) via weave_run; both
//     sides share one broker, so they see each other.
//
// Verdict: SV case failures accumulate into g_fail; a C++-side failure makes weave_run return a
// non-zero status. "SHUTTLE TESTS PASSED" is printed only when both sides pass (run-eda.sh keys on
// this marker), otherwise $fatal makes simv exit non-zero.
//
// Note: no file of this suite calls $error — negative cases are judged by return values; every
// assertion, pass or fail, is only logged via $display, with a final catch-all $fatal, so
// run-eda.sh's log matching is not disturbed.
// ============================================================================

module shuttle_tests;
  timeunit 1ns;
  timeprecision 1ps;

  import shuttle_pkg::*;
  import shuttle_tests_runner::*;

  shuttle_tests_publish   m_publish();
  shuttle_tests_broker    m_broker();
  shuttle_tests_trigger   m_trigger();
  shuttle_tests_mailbox   m_mailbox();
  shuttle_tests_xlang     m_xlang();
  shuttle_tests_requester m_requester();

  // ── SV-side case driver: local cases in this process, cross-language cases forked alongside ──
  initial begin : sv_cases
    // The cross-language cases rendezvous with their C++ peers through xlang_barrier (one barrier
    // per case pair), so each runs in its own process: their order relative to each other and to
    // the local cases carries no timing meaning, and a new case is just a new forked task plus one
    // barrier call on each side; the g_xlang_done counter below collects the group
    // before the verdict.
    fork
      m_requester.case_rq_xlang();
      m_xlang.case_xlang();
      m_xlang.case_xlang_mixed();
      m_xlang.case_xlang_nested();
      m_xlang.case_xlang_inherit();
      m_xlang.case_xlang_namespace();
      m_requester.case_rq_sv_requester();
    join_none
    m_publish.case_now_sync();
    m_publish.case_now_blocking();
    m_publish.case_publish_serial();
    m_publish.case_publish_concurrent();
    m_trigger.case_wait_timeout();
    m_publish.case_nb_in_background();
    m_broker.case_dual_registration();
    m_trigger.case_trigger();
    m_publish.case_retained();
    m_publish.case_multi_pub();
    m_broker.case_unsubscribe();
    m_broker.case_retired();
    m_broker.case_bind_conflict();
    m_broker.case_bad_topic();
    m_publish.case_snapshot();
    m_trigger.case_event();
    m_trigger.case_edge_trigger();
    m_trigger.case_self_wait();
    m_mailbox.case_mailbox();
    // SV-side request/response stays in this process: both ends are SV, so it needs no barrier.
    m_requester.case_sv_requester();
    // fork...join_none only starts the group; the counter (one increment per task end) is the
    // join, because `wait fork` would also collect weave's C++-spawned delivery processes.
    wait (g_xlang_done == 7);
    g_sv_done = 1;
    $display("[sv] %0d passed, %0d failed", g_pass, g_fail);
  end

  // ── Launch and final verdict of the C++-side tests ────────────────────────────────────────
  initial begin : run_cpp_tests
    automatic int status;
    weave_pkg::weave_run(status);
    if (status != 0) $fatal(1, "SHUTTLE CPP TESTS FAILED (status=%0d)", status);
    wait (g_sv_done);
    if (g_fail != 0) $fatal(1, "SHUTTLE SV TESTS FAILED (%0d failures)", g_fail);
    $display("SHUTTLE TESTS PASSED (%0d sv cases, cpp ok)", g_pass);
    $finish;
  end

endmodule
