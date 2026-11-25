// ============================================================================
// shuttle_tests_fatal_cpp.sv — VCS top for the C++ half of the fatal path
//
// Runs the WEAVE_TEST bodies of the fatal payload (tests/shuttle_tests_fatal.cpp) through
// weave_run, which is where the self-blocked wait ends the simulation ($fatal). Judged with
// inverted expectation by tests/run-eda.sh --expect-fatal: returning at all is the failure.
// ============================================================================

module shuttle_tests_fatal_cpp;
  timeunit 1ns;
  timeprecision 1ps;

  initial begin : run_fatal_cpp
    automatic int status;
    weave_pkg::weave_run(status);
    $display("[fatal-cpp] the payload returned (status=%0d): the fatal path did not end the run",
             status);
    $fatal(1, "SHUTTLE FATAL CPP PAYLOAD RETURNED");
  end
endmodule
