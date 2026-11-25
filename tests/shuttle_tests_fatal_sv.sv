// ============================================================================
// shuttle_tests_fatal_sv.sv — VCS top for the SV half of the fatal path
//
// An ordinary SV process subscribes to a topic, publishes on that same topic from the callback and
// calls wait_done() on the new frame. The frame queues behind the very delivery that is asking for
// it, so shuttle_c_wait_pub's self-block check fires and ends the simulation (weave::fatal →
// $fatal on this backend). Judged with inverted expectation by tests/run-eda.sh --expect-fatal:
// returning from wait_done() at all is the failure.
// ============================================================================

module shuttle_tests_fatal_sv;
  timeunit 1ns;
  timeprecision 1ps;

  import shuttle_pkg::*;
  import shuttle_tests_fixtures::*;

  class FatalSink implements shuttle_sink;
    shuttle_publisher #(XMsg) pub;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg              echo  = new();
      shuttle_publication inner;
      echo.addr = 32'hD2;
      echo.data = 32'h0;
      inner = pub.publish_serial(echo);  // same topic: queued behind this delivery
      inner.wait_done();
      $display("[fatal-sv] wait_done returned: the diagnosis is catchable again");
      $fatal(1, "SHUTTLE FATAL SV PAYLOAD RETURNED");
    endtask
  endclass

  initial begin : run_fatal_sv
    automatic FatalSink               sink = new();
    automatic shuttle_publisher #(XMsg) pub = new("fatal.sv.selfwait");
    automatic XMsg                    m    = new();
    automatic shuttle_subscription    sub;
    sink.pub = pub;
    sub = shuttle_subscribe("fatal.sv.selfwait", sink, "XMsg");
    m.addr = 32'hD1;
    m.data = 32'h0;
    pub.publish_serial(m).wait_done();
    #(1us);
    $display("[fatal-sv] the outer frame completed without the diagnosis");
    $fatal(1, "SHUTTLE FATAL SV PAYLOAD RETURNED");
  end
endmodule
