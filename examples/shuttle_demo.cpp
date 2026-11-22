// The top of the C++ half of the uart demo: the only WEAVE_TEST registration plus the demo order.
//
// Every subject file exposes void shuttle_demo_<subject>(); this file calls them in the order
// below and does nothing else (weave would otherwise order registered tests by name). The SV half
// mirrors the shape: examples/shuttle_demo_msgs.sv plus one module per subject, run in order by
// examples/shuttle_demo.sv.
//
// This file and its subject files use only the public shuttle/weave headers and are built exactly
// like a user linking libshuttle:
//   · systemc backend: compiled into the shuttle_examples_sc executable (entry:
//     tests/shuttle_tests_main.cpp), run locally;
//   · svdpi backend:   compiled into libshuttle_examples_svdpi.so, loaded by the simulator via
//                      -sv_lib and started by weave_pkg::weave_run from examples/shuttle_demo.sv.
//
// Expected output (values) is documented in examples/README.md.

#include <weave/test.hpp>

void shuttle_demo_publish();
void shuttle_demo_nested();
void shuttle_demo_inherit();
void shuttle_demo_mailbox();
void shuttle_demo_requester();
void shuttle_demo_trigger();
void shuttle_demo_edge();

WEAVE_TEST(shuttle_demo) {
  shuttle_demo_publish();
  shuttle_demo_nested();
  shuttle_demo_inherit();
  shuttle_demo_mailbox();
  shuttle_demo_requester();
  shuttle_demo_trigger();
  shuttle_demo_edge();
}
