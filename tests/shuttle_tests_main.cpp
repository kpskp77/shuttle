// SystemC entry point shared by the test suite (tests/shuttle_tests.cpp and its subject files) and
// the example build (examples/shuttle_demo.cpp and its subject files): it runs everything
// registered with WEAVE_TEST on a real SystemC kernel. The svdpi backend does not need this file —
// tests and examples are started by weave_pkg::weave_run inside the simulator; see
// tests/shuttle_tests.sv and examples/shuttle_demo.sv.

#include <weave/sc.hpp>

// xmake-repo's SystemC build looks up sc_main with C linkage, hence the
// explicit extern "C".
extern "C" __attribute__((visibility("default"))) int sc_main(int, char**);

extern "C" __attribute__((visibility("default"))) int sc_main(int, char**) {
  return weave::sc::run();
}
