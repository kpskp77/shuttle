// Test entry point of the systemc backend: runs all tests registered under
// tests/cpp on a real SystemC kernel.
// The svdpi backend does not need this file — tests are started by
// weave_pkg::weave_run inside the simulator; see tests/sv/shuttle_tests.sv.

#include <weave/sc.hpp>

// xmake-repo's SystemC build looks up sc_main with C linkage, hence the
// explicit extern "C".
extern "C" __attribute__((visibility("default"))) int sc_main(int, char**);

extern "C" __attribute__((visibility("default"))) int sc_main(int, char**) {
  return weave::sc::run();
}
