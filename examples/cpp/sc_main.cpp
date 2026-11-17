// Example entry point of the systemc backend: runs all registered examples
// (WEAVE_TEST) on a real SystemC kernel.
// The svdpi backend does not need this file — examples are started by
// weave_pkg::weave_run inside the simulator.

#include <weave/sc.hpp>

// xmake-repo's SystemC build looks up sc_main with C linkage, hence the
// explicit extern "C".
extern "C" __attribute__((visibility("default"))) int sc_main(int, char**);

extern "C" __attribute__((visibility("default"))) int sc_main(int, char**) {
  return weave::sc::run();
}
