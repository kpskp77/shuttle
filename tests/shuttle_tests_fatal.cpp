// Fatal-path self-check: a wait on a frame that can never start ends the run through
// weave::fatal() — it does not throw, and it is not an ordinary failure the caller could catch.
//
// This file is not part of the suites (xmake excludes it from shuttle_tests_sc /
// shuttle_tests_svdpi and compiles it into the fatal payloads only): a process can only die once,
// so the body cannot share a run with the ordinary cases, and its verdict is inverted — tests/
// expect_fatal.sh on SystemC and tests/run-eda.sh --expect-fatal on VCS require the run to die with
// the "weave: fatal:" headline and never to reach a pass marker.
//
// What it pins: detail::check_self_blocked fires at the wait (Publication::wait / wait(timeout) /
// SV shuttle_c_wait_pub) when the awaited frame sits behind a delivery chain this very task holds.
// SHUTTLE_FATAL_CHAIN=concurrent selects the per-subscriber chain of publish_concurrent; anything
// else uses the per-topic chain of publish_serial. Both end in the same diagnosis.
//
// Assertion bodies stay in tests/ (see AGENTS.md); the backend entry points are
// tests/shuttle_tests_main.cpp (SystemC) and tests/shuttle_tests_fatal_cpp.sv (VCS).

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <weave/weave.hpp>

struct FatalMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(FatalMsg, v)

WEAVE_TEST(fatal_selfwait) {
  const char* chain = std::getenv("SHUTTLE_FATAL_CHAIN");
  const bool concurrent = chain && std::string(chain) == "concurrent";
  std::printf("[fatal-selfwait] chain=%s: a callback publishes on its own topic and waits for it\n",
              concurrent ? "concurrent" : "serial");
  std::fflush(stdout);

  shuttle::Publisher<FatalMsg> pub("fatal.selfwait");
  std::vector<shuttle::Publication> inner;
  auto sub =
      shuttle::subscribe<FatalMsg>("fatal.selfwait", [&](const FatalMsg& m, const shuttle::Meta&) {
        if (m.v != 1) return;
        inner.push_back(concurrent ? pub.publish_concurrent(FatalMsg{2})
                                   : pub.publish_serial(FatalMsg{2}));
        // Frame 2 queues behind the delivery that is asking for it, so it can never start: the wait
        // must end the simulation here. Reaching the next line means the diagnosis degraded into
        // something catchable, which the inverted judgement reports as a failed run.
        inner.back().wait();
        std::printf("[fatal-selfwait] the wait returned: the diagnosis is catchable again\n");
        std::fflush(stdout);
      });
  // The outer frame must use the same flavor as the inner one: a frame of the other flavor does
  // not share this chain and would start immediately instead of queueing.
  auto outer = concurrent ? pub.publish_concurrent(FatalMsg{1}) : pub.publish_serial(FatalMsg{1});
  outer.wait();
  throw std::logic_error("case1: the self-blocked wait returned instead of ending the run");
}
