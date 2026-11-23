// The top of the C++-side test suite: the only WEAVE_TEST registration plus the suite order.
//
// Every suite is one translation unit exposing void shuttle_tests_<subject>(); this file declares
// them, calls them in the order below, and nothing else. Registration matters because weave runs
// registered tests in *name* order — the explicit calls here are what fix the order.
//
// Composition:
//   · suites own their message types, subscribers and case bodies, and use independent topic
//     namespaces (bkr.*, mbx.*, rq.*, pub.*), so one suite does not observe another's traffic;
//   · they do share one broker and one simulation timeline, so the order is still part of the
//     contract: the cross-language suite runs last (its SV peer registers the responder in the
//     first SV case and answers C++ case9 only afterwards — see tests/shuttle_tests_requester.sv);
//   · codec runs first: it asserts the wire format and touches no broker state.
//
// The systemc build has no SV peer, so xmake compiles this file without SHUTTLE_TESTS_XLANG and
// the cross-language suite is left out entirely.
//
// Run: just test-sc (systemc) / just test-vcs (svdpi, started by weave_pkg::weave_run inside the
// simulator; see tests/shuttle_tests.sv, which also calls the SV-side suites).

#include <weave/test.hpp>

void shuttle_tests_codec();
void shuttle_tests_publish();
void shuttle_tests_broker();
void shuttle_tests_trigger();
void shuttle_tests_mailbox();
void shuttle_tests_requester();
#ifdef SHUTTLE_TESTS_XLANG
void shuttle_tests_ref();
void shuttle_tests_xlang();
#endif

WEAVE_TEST(shuttle_tests) {
  shuttle_tests_codec();
  shuttle_tests_publish();
  shuttle_tests_broker();
  shuttle_tests_trigger();
  shuttle_tests_mailbox();
  shuttle_tests_requester();
#ifdef SHUTTLE_TESTS_XLANG
  shuttle_tests_ref();
  shuttle_tests_xlang();
#endif
}
