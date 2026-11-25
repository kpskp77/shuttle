# Common development actions (just). Some recipes first switch xmake to the required backend

# default: run all tests and examples
default: test-all example-all

# systemc backend: local executable; expected last line "WEAVE TEST PASSED"
example-sc:
    xmake f --backend=systemc -y && xmake run shuttle_examples_sc

# real simulator (VCS): compile weave_pkg → shuttle_pkg → shuttle_demo and run simv,
# judged by the output "SHUTTLE CPP DEMO PASSED". Must use xmake run so the library
# search path is prepared.
example-vcs:
    xmake f --backend=svdpi -y && xmake run shuttle_examples_svdpi_vcs

# run the examples on both backends
example-all: example-sc example-vcs

# test suite (systemc backend): all cases in tests/*.cpp, local executable
#   expected last line "WEAVE TEST PASSED"; the cross-language suite is compiled out here
test-sc:
    xmake f --backend=systemc -y && xmake run shuttle_tests_sc

# test suite (VCS): tests/*.cpp + the SV suites tests/shuttle_tests*.sv, judged by "SHUTTLE TESTS PASSED"
test-vcs:
    xmake f --backend=svdpi -y && xmake run shuttle_tests_svdpi_vcs

# fatal-path payloads (a self-blocked wait ends the run through weave::fatal): the judgement is
# inverted — the run must die with the "weave: fatal:" headline instead of printing a pass marker
test-fatal-sc:
    xmake f --backend=systemc -y && xmake run shuttle_tests_fatal_sc_run

# both halves on VCS: the C++ wait through weave_run, then the SV wait through shuttle_c_wait_pub
test-fatal-vcs:
    xmake f --backend=svdpi -y && xmake run shuttle_tests_fatal_cpp_vcs && xmake run shuttle_tests_fatal_sv_vcs

# run the whole test suite (cross-language and fatal paths included)
test-all: test-sc test-fatal-sc test-vcs test-fatal-vcs

# clean
clean:
    xmake clean -a
