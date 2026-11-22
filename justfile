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
#   expected last line "WEAVE TEST PASSED"; the xlang cases SKIP by design
test-sc:
    xmake f --backend=systemc -y && xmake run shuttle_tests_sc

# test suite (VCS): tests/*.cpp + tests/shuttle_tests.sv, judged by "SHUTTLE TESTS PASSED"
test-vcs:
    xmake f --backend=svdpi -y && xmake run shuttle_tests_svdpi_vcs

# run the whole test suite (cross-language included)
test-all: test-sc test-vcs

# clean
clean:
    xmake clean -a
