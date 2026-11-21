# Common development actions (just). Some recipes first switch xmake to the required backend

# default: build the library and examples for the current backend
default: build

# build library + all examples (backend of the current xmake configuration)
build:
    xmake build

# svdpi backend: build the shared library and the payload .so
svdpi:
    xmake f --backend=svdpi -y
    xmake build shuttle_examples_svdpi

# systemc backend: local executable; expected last line "WEAVE TEST PASSED"
sc:
    xmake f --backend=systemc -y
    xmake build shuttle_examples_sc
    xmake run shuttle_examples_sc

# real simulator (VCS): compile weave_pkg → shuttle_pkg → shuttle_demo and run simv,
# judged by the output "SHUTTLE CPP DEMO PASSED". Must use xmake run so the library
vcs:
    xmake f --backend=svdpi -y
    xmake run shuttle_examples_svdpi_vcs

# run the examples on both backends
test: sc vcs

# test suite (systemc backend): all cases in tests/cpp, local executable
#   expected last line "WEAVE TEST PASSED"; the xlang cases SKIP by design
test-sc:
    xmake f --backend=systemc -y
    xmake build shuttle_tests_sc
    xmake run shuttle_tests_sc

# test suite (VCS): all cases in tests/cpp + tests/sv, judged by "SHUTTLE TESTS PASSED"
test-vcs:
    xmake f --backend=svdpi -y
    xmake run shuttle_tests_svdpi_vcs

# run the whole test suite (cross-language included)
test-all: test-sc test-vcs

# clean
clean:
    xmake clean -a
