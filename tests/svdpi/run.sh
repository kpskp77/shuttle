#!/usr/bin/env bash
# Real-simulator (Synopsys VCS) run of the shuttle SVDPI payload library.
#
# Usage:
#   tests/svdpi/run.sh <payload.so>
#
# Flow (same as ../weave/tests/svdpi/run.sh): locate vcs → compile weave_pkg.sv +
# shuttle_pkg.sv + examples/sv/shuttle_demo.sv → run simv with the payload via
# -sv_root/-sv_lib → judge by the output marker "SHUTTLE CPP DEMO PASSED" and
# the absence of $error lines.
#
# runtime environment (xmake run sets `LD_LIBRARY_PATH` for this target's dependencies) is
# enough for simv to find libshuttle.so → libweave.so and their simlog/spdlog dependencies;
# do NOT assemble LD_LIBRARY_PATH yourself here (it would override xmake's prepared search
# path and cause the load to fail instead).
# Skipped (exit 0) when vcs is not on PATH.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/../.." && pwd)
weave_root=$(cd -- "$root/../weave" && pwd)
lib=${1:-}
# Optional overrides (only for the framework's own compatibility probe): the SV package and
sv_pkg=${SHUTTLE_SV_PKG:-$root/sv/shuttle_pkg.sv}
sv_tb=${SHUTTLE_SV_TB:-$root/examples/sv/shuttle_demo.sv}
# The success marker can be overridden: the test suite (tests/sv/shuttle_tests.sv)
# prints "SHUTTLE TESTS PASSED".
vcs=""
if [[ -n ${VCS_HOME:-} && -x $VCS_HOME/bin/vcs ]]; then
  vcs=$VCS_HOME/bin/vcs
else
  vcs=$(command -v vcs || true)
fi
if [[ -z $vcs ]]; then
  echo "[svdpi-test] skipped: no vcs on PATH"
  exit 0
fi
if [[ -z $lib || ! -f $lib ]]; then
  echo "[svdpi-test] usage: run.sh <payload.so>" >&2
  exit 2
fi
lib=$(cd -- "$(dirname -- "$lib")" && pwd)/$(basename -- "$lib")
libdir=$(dirname -- "$lib")
base=$(basename -- "$lib" .so)

marker=${SHUTTLE_PASS_MARKER:-SHUTTLE CPP DEMO PASSED}

work="$libdir/svdpi_work_$base"
rm -rf -- "$work"
mkdir -p -- "$work"

echo "[svdpi-test] compiling $base (vcs: $vcs)"
if ! (cd "$work" &&
  timeout 900 "$vcs" -full64 -sverilog -LDFLAGS "-Wl,--export-dynamic" \
    "$weave_root/sv/weave_pkg.sv" "$sv_pkg" "$sv_tb" >compile.log 2>&1 </dev/null); then
  echo "[svdpi-test] FAIL: vcs compile failed, see $work/compile.log" >&2
  tail -n 20 "$work/compile.log" >&2
  exit 2
fi

echo "[svdpi-test] running simv (-sv_lib $base)"
if ! (cd "$work" && timeout 300 ./simv -sv_root "$libdir" -sv_lib "$base" \
    >run.log 2>&1 </dev/null); then
  echo "[svdpi-test] FAIL: simv exited non-zero, see $work/run.log" >&2
  tail -n 20 "$work/run.log" >&2
  exit 2
fi

# Simulator error shapes: VCS's "Error-[CODE]" and $error's "^Error".
# A bare "Error" must not be used for matching — the example output itself legitimately
# contains normal text such as CapabilityError.
if grep -qE 'Error-\[|^Error' "$work/run.log"; then
  echo "[svdpi-test] FAIL: \$error reported, see $work/run.log" >&2
  grep -E 'Error-\[|^Error' "$work/run.log" | head -n 5 >&2
  exit 2
fi
if ! grep -qF "$marker" "$work/run.log"; then
  echo "[svdpi-test] FAIL: pass marker missing, see $work/run.log" >&2
  tail -n 20 "$work/run.log" >&2
  exit 2
fi

echo "[svdpi-test] PASS"
