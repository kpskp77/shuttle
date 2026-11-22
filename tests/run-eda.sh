#!/usr/bin/env bash
# Real-simulator (Synopsys VCS) run of a shuttle SVDPI payload library.
#
# Usage:
#   tests/run-eda.sh <payload.so> [<testbench.sv>...]
#
# The calling xmake target picks what to compile and what proves a pass:
#   <testbench.sv>...    SV sources, compiled in the given order (a top instantiated last)
#   SHUTTLE_SV_TB        SV testbench when no file argument is given (default: examples/shuttle_demo.sv)
#   SHUTTLE_PASS_MARKER  output marker the run must print (default: SHUTTLE DEMO PASSED)
#   SHUTTLE_SV_PKG       shuttle package (default: sv/shuttle_pkg.sv)
#
# The runtime environment (xmake run sets `LD_LIBRARY_PATH` for this target's dependencies) is
# enough for simv to find libshuttle.so → libweave.so and their simlog/spdlog dependencies;
# do NOT assemble LD_LIBRARY_PATH yourself here (it would override xmake's prepared search
# path and cause the load to fail instead).
# Skipped (exit 0) when vcs is not on PATH.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/.." && pwd)
weave_root=$(cd -- "$root/../weave" && pwd)
lib=${1:-}
sv_pkg=${SHUTTLE_SV_PKG:-$root/sv/shuttle_pkg.sv}
sv_tbs=()
if [[ $# -gt 1 ]]; then
  shift
  sv_tbs=("$@")
fi
if [[ ${#sv_tbs[@]} -eq 0 ]]; then
  sv_tbs=("${SHUTTLE_SV_TB:-$root/examples/shuttle_demo.sv}")
fi
marker=${SHUTTLE_PASS_MARKER:-SHUTTLE DEMO PASSED}
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
  echo "[svdpi-test] usage: run-eda.sh <payload.so> [<testbench.sv>...]" >&2
  exit 2
fi
lib=$(cd -- "$(dirname -- "$lib")" && pwd)/$(basename -- "$lib")
libdir=$(dirname -- "$lib")
base=$(basename -- "$lib" .so)
for tb in "${sv_tbs[@]}"; do
  if [[ ! -f $tb ]]; then
    echo "[svdpi-test] FAIL: missing SV source $tb" >&2
    exit 2
  fi
done

work="$libdir/svdpi_work_$base"
rm -rf -- "$work"
mkdir -p -- "$work"

echo "[svdpi-test] compiling $base (vcs: $vcs)"
if ! (cd "$work" &&
  timeout 900 "$vcs" -full64 -sverilog -LDFLAGS "-Wl,--export-dynamic" \
    +incdir+"$root/sv" \
    "$weave_root/sv/weave_pkg.sv" "$sv_pkg" "${sv_tbs[@]}" >compile.log 2>&1 </dev/null); then
  echo "[svdpi-test] FAIL: vcs compile failed, see $work/compile.log" >&2
  tail -n 20 "$work/compile.log" >&2
  exit 2
fi

echo "[svdpi-test] running simv (-sv_lib $base)"
simv_status=0
(cd "$work" && timeout 300 ./simv -sv_root "$libdir" -sv_lib "$base" \
    >run.log 2>&1 </dev/null) || simv_status=$?
cat "$work/run.log"

if [[ $simv_status -ne 0 ]]; then
  echo "[svdpi-test] FAIL: simv exited non-zero, see $work/run.log" >&2
  exit 1
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
