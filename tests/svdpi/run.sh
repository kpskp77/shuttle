#!/usr/bin/env bash
# Real-simulator (Synopsys VCS) run of the shuttle SVDPI payload library.
#
# Usage:
#   tests/svdpi/run.sh <payload.so>
#
# Flow (同 ../weave/tests/svdpi/run.sh): locate vcs → compile weave_pkg.sv +
# shuttle_pkg.sv + examples/sv/shuttle_demo.sv → run simv with the payload via
# -sv_root/-sv_lib → judge by the output marker "SHUTTLE CPP DEMO PASSED" and
# the absence of $error lines.
#
# 运行时环境（xmake run 会为本 target 的依赖设置 `LD_LIBRARY_PATH`）即足以让
# simv 找到 libshuttle.so → libweave.so 及其 simlog/spdlog 依赖；不要在这里
# 自己拼 LD_LIBRARY_PATH（会覆盖 xmake 准备好的搜索路径，反而加载失败）。
#
# Skipped (exit 0) when vcs is not on PATH.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/../.." && pwd)
weave_root=$(cd -- "$root/../weave" && pwd)
lib=${1:-}
# 可选覆盖（仅供框架自身的兼容性探针使用）：待编译的 SV 包与测试顶层
sv_pkg=${SHUTTLE_SV_PKG:-$root/sv/shuttle_pkg.sv}
sv_tb=${SHUTTLE_SV_TB:-$root/examples/sv/shuttle_demo.sv}
#
# 成功标记可覆盖：测试套件（tests/sv/shuttle_tests.sv）打印的是 "SHUTTLE TESTS PASSED"。

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

# 成功标记（测试套件用环境变量 SHUTTLE_PASS_MARKER="SHUTTLE TESTS PASSED" 覆盖）
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

# 仿真器报错形态：VCS 的 "Error-[CODE]" 与开关 $error 的 "^Error"。
# 不能用裸 "Error" 匹配——示例输出里本身就含 CapabilityError 之类的正常文本。
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
