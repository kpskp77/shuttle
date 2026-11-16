# shuttle 开发常用动作（just）。部分 recipe 会先把 xmake 切到所需后端（见各自注释）。

# 默认：按当前后端构建库与示例
default: build

# 构建库 + 全部示例（当前 xmake 配置的后端）
build:
    xmake build

# svdpi 后端：构建共享库与 payload .so
svdpi:
    xmake f --backend=svdpi -y
    xmake build shuttle_examples_svdpi

# systemc 后端：本地可执行，预期末行 "WEAVE TEST PASSED"
sc:
    xmake f --backend=systemc -y
    xmake build shuttle_examples_sc
    xmake run shuttle_examples_sc

# 真实仿真器（VCS）：编译 weave_pkg → shuttle_pkg → shuttle_demo 并跑 simv，
# 按输出 "SHUTTLE CPP DEMO PASSED" 判定。必须用 xmake run 以准备库搜索路径。
vcs:
    xmake f --backend=svdpi -y
    xmake run shuttle_examples_svdpi_vcs

# 两个后端的示例全过一遍
test: sc vcs

# 测试套件（systemc 后端）：tests/cpp 全部用例，本地可执行
#   预期末行 "WEAVE TEST PASSED"，其中 xlang 用例按设计 SKIP
test-sc:
    xmake f --backend=systemc -y
    xmake build shuttle_tests_sc
    xmake run shuttle_tests_sc

# 测试套件（VCS）：tests/cpp + tests/sv 全部用例，按 "SHUTTLE TESTS PASSED" 判定
test-vcs:
    xmake f --backend=svdpi -y
    xmake run shuttle_tests_svdpi_vcs

# 测试套件全过一遍（含跨语言）
test-all: test-sc test-vcs

# 格式化（工具缺失时跳过）：C++ / SV / Lua 配置各自独立
fmt:
    -bash -c 'command -v clang-format >/dev/null && clang-format -i $$(git ls-files "include/**/*.hpp" "src/*.cpp" "src/internal/**/*.hpp" "src/backend/**/*.{cpp,hpp}" "examples/cpp/*.cpp" "tests/cpp/*.cpp" "tests/cpp/*.hpp" 2>/dev/null || echo include/shuttle/*.hpp src/*.cpp examples/cpp/*.cpp tests/cpp/*.cpp)'
    -bash -c 'command -v verible-verilog-format >/dev/null && verible-verilog-format -i sv/shuttle_pkg.sv examples/sv/shuttle_demo.sv tests/sv/shuttle_tests.sv'

# 清理
clean:
    xmake clean -a
