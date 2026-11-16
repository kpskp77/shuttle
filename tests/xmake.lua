-- 测试套件（tests/）。与 examples/ 一样只经公开头文件 + add_deps("shuttle") 链接，
-- 区别是这里的用例只断言行为、不做演示：
--
--   shuttle_tests_sc         systemc → 可执行文件，本地运行（入口 cpp/sc_main.cpp）
--   shuttle_tests_svdpi      svdpi   → 共享库，随仿真器经 -sv_lib 加载，
--                                       由 sv/shuttle_tests.sv 的 weave_run 启动
--   shuttle_tests_svdpi_vcs  phony   → 编译 weave_pkg + shuttle_pkg + SV 测试顶层，
--                                       跑 simv 并按 "SHUTTLE TESTS PASSED" 判定
--
-- 同一份 xmake 配置只能有一个后端，故按 get_config("backend") 二选一。

local backend = get_config("backend")

if backend == "systemc" then
  target("shuttle_tests_sc", function()
    set_kind("binary")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    add_includedirs("cpp")
    -- 含 sc_main.cpp（systemc 入口）；排除 xlang_tests：它直接调 SV 桥入口，
    -- 只在 svdpi 后端的 target 里编译
    add_files("cpp/*.cpp")
    remove_files("cpp/xlang_tests.cpp")
    add_packages("weave")
  end)
else
  target("shuttle_tests_svdpi", function()
    set_kind("shared")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    add_includedirs("cpp")
    -- 不含 sc_main.cpp：svdpi 后端的入口在仿真器一侧
    add_files("cpp/codec_tests.cpp", "cpp/broker_tests.cpp", "cpp/publish_tests.cpp",
              "cpp/trigger_tests.cpp", "cpp/xlang_tests.cpp")
    add_packages("weave")
  end)

  -- VCS 运行（phony）：必须用 xmake run 以准备库搜索路径，见 tests/svdpi/run.sh
  target("shuttle_tests_svdpi_vcs", function()
    set_kind("phony")
    set_default(false)
    add_deps("shuttle_tests_svdpi")
    on_run(function(target)
      local root = os.projectdir()
      os.setenv("SHUTTLE_SV_TB", path.join(root, "tests/sv/shuttle_tests.sv"))
      os.setenv("SHUTTLE_PASS_MARKER", "SHUTTLE TESTS PASSED")
      os.execv("bash", {
        path.join(root, "tests/svdpi/run.sh"),
        target:dep("shuttle_tests_svdpi"):targetfile(),
      })
    end)
  end)
end
