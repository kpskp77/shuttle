-- 用户侧 API 示范（examples/）。构建方式与真实用户一致：只编译 examples/cpp 下的源文件，
-- 只使用 include/shuttle 的公开头文件，经 add_deps("shuttle") 链接 libshuttle.so
-- （间接带入 libweave.so），不内联任何内部源文件。
--
--   shuttle_examples_sc    systemc → 可执行文件，本地运行（入口 cpp/sc_main.cpp）
--   shuttle_examples_svdpi svdpi   → 共享库，随仿真器经 -sv_lib 加载，
--                                    由 sv/shuttle_demo.sv 中的 weave_run 启动

target("shuttle_examples_sc", function()
  set_kind("binary")
  set_default(false)
  set_warnings("all", "extra", "error")
  add_deps("shuttle")
  add_rpathdirs("$ORIGIN")
  add_files("cpp/*.cpp")
  add_packages("systemc")
end)

target("shuttle_examples_svdpi", function()
  set_kind("shared")
  set_default(false)
  set_warnings("all", "extra", "error")
  add_deps("shuttle")
  add_rpathdirs("$ORIGIN")
  add_files("cpp/basic_demo.cpp", "cpp/trigger_demo.cpp")
end)
