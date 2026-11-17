-- User-side API demos (examples/). Built exactly like a real user: only the sources under
-- examples/cpp are compiled, only the public headers in include/shuttle are used, and
-- libshuttle.so is linked via add_deps("shuttle") (which brings in libweave.so);
--
--   shuttle_examples_sc    systemc → executable, run locally (entry cpp/sc_main.cpp)
--   shuttle_examples_svdpi svdpi   → shared library, loaded by the simulator via -sv_lib,
--                                    started by weave_run in sv/shuttle_demo.sv

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
  add_files("cpp/basic_demo.cpp", "cpp/trigger_demo.cpp", "cpp/mailbox_demo.cpp",
            "cpp/request_response_demo.cpp")
end)
