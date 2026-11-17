-- Test suite (tests/). Like examples/, it only links via the public headers +
-- add_deps("shuttle"); the difference is that these cases assert behavior instead of
--
--   shuttle_tests_sc         systemc → executable, run locally (entry cpp/sc_main.cpp)
--   shuttle_tests_svdpi      svdpi   → shared library, loaded by the simulator via -sv_lib,
--                                       started by weave_run in sv/shuttle_tests.sv
--   shuttle_tests_svdpi_vcs  phony   → compile weave_pkg + shuttle_pkg + the SV test top,
--                                       run simv and judge by "SHUTTLE TESTS PASSED"
--
-- One xmake configuration can only have one backend, so one of the two is chosen by

local backend = get_config("backend")

if backend == "systemc" then
  target("shuttle_tests_sc", function()
    set_kind("binary")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    add_includedirs("cpp")
    -- includes sc_main.cpp (the systemc entry); excludes xlang_tests, which calls the SV
    -- bridge entries directly and is compiled only in the svdpi backend target
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
    -- no sc_main.cpp: the svdpi backend's entry lives on the simulator side
    add_files("cpp/codec_tests.cpp", "cpp/broker_tests.cpp", "cpp/publish_tests.cpp",
              "cpp/trigger_tests.cpp", "cpp/mailbox_tests.cpp", "cpp/xlang_tests.cpp")
    add_packages("weave")
  end)

  -- VCS run (phony): must use xmake run so the library search path is prepared;
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
