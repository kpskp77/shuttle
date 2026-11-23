set_project("shuttle")
set_version("0.1.0")

add_rules("mode.debug", "mode.release")
set_languages("c++17")

option("backend", function()
  set_showmenu(true)
  set_default("svdpi")
  set_values("svdpi", "systemc")
  set_description("Simulation backend")
end)

-- Add dependencies
add_requires("weave >=1.2.0", { configs = { shared = true, backend = get_config("backend") } })
add_requires("simlog", { configs = { shared = true, backend = get_config("backend") } })

-- ── Library ──────────────────────────────────────────────────────────────────────────────
target("shuttle", function()
  set_kind("shared")
  set_warnings("all", "extra", "error")
  add_headerfiles("include/(shuttle/**.hpp)")
  add_includedirs("include", { public = true })
  -- private: src/ is the include root for internal headers; broker/ and detail/ are not
  -- installed; the backend narrow port stays in backend/. The svdpi backend directory
  add_includedirs("src", { public = false })
  -- public API/handles + broker core + current backend. Core and backend are enumerated
  -- separately so a recursive rule never compiles both backends in; switching backends
  add_files("src/*.cpp")
  add_files("src/broker/*.cpp")
  add_files("src/backend/" .. (get_config("backend") or "svdpi") .. "/**.cpp")
  add_rpathdirs("$ORIGIN")
  add_packages("weave", "simlog", { public = true })
end)

-- ── Examples ───────────────────────────────────────────────────────────────────────────────
target("shuttle_examples_sc", function()
  set_kind("binary")
  set_default(false)
  set_warnings("all", "extra", "error")
  add_deps("shuttle")
  add_rpathdirs("$ORIGIN")
  add_files("examples/*.cpp", "tests/shuttle_tests_main.cpp")
end)

target("shuttle_examples_svdpi", function()
  set_kind("shared")
  set_default(false)
  set_warnings("all", "extra", "error")
  add_deps("shuttle")
  add_rpathdirs("$ORIGIN")
  add_files("examples/*.cpp")
end)

-- VCS run (phony): compile weave_pkg + shuttle_pkg + the SV demo modules in order,
-- run simv with shuttle_examples_svdpi as the payload, judge by the output marker.
target("shuttle_examples_svdpi_vcs", function()
  set_kind("phony")
  set_default(false)
  add_deps("shuttle_examples_svdpi")
  on_run(function(target)
    local root = os.projectdir()
    -- compile order: shared message package, one file per subject, the ordered top last
    local sv = {
      "examples/shuttle_demo_msgs.sv",
      "examples/shuttle_demo_publish.sv",
      "examples/shuttle_demo_nested.sv",
      "examples/shuttle_demo_inherit.sv",
      "examples/shuttle_demo_namespace.sv",
      "examples/shuttle_demo_mailbox.sv",
      "examples/shuttle_demo_requester.sv",
      "examples/shuttle_demo.sv",
    }
    os.setenv("SHUTTLE_PASS_MARKER", "SHUTTLE DEMO PASSED")
    local argv =
      { path.join(root, "tests/run-eda.sh"), target:dep("shuttle_examples_svdpi"):targetfile() }
    for _, f in ipairs(sv) do
      table.insert(argv, path.join(root, f))
    end
    os.execv("bash", argv)
  end)
end)

-- ── Tests ───────────────────────────────────────────────────────────────────────────────
if get_config("backend") == "systemc" then
  target("shuttle_tests_sc", function()
    set_kind("binary")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    -- includes shuttle_tests_main.cpp (the systemc entry) and the top shuttle_tests.cpp;
    -- excludes the cross-language suite, which needs the SV peer and is compiled only in the
    -- svdpi backend target (the top leaves it out without SHUTTLE_TESTS_XLANG)
    add_files("tests/*.cpp")
    remove_files("tests/shuttle_tests_xlang.cpp")
    add_packages("weave")
  end)
else
  target("shuttle_tests_svdpi", function()
    set_kind("shared")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    -- no shuttle_tests_main.cpp: the svdpi backend's entry lives on the simulator side; the top
    -- shuttle_tests.cpp compiles the cross-language suite because of SHUTTLE_TESTS_XLANG
    add_files("tests/*.cpp")
    remove_files("tests/shuttle_tests_main.cpp")
    add_defines("SHUTTLE_TESTS_XLANG")
    add_packages("weave")
  end)

  -- VCS run (phony): must use xmake run so the library search path is prepared;
  target("shuttle_tests_svdpi_vcs", function()
    set_kind("phony")
    set_default(false)
    add_deps("shuttle_tests_svdpi")
    on_run(function(target)
      local root = os.projectdir()
      -- compile order: shared fixtures, the case runner, one file per suite, the top last
      local sv = {
        "tests/shuttle_tests_fixtures.sv",
        "tests/shuttle_tests_runner.sv",
        "tests/shuttle_tests_publish.sv",
        "tests/shuttle_tests_broker.sv",
        "tests/shuttle_tests_trigger.sv",
        "tests/shuttle_tests_mailbox.sv",
        "tests/shuttle_tests_xlang.sv",
        "tests/shuttle_tests_requester.sv",
        "tests/shuttle_tests.sv",
      }
      os.setenv("SHUTTLE_PASS_MARKER", "SHUTTLE TESTS PASSED")
      local argv =
        { path.join(root, "tests/run-eda.sh"), target:dep("shuttle_tests_svdpi"):targetfile() }
      for _, f in ipairs(sv) do
        table.insert(argv, path.join(root, f))
      end
      os.execv("bash", argv)
    end)
  end)
end
