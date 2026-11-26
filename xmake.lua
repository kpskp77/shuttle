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
add_requires("weave", { configs = { shared = true, backend = get_config("backend") } })
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
      "examples/shuttle_demo_ref.sv",
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
    remove_files("tests/shuttle_tests_ref.cpp", "tests/shuttle_tests_xlang.cpp")
    -- the fatal payload dies on purpose and has its own target (judged with inverted expectation)
    remove_files("tests/shuttle_tests_fatal.cpp")
    add_packages("weave")
  end)

  -- Fatal-path payload: a self-blocked wait ends the run through weave::fatal, so this body cannot
  -- share a run with the suites (one process, one death). tests/expect_fatal.sh asserts the death
  -- instead of passing it to `just` as a failed run: the log must carry the "weave: fatal:"
  -- headline and never the pass marker.
  target("shuttle_tests_fatal_sc", function()
    set_kind("binary")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    add_files("tests/shuttle_tests_fatal.cpp", "tests/shuttle_tests_main.cpp")
  end)

  -- This run takes the per-topic chain (publish_serial); the VCS run below takes the
  -- per-subscriber one, so both flavors of the chain link the diagnosis compares are exercised.
  target("shuttle_tests_fatal_sc_run", function()
    set_kind("phony")
    set_default(false)
    add_deps("shuttle_tests_fatal_sc")
    on_run(function(target)
      os.execv("bash", {
        path.join(os.projectdir(), "tests/expect_fatal.sh"),
        "--expect",
        "on topic 'fatal.selfwait' can never start",
        target:dep("shuttle_tests_fatal_sc"):targetfile(),
      })
    end)
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
    -- the fatal payload dies on purpose and has its own targets (inverted expectation)
    remove_files("tests/shuttle_tests_fatal.cpp")
    add_defines("SHUTTLE_TESTS_XLANG")
    add_packages("weave")
  end)

  -- Fatal-path payloads: the payload library carries the C++ body, the two tops below drive the two
  -- halves of the path (weave_run for the C++ wait, a plain SV process for shuttle_c_wait_pub). A
  -- self-blocked wait ends the run, so both are judged with inverted expectation by run-eda.sh:
  -- the log must carry the "weave: fatal:" headline and never the pass marker.
  target("shuttle_tests_fatal_svdpi", function()
    set_kind("shared")
    set_default(false)
    set_warnings("all", "extra", "error")
    add_deps("shuttle")
    add_rpathdirs("$ORIGIN")
    add_files("tests/shuttle_tests_fatal.cpp")
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
        "tests/shuttle_tests_ref.sv",
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

  -- Fatal path, C++ half: weave_run starts the payload's WEAVE_TEST, whose self-blocked wait ends
  -- simv. This run takes the per-subscriber chain (publish_concurrent); the SystemC run takes the
  -- per-topic one.
  target("shuttle_tests_fatal_cpp_vcs", function()
    set_kind("phony")
    set_default(false)
    add_deps("shuttle_tests_fatal_svdpi")
    on_run(function(target)
      local root = os.projectdir()
      os.setenv("SHUTTLE_FATAL_CHAIN", "concurrent")
      os.execv("bash", {
        path.join(root, "tests/run-eda.sh"),
        "--expect-fatal",
        "--expect",
        "on topic 'fatal.selfwait' can never start",
        "--expect",
        "chain=concurrent",
        target:dep("shuttle_tests_fatal_svdpi"):targetfile(),
        path.join(root, "tests/shuttle_tests_fatal_cpp.sv"),
      })
    end)
  end)

  -- Fatal path, SV half: the SV process itself waits on a frame its own callback published.
  target("shuttle_tests_fatal_sv_vcs", function()
    set_kind("phony")
    set_default(false)
    add_deps("shuttle_tests_fatal_svdpi")
    on_run(function(target)
      local root = os.projectdir()
      os.execv("bash", {
        path.join(root, "tests/run-eda.sh"),
        "--expect-fatal",
        "--expect",
        "on topic 'fatal.sv.selfwait' can never start",
        target:dep("shuttle_tests_fatal_svdpi"):targetfile(),
        path.join(root, "tests/shuttle_tests_fixtures.sv"),
        path.join(root, "tests/shuttle_tests_fatal_sv.sv"),
      })
    end)
  end)
end
