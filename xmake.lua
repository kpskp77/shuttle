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
if get_config("backend") == "systemc" then
  add_requires("systemc", { configs = { shared = true } })
else
  add_requires("svdpi")
end

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
  add_packages("weave", { public = true })
end)

-- VCS run (phony): compile weave_pkg + shuttle_pkg + the SV demo,
-- run simv with shuttle_examples_svdpi as the payload, judge by the output marker.
target("shuttle_examples_svdpi_vcs", function()
  set_kind("phony")
  set_default(false)
  add_deps("shuttle_examples_svdpi")
  on_run(function(target)
    os.execv("bash", {
      path.join(os.projectdir(), "tests/svdpi/run.sh"),
      target:dep("shuttle_examples_svdpi"):targetfile(),
    })
  end)
end)


-- Compatibility probe (optional): run VCS once with the given SV package/top —
--   SHUTTLE_SV_PKG=<pkg.sv> SHUTTLE_SV_TB=<tb.sv> SHUTTLE_PROBE_LIB=<lib.so> --     xmake run shuttle_probe_vcs
target("shuttle_probe_vcs", function()
  set_kind("phony")
  set_default(false)
  add_deps("shuttle_examples_svdpi")
  on_run(function(target)
    local lib = os.getenv("SHUTTLE_PROBE_LIB") or target:dep("shuttle_examples_svdpi"):targetfile()
    os.execv("bash", {path.join(os.projectdir(), "tests/svdpi/run.sh"), lib})
  end)
end)

includes("examples")
includes("tests")
