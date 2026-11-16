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
  add_files("src/**.cpp")
  add_rpathdirs("$ORIGIN")
  add_packages("weave", { public = true })
  if get_config("backend") == "systemc" then
    add_defines("SHUTTLE_BACKEND_SYSTEMC", { public = true })
  else
    add_defines("SHUTTLE_BACKEND_SVDPI", { public = true })
  end
end)

-- VCS 运行（phony）：编译 weave_pkg + shuttle_pkg + SV demo，
-- 以 shuttle_examples_svdpi 为 payload 跑 simv，按输出标记判定。
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


-- 兼容性探针（可选）：用指定的 SV 包/顶层跑一次 VCS —— 定位仿真器兼容性用。
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
