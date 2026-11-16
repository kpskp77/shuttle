#pragma once

// svdpi 后端内部头：只被 `src/backend/svdpi/` 的 TU 包含，不对外。
//
// 这里放的是"svdpi 这个后端自己怎么实现"的东西：
//   - 包级 export 的作用域修正（SvScopeGuard）；
//   - C++→SV 的帧字节拉取通道（FrameContext + frame_len/frame_qword）；
//   - 外部门面的声明（`shuttle::detail::ext_*` / `pub_*` / 事件注册表 /
//     set_last_error；实现在 facade.cpp）。
//
// 注意：svdpi 是本仓库唯一带 FFI 通道的后端（且永不会有第二个），所以外部门面
// 声明也住这里——它对 broker 内部（broker/broker.hpp）的依赖是显式接受、永久的不对称
// （systemc 后端仍只经 `backend/backend.hpp` 窄口认识 broker）。
// 协议定义见 docs/dpi_protocol.md。

#include <svdpi.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
#include <string>
#include <vector>

// 包名可用 -DSHUTTLE_SV_PACKAGE= / -DSHUTTLE_WEAVE_SV_PACKAGE= 覆盖。
#ifndef SHUTTLE_SV_PACKAGE
#define SHUTTLE_SV_PACKAGE "shuttle_pkg"
#endif
// weave 的 SV 包名（其 export 定义于此；shuttle 在 import 入口处把作用域切到它，
// 使 C++ 侧调用 weave 原语（sleep/create_event/spawn…）时的 export 可见）。
#ifndef SHUTTLE_WEAVE_SV_PACKAGE
#define SHUTTLE_WEAVE_SV_PACKAGE "weave_pkg"
#endif

namespace shuttle::detail {
namespace backend {

// 按包名取作用域（带缓存；仿真线程单活跃，无需加锁）。
inline svScope scope_for(const char* pkg) {
  static const char* cached_pkg = nullptr;
  static svScope cached = nullptr;
  if (cached_pkg != pkg) {
    cached = svGetScopeFromName(pkg);
    cached_pkg = pkg;
  }
  return cached;
}

// ── 包级 export 的作用域修正 ────────────────────────────────────────────────
// VCS 2018.09 实测（../dpi_probe 最小复现）：在同一个 SV 进程里调用过返回 string
// 的 import（shuttle_c_last_error）之后，再调用包级 export 会报
//   Error-[DPI-DXFNF] DPI export function not found
// 原因是 import 调用改变了当前 svScope，导致 export 在当前作用域内不可见。
// 因此在每次进入 export 前显式把作用域切到 shuttle 的 SV 包，返回后还原，
// 以免影响其它库（如 weave_pkg）的 export 解析。
class SvScopeGuard {
public:
  explicit SvScopeGuard(const char* pkg = SHUTTLE_SV_PACKAGE) : prev_(svGetScope()) {
    if (svScope s = scope_for(pkg)) svSetScope(s);
  }
  ~SvScopeGuard() {
    if (prev_) svSetScope(prev_);
  }
  SvScopeGuard(const SvScopeGuard&) = delete;
  SvScopeGuard& operator=(const SvScopeGuard&) = delete;

private:
  svScope prev_;
};

// 当前待拉取帧的字节流：C++ 调 export 前设置，SV 在 export 体内经
// `shuttle_c_frame_len` / `shuttle_c_frame_qword` 分块拉取。全局变量即可见，
// 因为"任一时刻只有一个进程执行 C++ 侧记账代码"（AGENTS.md 不变量 1）——
// 仿真没有多线程，不需要 thread_local。
inline const std::vector<std::byte>* g_frame = nullptr;

class FrameContext {
public:
  explicit FrameContext(const std::vector<std::byte>& bytes) : prev_(g_frame) { g_frame = &bytes; }
  ~FrameContext() { g_frame = prev_; }
  FrameContext(const FrameContext&) = delete;
  FrameContext& operator=(const FrameContext&) = delete;

private:
  const std::vector<std::byte>* prev_;
};

// 帧字节流的分块视图（SV 侧只能拉标量，所以按 8 字节小端分块）。
int frame_len();
std::uint64_t frame_qword(int index);

}  // namespace backend

// ── 外部门面（实现在 facade.cpp；命名空间保持 shuttle::detail，
//    供 dpi.cpp 以 detail::* 直接调用）────────────────────────────────

// 每进程独立的最近错误信息（shuttle_c_last_error 数据源）。
// 任何返回错误码的内部入口都经这里记录详细原因。
void set_last_error(std::string msg);
const std::string& last_error();

// `ext_*` = 外部调用方进来的动作（字节帧发布、外部订阅者注册；错误经异常上抛，
// 由 DPI 包装层捕获转成 set_last_error + 返回码）。
// `pub_*`（首参是 pub_id）= 按发布 id 读句柄的查询/等待。
Publication ext_pub_now(const std::string& topic, const std::string& tag,
                        const std::vector<std::byte>& frame, NowPolicy policy);
Publication ext_pub_serial(const std::string& topic, const std::string& tag,
                           const std::vector<std::byte>& frame, Trigger trig);
Publication ext_pub_concurrent(const std::string& topic, const std::string& tag,
                               const std::vector<std::byte>& frame, Trigger trig);

// 取句柄对应的 pub_id（无效句柄返回 0）——DPI 层用它把 Publication 换成 int。
int pub_id(const Publication& p);

// 外部订阅注册：handle = SV 注册表编号，broker 原样记录、dispatch 时带回。
// 返回 0 成功，非 0 失败（类型绑定冲突等）。
int ext_sub(std::string topic_pattern, int caps, std::string type_tag, int handle);
void ext_unsub(int handle);

// Publication 完成跟踪（pub_id 由发布时分配）
bool pub_done(int pub_id);

// 发布计数查询的种类：值与线上 kind 0-4 一一对应（见 docs/dpi_protocol.md 常量表，
// 跨 DPI 不变）。用枚举让编译器检查 switch 漏项。
enum class PubStatKind { expected = 0, finished = 1, skipped = 2, deferred = 3, retired = 4 };

// 发布计数查询：kind 0=目标数 1=已完成 2=策略跳过 3=deferred 4=退订跳过；未知 pub_id 返回 -1
long long pub_stat(int pub_id, int kind);
std::string pub_error(int pub_id);
// 阻塞等待（必须在 weave task / 仿真进程上下文中调用）。
// status 输出：0=完成（不代表无订阅者失败，需再查 pub_state） 1=超时
// 2=有订阅者失败（详情见 pub_error）或内部错误（详情见 last_error）。
void pub_wait(int pub_id, std::int64_t timeout_fs, int* status);

// 发布完成状态：0=进行中 1=已完成且无失败 2=已完成但有失败 -1=未知 pub_id
int pub_state(int pub_id);
// 注册"完成瞬间"回调（仅在 pub 未完成时有意义；须零阻塞）。
// 调用前后不得有挂起点，故 pub_state()==0 判定与注册之间不会漏掉完成事件。
void pub_on_complete(int pub_id, std::function<void()> fn);

// ── Event 注册表（SV shuttle_event / TRIG_EVENT；语义上 SV 专属，故住本目录）──
// event_wait_id：TRIG_EVENT 的等待入口（必须在 weave task 上下文调用）。
void event_wait_id(int ev_id);
// SV 侧 shuttle_event 的底层对象由注册表持有（close 前持续有效）。
int event_registry_create();
void event_registry_set(int ev_id);
void event_registry_destroy(int ev_id);

}  // namespace shuttle::detail
