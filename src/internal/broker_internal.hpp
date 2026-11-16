#pragma once

// 框架内部接口：后端（src/backend/*）与 FFI 层调用的 broker 内部入口。
// 只被 src/ 包含，不属于安装 API。

#include <cstdint>
#include <string>
#include <functional>
#include <vector>

#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>

namespace shuttle::detail {

// 每进程独立的最近错误信息（svdpi 桥的 shuttle_c_last_error 数据源）。
// 任何返回错误码的内部入口都经这里记录详细原因。
void set_last_error(std::string msg);
const std::string& last_error();

// ── 外部接入门面（错误经 set_last_error + 返回码/异常报告）────────────
//
// `ext_*` = 外部调用方进来的动作（字节帧发布、外部订阅者注册）。
// `pub_*`（首参是 pub_id）= 按发布 id 读句柄的通用查询/等待，与具体后端无关。

// 外部发布（载荷是"已编码帧" = 字节流 + type_tag；三种调度与 typed 路径一一对应）。
// 失败一律抛异常（shuttle_c_* 包装层捕获并转成 set_last_error + 返回码）。
Publication ext_pub_now(const std::string& topic, const std::string& tag,
                       const std::vector<std::byte>& frame, NowPolicy policy);
Publication ext_pub_serial(const std::string& topic, const std::string& tag,
                          const std::vector<std::byte>& frame, Trigger trig);
Publication ext_pub_concurrent(const std::string& topic, const std::string& tag,
                              const std::vector<std::byte>& frame, Trigger trig);

// 取句柄对应的 pub_id（无效句柄返回 0）——DPI 层用它把 Publication 换成 int。
int pub_id(const Publication& p);

// 外部订阅注册：handle 由后端分配（svdpi = SV 注册表编号），broker 原样记录、dispatch 时带回。
// 返回 0 成功，非 0 失败（类型绑定冲突等）。
int ext_sub(std::string topic_pattern, int caps, std::string type_tag, int handle);
void ext_unsub(int handle);

// Publication 完成跟踪（pub_id 由发布时分配）
bool pub_done(int pub_id);
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

// retained（与 C++ 路径共用同一个入口，声明见 detail/abi.hpp）

// ── Event 注册表 ─────────────────────────────────────────────────────────
// event_wait_id：TRIG_EVENT 的等待入口（必须在 weave task 上下文调用）。
void event_wait_id(int ev_id);
// SV 侧 shuttle_event 的底层对象由注册表持有（close 前持续有效）。
int event_registry_create();
void event_registry_set(int ev_id);
void event_registry_destroy(int ev_id);

}  // namespace shuttle::detail
