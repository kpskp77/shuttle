#pragma once

// 框架内部接口：broker 核心（broker.cpp，后端无关）与后端桥（svdpi.cpp / systemc.cpp）
// 之间的边界。broker 通过这些函数把帧投递到 SV 订阅者、读取仿真时间、
// 管理 shuttle::Event 的句柄注册表。
//
// 只被 src/ 包含，不属于安装 API。

#include <cstdint>
#include <string>
#include <vector>

namespace shuttle {
namespace detail {
namespace sv {

// 当前后端是否有 SV 桥（svdpi 后端恒 true；systemc 后端恒 false，
// 此时以下函数抛 std::logic_error）。
bool available();

// `serial`/`concurrent` 的 SV 投递：必须在 weave task（仿真进程）上下文中调用，
// SV 侧经 exported task shuttle_sv_dispatch 进入用户回调。
void dispatch(int sv_sid, const std::string& topic, const std::string& type_tag,
              std::uint64_t seq, std::int64_t time_fs, const std::vector<std::byte>& payload);

// `publish_now` 的 SV 投递：在发布者进程上下文中调用，必须零阻塞；
// SV 侧经 exported function shuttle_sv_dispatch_nb 进入用户回调。
void dispatch_nb(int sv_sid, const std::string& topic, const std::string& type_tag,
                 std::uint64_t seq, std::int64_t time_fs, const std::vector<std::byte>& payload);

// 当前仿真时间（飞秒）
std::int64_t now_fs();

// shuttle::Event 的句柄注册表（TRIG_EVENT 与 SV 侧 shuttle_event 共用）
int event_create();
void event_set(int ev_id);
void event_destroy(int ev_id);
// 在 weave task 上下文中等待事件（阻塞原语）
void event_wait(int ev_id);

// 当前 dispatch 帧的字节流访问（仅 svdpi 桥内部使用；
// SV 在 shuttle_sv_dispatch 体内经 shuttle_c_frame_* DPI import 拉取）。
int frame_len();
std::uint64_t frame_qword(int index);  // 8 字节小端块

// SV 注册表发现目标已退订（retired）时，会在 dispatch 期间经 DPI import
// `shuttle_c_sub_retired` 回告一次；Broker::deliver 在 export 返回后取走该标记，把这次
// 投递记成 filtered（进了 dropped，但不是 delivered，也不报错）。
void note_retired_delivery();
bool take_retired_delivery();

}  // namespace sv
}  // namespace detail
}  // namespace shuttle
