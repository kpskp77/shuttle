#pragma once

// 框架内部接口：broker 核心（`src/broker.cpp`，后端无关）与仿真后端
// （`src/backend/<backend>/`）之间的**唯一**依赖面。broker 只从这里认识后端。
//
// 后端自己的实现细节——帧字节拉取通道、SV 事件注册表、DPI-C 导出面、作用域修正——
// 全部留在 `src/backend/<backend>/` 里，不进这个头。加一个后端 = 加一个目录，
// 不改 broker，也不改这个头（除非新后端连"投递 + 时间"这两件事都给不出）。
//
// 当前实现：
//   src/backend/svdpi/    bridge.cpp（本接口）+ dpi.cpp（DPI-C 导出面）
//   src/backend/systemc/  bridge.cpp（退化：无外部订阅者通道）

#include <cstdint>
#include <string>
#include <vector>

namespace shuttle::detail {
namespace backend {

// 当前构建是否带外部（SV）订阅者通道：svdpi = true，systemc = false。
bool available();

// `serial` / `concurrent` 的投递：在 weave task（仿真进程）上下文中调用；
// 后端负责在外部进程里执行用户回调。`handle` 是该订阅者注册时拿到的后端侧编号，
// broker 只透传、不解释。
void dispatch(int handle, const std::string& topic, const std::string& type_tag,
              std::uint64_t seq, std::int64_t time_fs, const std::vector<std::byte>& payload);

// `publish_now` 的投递：在发布者进程上下文中调用，**必须零阻塞**
// （import function 的调用链里不能调 export task，见 AGENTS.md 不变量 3）。
void dispatch_nb(int handle, const std::string& topic, const std::string& type_tag,
                 std::uint64_t seq, std::int64_t time_fs,
                 const std::vector<std::byte>& payload);

// 当前仿真时间（飞秒），用作 `Meta::time_fs`。
std::int64_t now_fs();

// 退订回告通道：后端在 dispatch 期间发现目标已退订（在途投递被丢弃）时调
// `note_retired_delivery()`；broker 在 export 返回后调 `take_retired_delivery()`
// 把这次投递记成 retired（AGENTS.md 不变量 7）。
void note_retired_delivery();
bool take_retired_delivery();

}  // namespace backend
}  // namespace shuttle::detail
