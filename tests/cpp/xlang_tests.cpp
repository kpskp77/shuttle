// 跨语言往返自检（仅 svdpi 后端）：C++ → SV 与 SV → C++ 各自的发布/订阅互通。
//
// 与 tests/sv/shuttle_tests.sv 里的 xlang 段配合，握手顺序：
//   1. SV 侧先建立 C++→SV 的订阅，再在 "xlang.sv_ready" 上发布一帧（retained）；
//   2. C++ 侧订阅 "xlang.sv_ready"（retained 保证即便 SV 抢先发布也能收到），
//      随后在 "xlang.cpp2sv" 上顺序发布 3 帧并等每帧完成 —— 完成即代表 SV 侧
//      3 个回调都已执行完（`Publication` 的完成语义正是"全部订阅者回调结束"）；
//   3. SV 侧确认收到 3 帧后，在 "xlang.sv2cpp" 上发布 3 帧；
//   4. C++ 侧等待并校验这 3 帧（内容、type_tag、seq 单调）。
//
// 两侧都完成自己的断言后，由 tests/sv/shuttle_tests.sv 的 run_cpp_tests 汇总判定。

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <shuttle/shuttle.hpp>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

#if defined(SHUTTLE_BACKEND_SVDPI)

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// 消息类型（SHUTTLE_REFLECT 必须在全局作用域使用）。
// 与 tests/sv/shuttle_tests.sv 的 class XMsg 逐字节对等：
//   {>>{addr, data}} —— 两个 32 位无符号，大端；tag 均为 "XMsg"。
struct XMsg {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(XMsg, addr, data)

namespace {

constexpr const char* kReadyTopic  = "xlang.sv_ready";  // SV → C++ 就绪广播（retained）
constexpr const char* kToSvTopic   = "xlang.cpp2sv";    // C++ → SV
constexpr const char* kFromSvTopic = "xlang.sv2cpp";    // SV → C++

// 跨用例共享的状态（订阅句柄必须活到测试结束，故放在堆上的 holder 里）
struct XlangState {
  shuttle::Subscription    ready;    // retained 就绪帧
  shuttle::Subscription    from_sv;  // SV → C++
  shuttle::Event           ready_ev;
  std::vector<XMsg>        got;
  std::vector<std::uint64_t> seqs;
  std::vector<std::string>  tags;
  std::size_t              ready_frames = 0;
};

}  // namespace

WEAVE_TEST(xlang_tests) {
  auto st = std::make_shared<XlangState>();

  // case1：等待 SV 侧就绪（订阅 "xlang.sv_ready"；retained 语义保证不丢）
  run_case(__func__, "case1", [st] {
    st->ready = shuttle::subscribe_bytes(
        kReadyTopic, [st](const std::vector<std::byte>& bytes, const shuttle::Meta& meta) {
          ++st->ready_frames;
          // 就绪帧与业务帧同一套线格式：两个 32 位
          expect(bytes.size() == 8, "就绪帧线长应为 8 字节");
          expect(meta.type_tag == "XMsg", "就绪帧的类型标签来自 SV 的 class XMsg");
          st->ready_ev.set();
        }, shuttle::Capability::blocking);

    for (int i = 0; i < 200 && !st->ready_ev.is_set(); ++i) weave::sleep(100_us);
    expect(st->ready_ev.is_set(), "等不到 SV 侧就绪广播（xlang.sv_ready）");
    expect_eq(static_cast<long long>(st->ready_frames), 1, "就绪帧恰好一帧");
  });

  // case2：C++ → SV —— 顺序发布 3 帧，逐帧等到"SV 侧回调结束"
  run_case(__func__, "case2", [st] {
    // 先建好订阅：SV 收到 3 帧后才会回发，但句柄要在本测试内一直活着
    st->from_sv = shuttle::subscribe<XMsg>(kFromSvTopic, [st](const XMsg& m,
                                                             const shuttle::Meta& meta) {
      st->got.push_back(m);
      st->seqs.push_back(meta.seq);
      st->tags.push_back(meta.type_tag);
    });

    shuttle::Publisher<XMsg> pub(kToSvTopic);
    for (std::uint32_t i = 1; i <= 3; ++i) {
      auto h = pub.publish_serial(XMsg{i, i * 0x100});
      expect_eq(static_cast<long long>(h.subscriber_count()), 1, "SV 侧应有 1 个阻塞订阅者");
      h.wait();
      expect(h.done(), "发布完成后句柄应处于完成态");
      expect(h.failures().empty(), "SV 侧回调不应失败");
    }
    auto info = shuttle::broker::topic_info(kToSvTopic);
    expect(info.has_value(), "topic 应已登记");
    expect_eq(static_cast<long long>(info->published), 3, "累计发布 3 帧");
    expect_eq(static_cast<long long>(info->delivered), 3, "累计投递 3 次");
  });

  // case3：SV → C++ —— 校验回发的 3 帧（内容、类型标签、seq 单调）
  run_case(__func__, "case3", [st] {
    for (int i = 0; i < 200 && st->got.size() < 3; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(st->got.size()), 3, "SV → C++ 应收到 3 帧");

    for (std::size_t i = 0; i < st->got.size(); ++i) {
      expect_eq(static_cast<long long>(st->got[i].addr), static_cast<long long>(0x51 + i),
                "addr 字段跨语言一致");
      expect_eq(static_cast<long long>(st->got[i].data), static_cast<long long>(0xA0 + i),
                "data 字段跨语言一致");
      expect(st->tags[i] == "XMsg", "type_tag 为 SV 侧声明的 XMsg");
    }
    expect(st->seqs[0] < st->seqs[1] && st->seqs[1] < st->seqs[2],
           "SV 侧发布的 seq 在 topic 内单调递增");
  });

  report(__func__);
}

#else

WEAVE_TEST(xlang_tests) {
  std::printf("[SKIP] xlang_tests: systemc 后端没有 SV 桥（仅 svdpi 后端可测跨语言）\n");
  std::fflush(stdout);
}

#endif  // SHUTTLE_BACKEND_SVDPI
