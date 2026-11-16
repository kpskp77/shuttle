// broker 与订阅语义自检：通配符匹配、retained、退订、多发布者、内省、类型绑定。
//
// 这些用例都用独立的 topic 命名空间（bkr.*），彼此不共享状态；
// broker 是进程级单例，故断言只用本用例独占的 topic（或它与本次用例的增量）。

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <shuttle/shuttle.hpp>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── 测试用消息类型（SHUTTLE_REFLECT 只能在全局作用域使用）──
// 类型名同时是跨语言 type_tag，故各测试文件用互不相同的名字（避免 ODR 冲突）。
struct BkrTx {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(BkrTx, addr, data)

struct BkrOther {
  std::uint16_t v = 0;
};
SHUTTLE_REFLECT(BkrOther, v)

namespace {

// 收集 topic 的字节订阅（用于通配符用例）
struct ByteTaps {
  std::vector<std::string> topics;
  std::size_t              frames = 0;
};

std::shared_ptr<ByteTaps> tap(shuttle::Subscription& keep, std::string pattern) {
  auto rec = std::make_shared<ByteTaps>();
  // 这些用例都用 `publish_now` 投递，故订阅者必须注册 nonblocking 入口；
  // 回调只做记录，不消耗仿真时间，符合 nb 契约。
  keep = shuttle::subscribe_bytes(
      std::move(pattern),
      [rec](const std::vector<std::byte>& b, const shuttle::Meta& m) {
        ++rec->frames;
        rec->topics.push_back(m.topic);
        rec->topics.push_back(std::to_string(b.size()));  // 顺带记录线长，验证编码长度
      },
      shuttle::Capability::nonblocking);
  return rec;
}

}  // namespace

WEAVE_TEST(broker_tests) {
  // case1：通配符 —— "*" 只匹配单级
  run_case(__func__, "case1", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.w.a");
    shuttle::Publisher<BkrTx> pub_ab("bkr.w.a.b");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.w.*");

    pub_a.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_ab.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 1, "\"*\" 只收到单级 topic");
    expect(rec->topics.at(0) == "bkr.w.a", "收到的是单级 topic");
  });

  // case2：通配符 —— ">" 匹配其后任意多级（至少一级）
  run_case(__func__, "case2", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.m.a");
    shuttle::Publisher<BkrTx> pub_ab("bkr.m.a.b");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.m.>");

    pub_a.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_ab.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 2, "\">\" 收到任意多级");
    // 线长 = 两个 u32
    expect(rec->topics.at(1) == "8" && rec->topics.at(3) == "8", "线长为 8 字节");
  });

  // case3：精确 topic 不跨 topic 泄漏
  run_case(__func__, "case3", [] {
    shuttle::Publisher<BkrTx> pub_x("bkr.x.one");
    shuttle::Publisher<BkrTx> pub_y("bkr.x.two");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.x.one");

    pub_x.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_y.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 1, "只收到精确 topic 的帧");
  });

  // case4：retained —— 开启后新订阅者立即收到最新一帧
  run_case(__func__, "case4", [] {
    shuttle::Publisher<BkrTx> pub("bkr.ret.tx");
    pub.set_retained(true);
    pub.publish_now(BkrTx{0x13, 0xC3}, shuttle::NowPolicy::skip);

    BkrTx seen{};
    std::size_t frames = 0;
    auto late = shuttle::subscribe<BkrTx>(
        "bkr.ret.tx", shuttle::nb([&](const BkrTx& tx, const shuttle::Meta&) {
          seen = tx;
          ++frames;
        }));
    expect_eq(static_cast<long long>(frames), 1, "retained 帧同步补发");
    expect(seen.addr == 0x13, "retained 帧内容为最新一次发布");

    pub.publish_now(BkrTx{0x14, 0xC4}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 2, "补发后仍正常接收后续帧");
  });

  // case5：retained 关闭后新订阅者不再收到历史帧
  run_case(__func__, "case5", [] {
    shuttle::Publisher<BkrTx> pub("bkr.ret.off");
    pub.set_retained(true);
    pub.publish_now(BkrTx{0x20, 0x20}, shuttle::NowPolicy::skip);
    pub.set_retained(false);

    std::size_t frames = 0;
    auto late = shuttle::subscribe<BkrTx>(
        "bkr.ret.off",
        shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));
    expect_eq(static_cast<long long>(frames), 0, "关闭 retained 后不再补发");
  });

  // case6：显式退订（幂等）后不再收到；received() 反映已投递帧数
  run_case(__func__, "case6", [] {
    shuttle::Publisher<BkrTx> pub("bkr.unsub");
    std::size_t frames = 0;
    auto sub = shuttle::subscribe<BkrTx>(
        "bkr.unsub", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));

    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "退订前正常收到");
    expect_eq(static_cast<long long>(sub.received()), 1, "received() 计数");

    sub.unsubscribe();
    expect(sub.valid() && sub.received() == 1, "退订后句柄仍可查询计数");
    sub.unsubscribe();  // 幂等，不得抛
    pub.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "退订后不再收到");
    expect_eq(static_cast<long long>(sub.received()), 1, "不再投递后计数不增");
  });

  // case7：句柄共享语义 —— 最后一份析构自动退订
  run_case(__func__, "case7", [] {
    shuttle::Publisher<BkrTx> pub("bkr.raii");
    std::size_t frames = 0;
    {
      auto a = shuttle::subscribe<BkrTx>(
          "bkr.raii", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));
      shuttle::Subscription b = a;  // 共享同一订阅
      pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
      expect_eq(static_cast<long long>(frames), 1, "共享句柄期间正常投递");
    }  // 两份都析构 → 自动退订
    pub.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "全部句柄析构后自动退订");
  });

  // case8：内省 —— topic_info 的计数与订阅者数
  run_case(__func__, "case8", [] {
    shuttle::Publisher<BkrTx> pub("bkr.info");
    auto s1 = shuttle::subscribe<BkrTx>("bkr.info", shuttle::nb([](const BkrTx&,
                                                                   const shuttle::Meta&) {}));
    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);

    const auto info = shuttle::broker::topic_info("bkr.info");
    expect(info.has_value(), "topic_info 应返回已绑定的 topic");
    expect(info->name == "bkr.info", "topic 名");
    expect(info->type_tag == "BkrTx", "类型标签来自 SHUTTLE_REFLECT");
    expect_eq(static_cast<long long>(info->subscriber_count), 1, "订阅者数");
    expect(info->published >= 1, "published 计数");
    expect(info->delivered >= 1, "delivered 计数");
    expect(!info->retained, "retained 默认关闭");

    expect(!shuttle::broker::topic_info("bkr.info.absent").has_value(),
           "未出现的 topic 返回 nullopt");

    // topics() 快照包含该 topic
    bool found = false;
    for (const std::string& name : shuttle::broker::topics()) found = found || name == "bkr.info";
    expect(found, "topics() 应包含该 topic");
  });

  // case9：多对多 —— 同一 topic 两个 Publisher，Meta.seq topic 级全局单调
  run_case(__func__, "case9", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.multi");
    shuttle::Publisher<BkrTx> pub_b("bkr.multi");
    std::vector<std::uint64_t> seqs;
    std::vector<std::uint32_t> addrs;
    auto sub = shuttle::subscribe<BkrTx>(
        "bkr.multi", shuttle::nb([&](const BkrTx& tx, const shuttle::Meta& m) {
          seqs.push_back(m.seq);
          addrs.push_back(tx.addr);
        }));

    pub_a.publish_now(BkrTx{0xA1, 0}, shuttle::NowPolicy::skip);
    pub_b.publish_now(BkrTx{0xB1, 0}, shuttle::NowPolicy::skip);
    pub_a.publish_now(BkrTx{0xA2, 0}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(seqs.size()), 3, "三个发布帧都到达");
    expect(seqs[0] < seqs[1] && seqs[1] < seqs[2], "seq 跨 Publisher 全局单调");
    expect(addrs[0] == 0xA1 && addrs[1] == 0xB1 && addrs[2] == 0xA2, "publish_now 保发布顺序");
  });

  // case10：类型绑定冲突 —— 同 topic 用不同类型发布/订阅都被拒绝
  run_case(__func__, "case10", [] {
    shuttle::Publisher<BkrTx> pub("bkr.typed");
    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);

    bool threw = false;
    try {
      shuttle::Publisher<BkrOther> bad("bkr.typed");  // 构造即声明类型
    } catch (const shuttle::TypeMismatchError&) {
      threw = true;
    }
    expect(threw, "同一 topic 绑定不同类型应抛 TypeMismatchError");

    threw = false;
    try {
      auto bad_sub = shuttle::subscribe<BkrOther>("bkr.typed", shuttle::nb([](const BkrOther&,
                                                                          const shuttle::Meta&) {}));
    } catch (const shuttle::TypeMismatchError&) {
      threw = true;
    }
    expect(threw, "订阅已绑定 topic 用错类型应抛 TypeMismatchError");
  });

  // case11：类型不相交 = 不在目标集 —— 通配符订阅者（BkrTx）只对 tag 相同的帧是目标，
  // bkr.mix.other 的帧既不进回调、也不计 dropped（它本就不是目标）。
  // 同时守住 `Broker::check_bind_compat` 的**通配符豁免**：bkr.mix.other 首次绑定的 tag 与
  // 上面的通配符订阅者（BkrTx）不同，但通配符本就横跨类型，绑定不得报错。
  run_case(__func__, "case11", [] {
    // 通配符订阅在订阅时无法判定具体 topic 的类型，目标集判定推迟到每帧的快照期
    std::size_t uart_frames = 0;
    auto wild = shuttle::subscribe<BkrTx>(
        "bkr.mix.>", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++uart_frames; }));

    shuttle::Publisher<BkrTx> pub_u("bkr.mix.uart");
    shuttle::Publisher<BkrOther> pub_o("bkr.mix.other");
    pub_u.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_o.publish_now(BkrOther{7}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(uart_frames), 1, "只投递 tag 匹配的帧");
    expect_eq(static_cast<long long>(wild.received()), 1, "只把匹配的帧记成 receive");
    expect_eq(static_cast<long long>(wild.dropped()), 0, "类型不相交的帧不是目标，不计 dropped");
  });

  // case12：退订即生效 —— 快照之后、回调之前被退订的目标记 retired。
  // 快照只保证"谁被选中"，不保证"一定被调用"：C++ 侧快照持有 shared_ptr、对象还活着，
  // 所以 deliver 要显式看一眼 `removed`；SV 侧 sink 已被注册表删除，由 shuttle_c_sub_retired
  // 回告。两种语言从此只有一种口径。
  run_case(__func__, "case12", [] {
    shuttle::Publisher<BkrTx> pub("bkr.inflight");
    std::size_t a_frames = 0;
    std::size_t b_frames = 0;
    shuttle::Subscription a;
    // 注册顺序 = 串行链里的投递顺序：B 先跑，并在自己的回调里退订排在其后的 A
    auto b = shuttle::subscribe<BkrTx>(
        "bkr.inflight",
        shuttle::nb([&](const BkrTx&, const shuttle::Meta&) {
          ++b_frames;
          a.unsubscribe();
        }));
    a = shuttle::subscribe<BkrTx>(
        "bkr.inflight", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++a_frames; }));

    auto p = pub.publish_serial(BkrTx{1, 1});
    p.wait();

    expect_eq(static_cast<long long>(b_frames), 1, "排在前面的订阅者照常收到");
    expect_eq(static_cast<long long>(a_frames), 0, "同帧内被退订的目标不再进回调");
    expect_eq(static_cast<long long>(p.subscriber_count()), 2, "目标数在发布瞬间冻结");
    expect_eq(static_cast<long long>(p.finished()), 2, "两个目标都到终态");
    expect(p.done(), "done() 在全部目标到终态后成立");
    const auto info = shuttle::broker::topic_info("bkr.inflight");
    expect(info.has_value() && info->delivered == 1, "delivered 只数真正进回调的");
    expect_eq(static_cast<long long>(p.retired()), 1, "被退订的那个记 retired");
    expect_eq(static_cast<long long>(a.dropped()), 1, "退订丢弃计入该订阅者的 dropped");
    expect(b.valid(), "b 未退订");
    // 三分解：dropped 是 retired / skipped / failed 的求和（口径同构，见 architecture §9.2 第 5 条）
    expect_eq(static_cast<long long>(info->retired), 1, "retired 分量单独可读");
    expect_eq(static_cast<long long>(info->skipped), 0, "没有策略跳过");
    expect_eq(static_cast<long long>(info->failed), 0, "没有失败");
    expect_eq(static_cast<long long>(info->dropped),
              static_cast<long long>(info->retired + info->skipped + info->failed),
              "topic 级 dropped == retired + skipped + failed");
  });

  // case13：">" 与 "*" 都不匹配"前缀本身"（至少要再多一级）。
  // 这条边界是匹配器从"split 成 vector 再比"改成"逐段 view 比较"时最容易写歪的地方。
  run_case(__func__, "case13", [] {
    shuttle::Publisher<BkrTx> p_prefix("bkr.deep");
    shuttle::Publisher<BkrTx> p_child("bkr.deep.x");
    shuttle::Subscription keep_a;
    shuttle::Subscription keep_b;
    auto gt = tap(keep_a, "bkr.deep.>");
    auto star = tap(keep_b, "bkr.deep.*");

    p_prefix.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(gt->frames), 0, "\">\" 不匹配前缀本身");
    expect_eq(static_cast<long long>(star->frames), 0, "\"*\" 不匹配前缀本身");

    p_child.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(gt->frames), 1, "\">\" 收到子级");
    expect_eq(static_cast<long long>(star->frames), 1, "\"*\" 收到单级子 topic");
  });

  report(__func__);
}
