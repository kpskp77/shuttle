// 三种发布方式的行为自检：
//   `publish_now()`         —— 发布者栈内同步回调 nonblocking 入口，全程不挂起
//   `publish_serial()`      —— 每 topic 一个后台 weave task 顺序回调，per-topic FIFO
//   `publish_concurrent()`  —— 每个订阅者一个后台 weave task 并发回调，无顺序承诺
// 以及它们的等待语义（wait / wait(timeout) / done / result / failures）。

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

// ── 消息类型（publish_tests 专用）：SHUTTLE_REFLECT 必须在全局作用域使用；
// 类型名即跨语言 type_tag，各测试文件用互不相同的名字避免 ODR 冲突。
struct PubMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(PubMsg, v)

// 另一种类型，用于验证通配符订阅者的类型过滤（type_tag 不同即 filtered）
struct PubMsgOther {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(PubMsgOther, v)

WEAVE_TEST(publish_tests) {
  // case1：`publish_now` 同步 —— nb 回调在 publish_now 返回前就已执行（发布者全程不挂起）
  run_case(__func__, "case1", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.sync");
    std::vector<std::uint32_t> order;
    auto s1 = shuttle::subscribe<PubMsg>("pub.now.sync", shuttle::nb(
        [&](const PubMsg& m, const shuttle::Meta&) { order.push_back(m.v + 100); }));
    auto s2 = shuttle::subscribe<PubMsg>("pub.now.sync", shuttle::nb(
        [&](const PubMsg& m, const shuttle::Meta&) { order.push_back(m.v + 200); }));

    pub.publish_now(PubMsg{1});
    expect_eq(static_cast<long long>(order.size()), 2, "publish_now 返回时回调已全部执行完毕");
    expect(order[0] == 101 && order[1] == 201, "按订阅顺序同步回调");
  });

  // case2：默认策略 = defer —— 入口为 blocking 的订阅者转后台串行链，发布者不被挂起
  run_case(__func__, "case2", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.blk");
    std::size_t blocking_frames = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.blk", [&](const PubMsg&, const shuttle::Meta&) {
      ++blocking_frames;
    });

    auto h = pub.publish_now(PubMsg{1});  // 不抛异常
    expect(h.valid(), "默认策略返回句柄");
    expect_eq(static_cast<long long>(h.deferred()), 1, "仅 blocking 入口的订阅者被 defer");
    expect_eq(static_cast<long long>(blocking_frames), 0, "defer 不阻塞发布者");
    h.wait();
    expect_eq(static_cast<long long>(blocking_frames), 1, "让出控制权后收到帧");
    expect(h.done(), "deferred 完成后句柄完成");
  });

  // case3：drop 策略 —— 跳过仅有 blocking 入口的订阅者，不抛异常且计数可见
  run_case(__func__, "case3", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.drop");
    std::size_t blocking_frames = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.drop", [&](const PubMsg&, const shuttle::Meta&) {
      ++blocking_frames;
    });
    std::size_t nb_frames = 0;
    auto n = shuttle::subscribe<PubMsg>("pub.now.drop", shuttle::nb(
        [&](const PubMsg&, const shuttle::Meta&) { ++nb_frames; }));

    auto h = pub.publish_now(PubMsg{1}, shuttle::NowPolicy::drop);
    expect_eq(static_cast<long long>(blocking_frames), 0, "drop 跳过仅有 blocking 入口的订阅者");
    expect_eq(static_cast<long long>(nb_frames), 1, "nonblocking 入口的订阅者照常投递");
    expect_eq(static_cast<long long>(h.excluded()), 1, "drop 计入 excluded");
  });

  // case4：reject / defer —— reject 报错且不部分投递；defer 不阻塞发布者
  run_case(__func__, "case4", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.defer");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.defer", [&](const PubMsg&, const shuttle::Meta&) {
      ++frames;
    });

    bool threw = false;
    try {
      pub.publish_now(PubMsg{0}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    expect(threw, "reject 策略需报错，而不是静默丢帧");
    expect_eq(static_cast<long long>(frames), 0, "报错时不得部分投递");

    pub.publish_now(PubMsg{1}, shuttle::NowPolicy::defer);
    expect_eq(static_cast<long long>(frames), 0, "defer 不阻塞发布者");
    for (int i = 0; i < 50 && frames == 0; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(frames), 1, "让出控制权后订阅者收到帧");
  });

  // case5：`publish_serial` per-topic FIFO —— 连续三帧按发布顺序到达
  run_case(__func__, "case5", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.fifo");
    std::vector<std::uint32_t> seen;
    std::vector<std::uint64_t> seqs;
    auto s = shuttle::subscribe<PubMsg>("pub.seq.fifo", [&](const PubMsg& m, const shuttle::Meta& meta) {
      seen.push_back(m.v);
      seqs.push_back(meta.seq);
      weave::sleep(50_us);  // 故意放慢：不改变顺序承诺
    });

    pub.publish_serial(PubMsg{1});
    pub.publish_serial(PubMsg{2});
    pub.publish_serial(PubMsg{3});
    auto last = pub.publish_serial(PubMsg{4});
    last.wait();

    expect_eq(static_cast<long long>(seen.size()), 4, "四帧全部到达");
    for (std::uint32_t i = 0; i < 4; ++i) {
      expect_eq(static_cast<long long>(seen.at(i)), i + 1, "publish_serial 保 topic 级 FIFO");
    }
    expect(seqs[0] < seqs[1] && seqs[1] < seqs[2] && seqs[2] < seqs[3], "seq 与投递顺序一致");
  });

  // case6：`publish_serial` 快照语义 —— 发布之后新增的订阅者不参与本次发布
  run_case(__func__, "case6", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.snap");
    std::size_t first = 0;
    auto s1 = shuttle::subscribe<PubMsg>("pub.seq.snap", [&](const PubMsg&, const shuttle::Meta&) {
      ++first;
      weave::sleep(200_us);  // 撑开窗口：让新订阅者在投递过程中注册
    });

    auto h = pub.publish_serial(PubMsg{1});
    expect_eq(static_cast<long long>(h.subscriber_count()), 1, "快照在发布瞬间锁定");

    std::size_t late = 0;
    auto s2 = shuttle::subscribe<PubMsg>("pub.seq.snap", [&](const PubMsg&, const shuttle::Meta&) {
      ++late;
    });
    h.wait();

    expect_eq(static_cast<long long>(first), 1, "既有订阅者收到");
    expect_eq(static_cast<long long>(late), 0, "发布之后新增的订阅者不收到本帧");
  });

  // case7：`publish_serial` 失败聚合 —— 单订阅者抛异常不影响其它订阅者，result() 重抛
  run_case(__func__, "case7", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.fail");
    std::size_t ok = 0;
    auto bad = shuttle::subscribe<PubMsg>("pub.seq.fail", [](const PubMsg&, const shuttle::Meta&) {
      throw std::runtime_error("订阅者故意失败");
    });
    auto good = shuttle::subscribe<PubMsg>("pub.seq.fail", [&](const PubMsg&, const shuttle::Meta&) {
      ++ok;
    });

    auto h = pub.publish_serial(PubMsg{1});
    h.wait();
    expect(h.done(), "全部订阅者回调结束（含失败）");
    expect_eq(static_cast<long long>(h.finished()), 2, "finished == subscriber_count");
    expect_eq(static_cast<long long>(ok), 1, "失败不阻断后续订阅者");
    expect_eq(static_cast<long long>(h.failures().size()), 1, "失败被逐订阅者聚合");
    expect(!h.failures().at(0).subscriber.empty(), "失败记录含订阅者描述");

    bool threw = false;
    std::string what;
    try {
      h.result();
    } catch (const std::exception& e) {
      threw = true;
      what = e.what();
    }
    expect(threw, "result() 必须重抛失败");
    expect(what.find("订阅者故意失败") != std::string::npos, "重抛的异常可辨识");
  });

  // case8：`publish_concurrent` —— 每个订阅者各自一个回调进程，全部到达后才完成
  run_case(__func__, "case8", [] {
    shuttle::Publisher<PubMsg> pub("pub.par.all");
    std::size_t a = 0, b = 0, c = 0;
    auto sa = shuttle::subscribe<PubMsg>("pub.par.all", [&](const PubMsg&, const shuttle::Meta&) {
      ++a;
      weave::sleep(20_us);
    });
    auto sb = shuttle::subscribe<PubMsg>("pub.par.all", [&](const PubMsg&, const shuttle::Meta&) {
      ++b;
      weave::sleep(40_us);
    });
    auto sc = shuttle::subscribe<PubMsg>("pub.par.all", [&](const PubMsg&, const shuttle::Meta&) {
      ++c;
    });

    auto h = pub.publish_concurrent(PubMsg{7});
    expect_eq(static_cast<long long>(h.subscriber_count()), 3, "发给三个订阅者");
    expect_eq(static_cast<long long>(a + b + c), 0, "publish_concurrent 立即返回，不阻塞发布者");

    h.wait();
    expect_eq(static_cast<long long>(a), 1, "订阅者 A 完成");
    expect_eq(static_cast<long long>(b), 1, "订阅者 B 完成");
    expect_eq(static_cast<long long>(c), 1, "订阅者 C 完成");
    expect(h.done(), "全部完成后 done() 为真");
  });

  // case9：零订阅者发布 —— 立即完成，不悬挂
  run_case(__func__, "case9", [] {
    shuttle::Publisher<PubMsg> pub("pub.empty");
    auto h = pub.publish_serial(PubMsg{1});
    expect_eq(static_cast<long long>(h.subscriber_count()), 0, "无订阅者");
    h.wait();
    expect(h.done(), "无订阅者的发布立即完成");

    auto p = pub.publish_concurrent(PubMsg{2});
    p.wait();
    expect(p.done(), "publish_concurrent 零订阅者同样立即完成");
  });

  // case10：wait(timeout) 超时返回 false，且不吞掉后续完成
  run_case(__func__, "case10", [] {
    shuttle::Publisher<PubMsg> pub("pub.timeout");
    auto s = shuttle::subscribe<PubMsg>("pub.timeout", [](const PubMsg&, const shuttle::Meta&) {
      weave::sleep(20_ms);
    });

    auto h = pub.publish_serial(PubMsg{1});
    expect(!h.wait(1_us), "超时返回 false");
    h.wait();  // 继续等，必须能等到
    expect(h.done(), "超时后仍可继续等待至完成");
    expect(h.failures().empty(), "无失败");
  });

  // case11：入口选择 —— `now` 只走 nonblocking 入口；`serial`/`concurrent` 全量投递（每个订阅者用自己注册的那一条入口）
  run_case(__func__, "case11", [] {
    shuttle::Publisher<PubMsg> pub("pub.once");
    std::size_t blocking_frames = 0;
    std::size_t nb_frames = 0;
    auto sb = shuttle::subscribe<PubMsg>("pub.once", [&](const PubMsg&, const shuttle::Meta&) {
      ++blocking_frames;
    });
    auto sn = shuttle::subscribe<PubMsg>("pub.once", shuttle::nb(
        [&](const PubMsg&, const shuttle::Meta&) { ++nb_frames; }));

    pub.publish_now(PubMsg{1}, shuttle::NowPolicy::drop);  // 只命中 nonblocking 入口
    expect_eq(static_cast<long long>(nb_frames), 1, "publish_now 命中 nonblocking 入口");
    expect_eq(static_cast<long long>(blocking_frames), 0, "publish_now drop 不命中 blocking 入口");

    // `serial`/`concurrent` 的目标集合是"全部匹配订阅者"：两者各收到一帧
    auto hp = pub.publish_concurrent(PubMsg{2});
    hp.wait();
    expect_eq(static_cast<long long>(blocking_frames), 1, "publish_concurrent 命中 blocking 入口");
    expect_eq(static_cast<long long>(nb_frames), 2, "publish_concurrent 也命中 nonblocking 入口");
    expect_eq(static_cast<long long>(hp.subscriber_count()), 2, "publish_concurrent 目标数 = 全部订阅者");

    auto hs = pub.publish_serial(PubMsg{3});
    hs.wait();
    expect_eq(static_cast<long long>(blocking_frames), 2, "publish_serial 命中 blocking 入口");
    expect_eq(static_cast<long long>(nb_frames), 3, "publish_serial 也命中 nonblocking 入口");
    expect_eq(static_cast<long long>(hs.subscriber_count()), 2, "publish_serial 目标数 = 全部订阅者");
  });

  // case12：可见性计数 —— publish_now(defer) 返回的句柄覆盖 nonblocking 与 deferred 两部分
  run_case(__func__, "case12", [] {
    shuttle::Publisher<PubMsg> pub("pub.vis");
    std::size_t blk = 0;
    std::size_t nbh = 0;
    auto sb = shuttle::subscribe<PubMsg>("pub.vis", [&](const PubMsg&, const shuttle::Meta&) {
      ++blk;
    });
    auto sn = shuttle::subscribe<PubMsg>("pub.vis", shuttle::nb(
        [&](const PubMsg&, const shuttle::Meta&) { ++nbh; }));

    auto h = pub.publish_now(PubMsg{1}, shuttle::NowPolicy::defer);
    expect_eq(static_cast<long long>(h.subscriber_count()), 2, "目标含 nonblocking 与 deferred 两部分");
    expect_eq(static_cast<long long>(h.deferred()), 1, "deferred 计数为 1");
    expect_eq(static_cast<long long>(h.excluded()), 0, "defer 不排除任何人");
    expect_eq(static_cast<long long>(nbh), 1, "nonblocking 部分返回时已结束");
    expect_eq(static_cast<long long>(blk), 0, "deferred 部分还没开始");
    expect(!h.done(), "deferred 未完成 → 句柄未完成");
    h.wait();
    expect(h.done(), "deferred 结束后句柄完成");
    expect_eq(static_cast<long long>(blk), 1, "deferred 订阅者最终收到");
    expect_eq(static_cast<long long>(h.finished()), 2, "finished 覆盖两部分");

    auto drop_h = pub.publish_now(PubMsg{2}, shuttle::NowPolicy::drop);
    expect_eq(static_cast<long long>(drop_h.excluded()), 1, "drop 计入 excluded");
    expect_eq(static_cast<long long>(drop_h.deferred()), 0, "drop 不计入 deferred");

    auto info = shuttle::broker::topic_info("pub.vis");
    expect(info.has_value(), "topic 存在");
    expect_eq(static_cast<long long>(info->deferred), 1, "topic 累计 deferred 计数");
  });

  // case13：defer 与 publish_serial 共用同一条串行链 → 后台投递顺序 == 发布顺序
  run_case(__func__, "case13", [] {
    shuttle::Publisher<PubMsg> pub("pub.defer.order");
    std::vector<std::uint32_t> order;
    auto sb = shuttle::subscribe<PubMsg>("pub.defer.order", [&](const PubMsg& m,
                                                              const shuttle::Meta&) {
      order.push_back(m.v);
      weave::sleep(20_us);
    });

    pub.publish_now(PubMsg{1}, shuttle::NowPolicy::defer);  // 骨架：走后台链
    pub.publish_serial(PubMsg{2});
    pub.publish_now(PubMsg{3}, shuttle::NowPolicy::defer);
    auto last = pub.publish_serial(PubMsg{4});
    last.wait();

    expect_eq(static_cast<long long>(order.size()), 4, "四帧全部后台投递");
    for (std::size_t i = 0; i < order.size(); ++i)
      expect_eq(static_cast<long long>(order.at(i)), static_cast<long long>(i + 1),
                "defer 与 serial 混合仍保发布序");
  });

  // case14：`publish_now` 同步段失败 —— 其余 nonblocking 目标照常尝试，defer 段照常入链，
  // 第一个异常在全部尝试之后才抛给调用者（不再“先 pub_complete 再 throw”）
  run_case(__func__, "case14", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.throw");
    std::size_t second = 0, blocking_hits = 0;
    auto s1 = shuttle::subscribe<PubMsg>("pub.now.throw", shuttle::nb(
        [](const PubMsg&, const shuttle::Meta&) { throw std::runtime_error("boom"); }));
    auto s2 = shuttle::subscribe<PubMsg>("pub.now.throw", shuttle::nb(
        [&](const PubMsg&, const shuttle::Meta&) { ++second; }));
    auto s3 = shuttle::subscribe<PubMsg>("pub.now.throw",
                                        [&](const PubMsg&, const shuttle::Meta&) {
                                          ++blocking_hits;
                                        });

    bool threw = false;
    try {
      pub.publish_now(PubMsg{1});  // 默认 defer
    } catch (const std::exception&) {
      threw = true;
    }
    expect(threw, "同步段异常仍传播给调用者");
    expect_eq(static_cast<long long>(second), 1,
              "第一个订阅者失败后仍尝试后面的 nonblocking 目标");
    weave::sleep(20_us);
    expect_eq(static_cast<long long>(blocking_hits), 1, "同步段失败不影响 defer 段投递");
    auto info = shuttle::broker::topic_info("pub.now.throw");
    expect(info.has_value(), "topic 存在");
    expect_eq(static_cast<long long>(info->published), 1, "帧已发布");
    expect_eq(static_cast<long long>(info->deferred), 1, "defer 段仍入链");
  });

  // case15：类型过滤 —— 通配符订阅者只收自己声明的类型，不匹配的帧计入 filtered()/dropped()
  run_case(__func__, "case15", [] {
    shuttle::Publisher<PubMsg> pub_a("pub.filter.a");
    shuttle::Publisher<PubMsgOther> pub_b("pub.filter.b");
    std::size_t got = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.filter.*", [&](const PubMsg&, const shuttle::Meta&) {
      ++got;
    });

    auto ha = pub_a.publish_serial(PubMsg{1});
    ha.wait();
    expect_eq(static_cast<long long>(got), 1, "匹配类型照常投递");
    expect_eq(static_cast<long long>(ha.filtered()), 0, "匹配时不产生过滤");

    auto hb = pub_b.publish_serial(PubMsgOther{2});
    hb.wait();
    expect_eq(static_cast<long long>(got), 1, "类型不匹配的帧不进入回调");
    expect_eq(static_cast<long long>(hb.filtered()), 1, "帧级 filtered 计数");
    expect_eq(static_cast<long long>(hb.finished()),
              static_cast<long long>(hb.subscriber_count()), "过滤也是终态");
    expect_eq(static_cast<long long>(s.dropped()), 1, "订阅者级 dropped 计入过滤");
  });

  // case16：零目标的 `publish_serial` 立即完成；`reject` 的帧从不发布（不消耗 seq/published）
  run_case(__func__, "case16", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.empty");
    auto h = pub.publish_serial(PubMsg{1});
    expect(h.done(), "零目标立即完成，不建投递单元");
    expect_eq(static_cast<long long>(h.subscriber_count()), 0, "零目标");
    expect_eq(static_cast<long long>(h.finished()), 0, "完成且计数为 0");

    shuttle::Publisher<PubMsg> p2("pub.now.reject2");
    std::size_t hits = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.reject2",
                                        [&](const PubMsg&, const shuttle::Meta&) { ++hits; });
    bool threw = false;
    try {
      p2.publish_now(PubMsg{1}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    expect(threw, "reject 抛 CapabilityError");
    auto info = shuttle::broker::topic_info("pub.now.reject2");
    expect(info.has_value(), "topic 存在");
    expect_eq(static_cast<long long>(info->published), 0,
              "被 reject 的帧从未发布（不消耗 seq）");
    expect_eq(static_cast<long long>(hits), 0, "没有任何回调被调用");
  });

  report(__func__);
}
