// ============================================================================
// shuttle_tests.sv —— shuttle 的 SV 侧测试套件（真实仿真器 VCS）
//
// 编译顺序：weave_pkg.sv（../weave/sv）→ shuttle_pkg.sv（../../sv）→ 本文件。
// 运行：xmake run shuttle_tests_svdpi_vcs（经 tests/svdpi/run.sh 编译并跑 simv）。
//
// 组成：
//   · sv_cases       —— SV 语言内的全部用例（`now`/`serial`/`concurrent`、触发器、retained、
//                       多发布者、退订、入口路由、seq 语义、快照语义）；
//   · case_xlang     —— 与 C++ 侧 tests/cpp/xlang_tests.cpp 配合的跨语言往返；
//   · run_cpp_tests  —— 经 weave_run 启动 C++ 侧测试（tests/cpp 的 WEAVE_TEST），
//                       两侧共用同一 broker，故相互可见。
//
// 判定：SV 用例失败计入 g_fail；C++ 侧失败使 weave_run 返回非 0 status。
// 两侧都通过才打印 "SHUTTLE TESTS PASSED"（run.sh 以此为成功标记），
// 否则 $fatal 让 simv 非零退出。
//
// 注意：本文件不调用 $error——负向用例靠返回值判定，所有断言无论成败都只 $display 记录，
// 由末尾统一 $fatal 兜底，避免干扰 run.sh 的日志判定。
// ============================================================================

module shuttle_tests;
  timeunit 1ns;
  timeprecision 1ps;

  import shuttle_pkg::*;

  int unsigned g_pass = 0;
  int unsigned g_fail = 0;
  bit          g_sv_done = 0;

  // ── 消息类：与 C++ 端 struct XMsg + SHUTTLE_REFLECT(XMsg, addr, data) 对等 ──
  // {>>{...}} 逐字节等于 C++ Writer 的大端线序（VCS 实测，见 docs/dpi_protocol.md 的
  // "仿真器约束"一节）。
  class XMsg;
    int unsigned addr;
    int unsigned data;
    localparam string type_tag = "XMsg";

    function automatic void pack(output byte q[$]);
      q = {>>{addr, data}};
    endfunction

    static function automatic XMsg unpack(byte b[]);
      XMsg m = new();
      {>>{m.addr, m.data}} = b;
      return m;
    endfunction
  endclass

  // 另一种类型：仅用于"订阅先于绑定"的标签冲突用例（type_tag 与 XMsg 不同即可）
  class YMsg;
    int unsigned addr;
    localparam string type_tag = "YMsg";

    function automatic void pack(output byte q[$]);
      q = {>>{addr}};
    endfunction
  endclass

  // ── 两个入口都实现：shuttle_subscribe 登记 blocking 入口、shuttle_subscribe_nb 登记
  //    nonblocking 入口（与 C++ 的 subscribe(fn) / subscribe(nb(fn)) 同构）─────────
  class Rec implements shuttle_sink, shuttle_sink_nb;
    int     blk_count = 0;
    int     nb_count  = 0;
    int     order[$];   // 收到的 addr 序列（顺序断言）
    longint seqs[$];    // 收到的 seq 序列（单调性断言）
    XMsg    last;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      blk_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.seq);
      #(1ns);  // 可阻塞回调里消耗仿真时间属于合法用法
    endtask

    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      nb_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.seq);
    endfunction
  endclass

  // ── 入口为 blocking 的订阅者：`publish_now` 应报错或跳过 ─────────────────────
  class BlkRec implements shuttle_sink;
    int count = 0;
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      count++;
      #(1ns);
    endtask
  endclass

  // ── 只有 nonblocking 入口（invoke_nb）的订阅者：`serial`/`concurrent` 也应当投递（走 nonblocking 入口）──
  class NbRec implements shuttle_sink_nb;
    int  count = 0;
    XMsg last;
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      count++;
      last = m;
    endfunction
  endclass

  task automatic check(bit cond, string name);
    if (cond) begin
      g_pass++;
      $display("[PASS] sv::%s", name);
    end else begin
      g_fail++;
      $display("[FAIL] sv::%s", name);
    end
  endtask

  // ── `publish_now`：发布者进程内同步回调，全程不挂起 ───────────────────────────
  task automatic case_now_sync();
    automatic Rec                 rec = new();
    // `now` 只投 nonblocking 入口 → 用 shuttle_subscribe_nb 登记
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.now", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.now");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h101;
    m.data = 32'h202;
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "now_sync.handle");
    check(h.subscriber_count() == 1, "now_sync.count");
    check(h.done() != 0, "now_sync.done_on_return");  // nonblocking 部分返回即结束
    check(rec.nb_count == 1, "now_sync.nb_hit");
    check(rec.blk_count == 0, "now_sync.blocking_not_hit");
    check(rec.last.addr == 32'h101 && rec.last.data == 32'h202, "now_sync.payload");
  endtask

  // ── `publish_now`：入口为 blocking 的订阅者 → reject 抛错 / skip 跳过（默认是 defer）─────
  task automatic case_now_blocking();
    automatic BlkRec              rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.nowblk", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.nowblk");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 2;
    h = pub.publish_now(m, SHUTTLE_NOW_REJECT);
    check(h == null, "now_blk.reject_returns_null");
    check(rec.count == 0, "now_blk.no_partial_delivery");
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "now_blk.skip_ok");
    check(h.skipped() == 1 && h.deferred() == 0, "now_blk.skip_counted");
    check(h.done() != 0, "now_blk.skip_completes");
    check(rec.count == 0, "now_blk.skip_skips");
  endtask

  // ── `publish_serial`：后台单进程顺序回调，per-topic FIFO ──────────────────────────
  task automatic case_publish_serial();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.seq", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.seq");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    for (int i = 1; i <= 3; i++) begin
      m.addr = i;
      m.data = i * 16;
      h = pub.publish_serial(m);
      h.wait_done();
    end
    check(rec.blk_count == 3, "serial.all_arrived");
    // 这次注册只登记了 blocking 入口 → 只走 invoke，invoke_nb 不会被调用
    check(rec.nb_count == 0, "serial.blocking_entry_only");
    check(rec.order.size() == 3 && rec.order[0] == 1 && rec.order[1] == 2 && rec.order[2] == 3,
          "serial.fifo");
    check(rec.seqs.size() == 3 && rec.seqs[0] < rec.seqs[1] && rec.seqs[1] < rec.seqs[2],
          "serial.seq_monotonic");
  endtask

  // ── `publish_concurrent`：每订阅者一进程并发回调，全部到达后才算完成 ──────────────────
  task automatic case_publish_concurrent();
    automatic Rec                 a   = new();
    automatic Rec                 b   = new();
    automatic shuttle_sub         sa  = shuttle_subscribe("sv.t.par", a, "XMsg");
    automatic shuttle_sub         sb  = shuttle_subscribe("sv.t.par", b, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.par");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h300;
    m.data = 32'h301;
    h = pub.publish_concurrent(m);
    check(h.done() == 0, "concurrent.immediate_return");  // 立即返回，尚未完成
    h.wait_done();
    check(a.blk_count == 1, "concurrent.sub_a");
    check(b.blk_count == 1, "concurrent.sub_b");
    check(h.done() != 0, "concurrent.done_after_wait");
    check(h.first_error() == "", "concurrent.no_failure");
    check(a.nb_count == 0 && b.nb_count == 0, "concurrent.blocking_entry_only");
  endtask

  // ── 入口为 nonblocking 的订阅者：`serial`/`concurrent` 同样全量投递（入口规则：该次注册
  //    只有这一条入口，投递就用它）──
  task automatic case_nb_in_background();
    automatic NbRec               rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.nbbg", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.nbbg");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'h55;
    m.data = 32'h66;
    h = pub.publish_serial(m);
    h.wait_done();
    check(rec.count == 1, "nb_bg.serial_hits_nonblocking_entry");
    check(h.subscriber_count() == 1, "nb_bg.counted_as_target");
    h = pub.publish_concurrent(m);
    h.wait_done();
    check(rec.count == 2, "nb_bg.concurrent_hits_nonblocking_entry");
  endtask

  // ── 入口由注册函数决定：同一对象注册两次 = 两个订阅者 = 每帧两次回调 ──────
  // 这就是 C++ 的 subscribe(fn) + subscribe(nb(fn))：两个订阅者、两条不同入口。
  task automatic case_dual_registration();
    automatic Rec                 rec  = new();
    automatic shuttle_sub         st   = shuttle_subscribe   ("sv.t.dual", rec, "XMsg");  // blocking 入口
    automatic shuttle_sub         snb  = shuttle_subscribe_nb("sv.t.dual", rec, "XMsg");  // nonblocking 入口
    automatic shuttle_pub #(XMsg) pub  = new("sv.t.dual");
    automatic XMsg                m    = new();
    automatic shuttle_handle      h;
    m.addr = 32'h77;
    m.data = 32'h88;

    // `serial`：两个订阅者、两条入口各一次
    h = pub.publish_serial(m);
    h.wait_done();
    check(h.subscriber_count() == 2, "dual_registration.counted_twice");
    check(rec.blk_count == 1, "dual_registration.blocking_entry_once");
    check(rec.nb_count == 1, "dual_registration.nonblocking_entry_once");

    // `now`：只有 nonblocking 入口的那个被同步调用；blocking 入口的那个按策略处置
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "dual_registration.nonblocking_handle");
    check(h.skipped() == 1, "dual_registration.skip_counted");
    check(rec.nb_count == 2, "dual_registration.nonblocking_sync_second");
    check(rec.blk_count == 1, "dual_registration.blocking_entry_not_synced");
    h = pub.publish_now(m);                                          // 默认 defer
    check(h != null, "dual_registration.defer_handle");
    check(h.deferred() == 1 && h.done() == 0, "dual_registration.defer_pending");
    h.wait_done();                                                      // 等后台链跑完
    check(rec.blk_count == 2, "dual_registration.defer_reaches_blocking_entry");
    check(h.done() != 0, "dual_registration.defer_done_after_wait");
    st.unsubscribe();
    snb.unsubscribe();
  endtask

  // ── 延迟触发：条件满足前不投递，且 Publication 保持未完成 ───────────────
  task automatic case_trigger();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.trig", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.trig");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 7;
    m.data = 8;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(200_000_000));  // 200ns
    check(rec.blk_count == 0, "trigger.not_before_delay");
    check(h.done() == 0, "trigger.not_done_before_delay");
    h.wait_done();
    check(rec.blk_count == 1, "trigger.delivered_after_delay");
    check(rec.last.addr == 7, "trigger.payload");
  endtask

  // ── retained：新订阅者注册时补发最新一帧 ──────────────────────────────
  task automatic case_retained();
    automatic Rec                 first = new();
    automatic shuttle_sub         s1    = shuttle_subscribe("sv.t.ret", first, "XMsg");
    automatic shuttle_pub #(XMsg) pub   = new("sv.t.ret");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_sub         s2;
    pub.set_retained(1);
    m.addr = 32'hACE;
    m.data = 32'h1;
    h = pub.publish_serial(m);
    h.wait_done();
    check(first.blk_count == 1, "retained.first_sub");

    late = new();
    s2   = shuttle_subscribe("sv.t.ret", late, "XMsg");
    // 新订阅者的阻塞回调经后台进程补发，需驱动调度器：再发布一帧并等其完成
    for (int i = 0; i < 5 && late.blk_count == 0; i++) begin
      h = pub.publish_serial(m);
      h.wait_done();
    end
    check(late.blk_count >= 1, "retained.catchup_to_late_sub");
    check(late.last.addr == 32'hACE, "retained.catchup_payload");

    pub.set_retained(0);
    late = null;
    s2   = null;
  endtask

  // ── 多发布者共享一个 topic：都投递，seq 在 topic 内全局单调 ─────────────
  task automatic case_multi_pub();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.multi", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pa  = new("sv.t.multi");
    automatic shuttle_pub #(XMsg) pb  = new("sv.t.multi");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 32'hA;
    m.data = 0;
    h = pa.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "multi_pub.handle");
    m.addr = 32'hB;
    h = pb.publish_now(m, SHUTTLE_NOW_SKIP);
    check(rec.nb_count == 2, "multi_pub.both_delivered");
    check(rec.order.size() == 2 && rec.order[0] == 32'hA && rec.order[1] == 32'hB,
          "multi_pub.now_order");
    check(rec.seqs.size() == 2 && rec.seqs[0] < rec.seqs[1], "multi_pub.seq_global");
  endtask

  // ── 退订（幂等）：退订后不再收到 ──────────────────────────────────────
  // case_retired：退订后的在途投递 —— SV 注册表静默丢弃，C++ 侧必须记成 retired（不得记成 delivered）
  task automatic case_retired();
    automatic Rec                 a   = new();
    automatic Rec                 b   = new();
    automatic shuttle_sub         sa  = shuttle_subscribe("sv.t.retired", a, "XMsg");
    automatic shuttle_sub         sb  = shuttle_subscribe("sv.t.retired", b, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.retired");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 9;
    m.data = 8'h3c;
    h = pub.publish_serial(m);  // 快照已含 a、b；回调在后台链上稍后才跑
    sa.unsubscribe();           // 投递前退订：a 的在途投递会被注册表丢弃
    h.wait_done();
    check(b.blk_count == 1, "retired.others_unaffected");
    check(h.finished() == h.subscriber_count(), "retired.counted_as_terminal");
    check(h.retired() == 1, "retired.counted_as_retired");
    check(h.first_error() == "", "retired.not_an_error");
  endtask

  // ── 订阅先于绑定：SV 只带 tag 登记（不像 C++ 的具体 topic 订阅那样立即 topic_declare），
  //    topic 的首次绑定发生在第一次发布。此时 C++ 侧必须做反向校验，把"标签写错"变成一次
  //    明确报错，而不是让它此后每帧都悄无声息地不属于目标集——同一个错误不应因注册顺序不同
  //    而有时报错、有时永不报错。
  task automatic case_bind_conflict();
    automatic Rec                 rec   = new();
    automatic Rec                 other = new();
    automatic shuttle_sub         subx  = shuttle_subscribe_nb("sv.t.late", rec, "XMsg");
    automatic shuttle_sub         suby  = shuttle_subscribe_nb("sv.t.late2", other, "YMsg");
    automatic shuttle_pub #(XMsg) pubx  = new("sv.t.late");
    automatic shuttle_pub #(YMsg) puby  = new("sv.t.late");
    automatic shuttle_pub #(XMsg) pubx2 = new("sv.t.late2");
    automatic XMsg                mx    = new();
    automatic XMsg                mx2   = new();
    automatic YMsg                my    = new();
    automatic shuttle_handle      h;
    mx.addr  = 5;
    mx.data  = 6;
    mx2.addr = 7;
    my.addr  = 8;
    // 首次绑定就是错的类型：发布被拒（返回 null），帧未发布
    h = puby.publish_now(my, SHUTTLE_NOW_SKIP);
    check(h == null, "bind_conflict.rejected");
    check(shuttle_c_last_error() != "", "bind_conflict.has_error_text");
    check(rec.nb_count == 0, "bind_conflict.no_callback");
    // 失败的那次不改动任何状态：正确类型仍能绑定并投递
    h = pubx.publish_now(mx, SHUTTLE_NOW_SKIP);
    check(h != null, "bind_conflict.retry_ok");
    check(rec.nb_count == 1 && rec.last.addr == 32'd5, "bind_conflict.retry_delivered");
    // 反方向：订阅者声明 YMsg、发布者带 XMsg —— 同样在首次绑定时就拒
    h = pubx2.publish_now(mx2, SHUTTLE_NOW_SKIP);
    check(h == null, "bind_conflict.rejected_reverse");
    check(other.nb_count == 0, "bind_conflict.reverse_no_callback");
  endtask

  // ── topic 名不得含通配符（* / > 只属于订阅模式）────────────────────────────────
  // 只测发布侧：`publish_now` 失败是静默的（原因走 shuttle_c_last_error），不会像
  // serial/concurrent 那样 $error；订阅模式的语法校验由 C++ 的 broker_tests case14 覆盖。
  task automatic case_bad_topic();
    automatic shuttle_pub #(XMsg) bad = new("sv.t.bad.*");
    automatic shuttle_pub #(XMsg) sib = new("sv.t.bad.ok");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 2;
    // 通配符 topic：拒发、无句柄、留下原因（不能立起幻影 topic，也不能"像广播一样"扇出）
    h = bad.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h == null, "badtopic.no_handle");
    check(shuttle_c_last_error() != "", "badtopic.has_error_text");
    // 同一层级的合法名字不受影响
    h = sib.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h != null, "badtopic.sibling_ok");
  endtask

  task automatic case_unsubscribe();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe_nb("sv.t.unsub", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.unsub");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    m.addr = 1;
    m.data = 1;
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(rec.nb_count == 1, "unsub.before");
    sub.unsubscribe();
    sub.unsubscribe();  // 幂等
    h = pub.publish_now(m, SHUTTLE_NOW_SKIP);
    check(h.subscriber_count() == 0 && h.done() != 0, "unsub.empty_pub_completes");
    check(rec.nb_count == 1, "unsub.after");
  endtask

  // ── `publish_serial` 的快照语义：发布之后新增的订阅者不参与本帧 ───────────────────
  task automatic case_snapshot();
    automatic Rec                 first = new();
    automatic shuttle_sub         s1    = shuttle_subscribe("sv.t.snap", first, "XMsg");
    automatic shuttle_pub #(XMsg) pub   = new("sv.t.snap");
    automatic XMsg                m     = new();
    automatic shuttle_handle      h;
    automatic Rec                 late;
    automatic shuttle_sub         s2;
    m.addr = 32'h401;
    m.data = 0;
    h = pub.publish_serial(m, shuttle_trigger::after_delay(100_000_000));  // 100ns 后启动
    late = new();
    s2   = shuttle_subscribe("sv.t.snap", late, "XMsg");  // 触发前注册的新订阅者
    h.wait_done();
    check(first.blk_count == 1, "snapshot.first_received");
    check(late.blk_count == 0, "snapshot.late_sub_not_target");
  endtask

  // ── 跨语言往返：C++ 侧见 tests/cpp/xlang_tests.cpp ────────────────────
  task automatic case_xlang();
    automatic Rec                 rec       = new();
    automatic shuttle_sub         sub       = shuttle_subscribe("xlang.cpp2sv", rec, "XMsg");
    automatic shuttle_pub #(XMsg) ready_pub = new("xlang.sv_ready");
    automatic shuttle_pub #(XMsg) to_cpp    = new("xlang.sv2cpp");
    automatic XMsg                m         = new();
    automatic shuttle_handle      h;
    automatic bit                 ok;

    // 1) 就绪广播（retained）：C++ 侧无论先订后订都能收到
    ready_pub.set_retained(1);
    m.addr = 32'h1;
    m.data = 32'h0;
    h = ready_pub.publish_serial(m);
    h.wait_done();

    // 2) 等 C++ 侧把 3 帧发完（每帧 10us 步进，最多 50ms 仿真时间）
    for (int i = 0; i < 5000 && rec.blk_count < 3; i++) #(10us);

    check(rec.blk_count == 3, "xlang.cpp2sv.count");
    check(rec.order.size() == 3 && rec.order[0] == 1 && rec.order[1] == 2 && rec.order[2] == 3,
          "xlang.cpp2sv.fifo");
    if (rec.last != null) check(rec.last.data == 32'h300, "xlang.cpp2sv.payload");
    check(rec.seqs.size() == 3 && rec.seqs[0] < rec.seqs[1] && rec.seqs[1] < rec.seqs[2],
          "xlang.cpp2sv.seq_monotonic");

    // 3) 回发 3 帧；wait_done 返回即代表 C++ 侧回调已执行完
    for (int i = 0; i < 3; i++) begin
      m.addr = 32'h51 + i;
      m.data = 32'hA0 + i;
      h = to_cpp.publish_serial(m);
      h.wait_done();
      ok = (h.first_error() == "");
      check(ok, "xlang.sv2cpp.no_failure");
    end
  endtask

  // ── shuttle_event：set() 放行被它触发的投递，close() 释放 C++ 注册表里的 id ──
  task automatic case_event();
    automatic shuttle_event       ev  = new();
    automatic Rec                 rec = new();
    automatic shuttle_sub         sub = shuttle_subscribe("sv.t.ev", rec, "XMsg");
    automatic shuttle_pub #(XMsg) pub = new("sv.t.ev");
    automatic XMsg                m   = new();
    automatic shuttle_handle      h;
    check(ev.valid() == 1, "event.created");
    m.addr = 32'hE0;
    m.data = 32'h0;
    h = pub.publish_serial(m, shuttle_trigger::after_event(ev.id()));
    check(rec.blk_count == 0, "event.not_before_set");
    #(10ns);
    check(rec.blk_count == 0, "event.still_waiting");
    ev.set();
    h.wait_done();
    check(rec.blk_count == 1, "event.delivered_after_set");
    check(rec.last.addr == 32'hE0, "event.payload");
    ev.close();            // 走到 shuttle_c_event_destroy
    check(ev.valid() == 0, "event.closed");
    ev.set();              // 已关闭：必须是 no-op，不能碰已释放的 id
    sub.unsubscribe();
  endtask

  // ── SV 侧的用例驱动：先跑本地用例，再跑跨语言往返 ──────────────────────
  initial begin : sv_cases
    case_now_sync();
    case_now_blocking();
    case_publish_serial();
    case_publish_concurrent();
    case_nb_in_background();
    case_dual_registration();
    case_trigger();
    case_retained();
    case_multi_pub();
    case_unsubscribe();
    case_retired();
    case_bind_conflict();
    case_bad_topic();
    case_snapshot();
    case_event();
    case_xlang();
    g_sv_done = 1;
    $display("[sv] %0d passed, %0d failed", g_pass, g_fail);
  end

  // ── C++ 侧测试的启动与最终判定 ────────────────────────────────────────
  initial begin : run_cpp_tests
    automatic int status;
    weave_pkg::weave_run(status);
    if (status != 0) $fatal(1, "SHUTTLE CPP TESTS FAILED (status=%0d)", status);
    wait (g_sv_done);
    if (g_fail != 0) $fatal(1, "SHUTTLE SV TESTS FAILED (%0d failures)", g_fail);
    $display("SHUTTLE TESTS PASSED (%0d sv cases, cpp ok)", g_pass);
    $finish;
  end

endmodule
