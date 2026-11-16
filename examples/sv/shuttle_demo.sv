// ============================================================================
// shuttle_demo.sv —— shuttle 的 SV 侧用户接口示范
//
// 编译顺序：weave_pkg.sv（../weave/sv）→ shuttle_pkg.sv（../../sv）→ 本文件。
// 运行：仿真器经 -sv_lib 加载 libshuttle_examples_svdpi.so（间接带入
//       libshuttle.so / libweave.so）；weave_run 会一并启动 C++ 侧注册的
//       示例（WEAVE_TEST，见 ../cpp/），两侧的发布/订阅在同一 broker 上互通：
//       本文件订阅的 "uart0.tx" 同时能收到 C++ basic_demo 发布的消息，反之亦然。
// ============================================================================

module shuttle_demo;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;

  // ── 消息类：与 C++ 端 struct UartTx + SHUTTLE_REFLECT(UartTx, addr, data) 对等 ──
  // 约定（见 sv/shuttle_pkg.sv 文件头）：
  //   · 成员类型属于编码子集，字段顺序与 C++ 端一致；
  //   · pack()/unpack() 用 {>>{...}} 流式打包（声明序、每字段高位在前），
  //     与 C++ Writer 的大端线序逐字节一致；
  //   · localparam type_tag 与 SHUTTLE_REFLECT 的类型名一致，发布时校验。
  class UartTx;
    int unsigned addr;
    int unsigned data;
    localparam string type_tag = "UartTx";

    function automatic void pack(output byte q[$]);
      q = {>>{addr, data}};
    endfunction

    static function automatic UartTx unpack(byte b[]);
      UartTx t = new();
      {>>{t.addr, t.data}} = b;
      return t;
    endfunction
  endclass

  // ── 订阅者：interface class 多实现，不占用单继承名额 ─────────────────────
  // implements shuttle_sink     → blocking 入口（export task），经 shuttle_subscribe 登记：
  //                              `serial`/`concurrent` 的投递单元与 `publish_now` 的 defer 段都调它；
  // implements shuttle_sink_nb  → nonblocking 入口（export function），经 shuttle_subscribe_nb 登记：
  //                              `publish_now` 在发布者栈内同步调它，`serial`/`concurrent` 也用它。
  // 两条入口都想拿到就注册两次（与 C++ 的 subscribe(fn) + subscribe(nb(fn)) 同构）。
  class Monitor implements shuttle_sink, shuttle_sink_nb;
    UartTx last;
    int    nb_count = 0;

    // 阻塞回调：在 broker 的后台仿真进程中执行，允许消耗仿真时间
    virtual task invoke(input byte payload[], input shuttle_meta meta);
      UartTx tx = UartTx::unpack(payload);
      $display("[monitor] seq=%0d addr=0x%08h data=0x%08h", meta.seq, tx.addr, tx.data);
      #(10ns);
    endtask

    // 不可阻塞回调：`publish_now` 在发布者进程内同步执行，function 由编译器强制零延时
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      last = UartTx::unpack(payload);
      nb_count++;
    endfunction
  endclass

  initial begin : demo
    automatic Monitor      mon = new();
    // 订阅时 type_tag 与 topic 的类型绑定校验；C++ Publisher<UartTx> 已绑定则必须一致
    automatic shuttle_sub  sub  = shuttle_subscribe   ("uart0.tx", mon, UartTx::type_tag);
    automatic shuttle_sub  subn = shuttle_subscribe_nb("uart0.tx", mon, UartTx::type_tag);
    automatic shuttle_pub #(UartTx) pub = new("uart0.tx");
    automatic shuttle_handle h;
    automatic UartTx       tx  = new();

    // ── `publish_now`：发布者栈内同步回调，发布进程全程不挂起 ───────────────────
    // 入口为 nonblocking 的那次注册被同步调用（invoke_nb）；入口为 blocking 的那次注册按策略处置——
    // 这里用 skip 跳过它，不传策略则默认 defer（转后台串行链，稍后再调 invoke）。
    tx.addr = 32'h10; tx.data = 32'hA5;
    begin
      automatic shuttle_handle hn = pub.publish_now(tx, SHUTTLE_NOW_SKIP);
      if (hn == null) $error("publish_now 发布失败: %s", shuttle_c_last_error());
      else if (hn.skipped() != 1)
        $error("publish_now 应当跳过 1 个 blocking 入口订阅者（实际 %0d）", hn.skipped());
    end
    if (mon.last.addr !== 32'h10)
      $error("publish_now 未同步投递");

    // ── `publish_serial`：后台单进程顺序回调，立即返回句柄 ─────────────────────────
    tx.addr = 32'h11; tx.data = 32'h5A;
    h = pub.publish_serial(tx);
    // 需要完成语义时等待：经 weave_sv_wait 挂起本进程，native 进程可直接调用
    h.wait_done();

    // ── `publish_concurrent`：后台每订阅者一进程并发回调 + 延迟触发 ────────────────────
    // 100ns = 1e8 fs 后才启动各订阅者的回调进程；订阅者集合与消息在调用瞬间锁定
    tx.addr = 32'h12; tx.data = 32'h3C;
    h = pub.publish_concurrent(tx, shuttle_trigger::after_delay(100_000_000));
    h.wait_done();
    if (h.first_error() != "")
      $error("存在订阅者失败: %s", h.first_error());

    // ── 触发器其他形态 ──────────────────────────────────────────────────
    // shuttle_trigger::after_event(ev_id) —— 等待 C++ 侧 weave::Event（句柄 id）
    // shuttle_trigger::after_pub(h)       —— 跟随另一个 Publication 完成
    // 立即发布等价于 shuttle_trigger::none() 或省略 trig 参数。

    // ── retained（latched）：开启后新订阅者在注册时补发最新一帧（这里只开开关）────
    pub.set_retained(1);

    // ── 一对多：多个发布器实例共享同一 topic 与订阅者集合 ──────────────────
    // （SV 同 C++：多个 shuttle_pub#(UartTx) 实例都绑定 "uart0.tx" 类型标签，
    //  各自发布、订阅者各自独立收到每个发布帧）
    begin
      automatic shuttle_pub #(UartTx) pub_b = new("uart0.tx");
      tx.addr = 32'h15; tx.data = 32'h55;
      begin
        automatic shuttle_handle hn = pub_b.publish_now(tx, SHUTTLE_NOW_SKIP);
        if (hn == null) $error("多发布器 publish_now 失败: %s", shuttle_c_last_error());
      end
      if (mon.last.addr !== 32'h15)
        $error("多发布器未投递");
    end

    // ── 退订（幂等）─────────────────────────────────────────────────────
    subn.unsubscribe();
    sub.unsubscribe();
  end

  // ── C++ 侧示例的统一启动入口 ────────────────────────────────────────────
  // C++ 示例注册为 WEAVE_TEST（见 ../cpp/*.cpp），由 weave 根任务串行执行。
  initial begin : run_cpp_demos
    automatic int status;
    weave_pkg::weave_run(status);
    if (status != 0) $fatal(1, "SHUTTLE CPP DEMO FAILED (status=%0d)", status);
    $display("SHUTTLE CPP DEMO PASSED");
  end

endmodule
