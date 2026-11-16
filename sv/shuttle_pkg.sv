// ============================================================================
// shuttle_pkg.sv —— shuttle 发布-订阅框架的 SV 侧接口
//
// 依赖：weave_pkg（../weave/sv/weave_pkg.sv，由仿真脚本一并编译）。
// DPI 协议总表见 docs/dpi_protocol.md；方向约定与 weave 一致：
//   import（C++ 提供）：shuttle_c_*  —— 发布 / 订阅注册 / 完成等待 / 帧拉取
//   export（SV 提供） ：shuttle_sv_* —— 订阅者回调分发入口 + 仿真时间
//
// 帧拉取协议（dispatch 的 payload 不走 DPI 参数）：
//   C++ 调 shuttle_sv_dispatch[_nb] 前把当前帧字节流设为线程上下文；
//   SV 在 export 体内经 shuttle_c_frame_len / shuttle_c_frame_qword（8 字节
//   小端块）分块拉取。依赖仿真器单活跃进程不变性（与 weave 相同前提）。
//
// 回调入口模型（对应 C++ 端 shuttle::Capability）：
//   shuttle_sink    —— blocking 入口（export task，可消耗仿真时间），经 shuttle_subscribe 登记
//   shuttle_sink_nb —— nonblocking 入口（export function，零延时），经 shuttle_subscribe_nb 登记
//   一个订阅类可同时 implements 两者（interface class 多实现，对已有代码零侵入）；
//   此时"用哪个入口"由调用哪个注册函数决定，两个都要就注册两次。
//
// 消息类约定（无需继承任何基类，供 shuttle_pub#(T) 使用）：
//   class Txn;
//     int unsigned addr;                 // 成员类型须属于编码子集（见协议表 §线上格式）
//     int unsigned data;
//     localparam string type_tag = "Txn";         // 与 C++ 端 SHUTTLE_REFLECT 的名字一致
//     function automatic void pack(output byte q[$]);  // 按声明顺序流式打包（大端线序）
//       q = {>>{addr, data}};   // ← 必须用 {>>{...}}（1 位切片，从高位向低位）
//     endfunction
//     static function automatic Txn unpack(byte b[]);
//       Txn t = new();
//       {>>{t.addr, t.data}} = b;
//       return t;
//     endfunction
//   endclass
// 注意：string 字段须先流式写入 32 位长度前缀再写字节内容，即 {>>{32'(s.len()), s}}，
//       与 C++ 端 Codec 对齐。
// 线序提示：{>>{...}} 得到"字段声明序 + 每个字段高位在前"的字节流，正好等于
//           C++ Writer 的大端写法；{<<{...}} 会把整段比特流反向，切勿混用。
// ============================================================================

package shuttle_pkg;
  // timeunit 约定与 weave_pkg 一致：决定 #() 与 1s 字面量的换算基准
  //（shuttle_sv_time_fs 用 $realtime/1s*1e15 换算飞秒）。
  timeunit 1ps; timeprecision 1ps;

  // ── DPI：import（由 C++ 的 libshuttle.so 提供）───────────────────────────
  // 全部为 context import：实现内可能回调下面的 export（dispatch / time）。
  import "DPI-C" context function int shuttle_c_publish_now(
      string topic, string type_tag, byte payload[], int now_policy);
  import "DPI-C" context function int shuttle_c_publish_serial(
      string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg);
  import "DPI-C" context function int shuttle_c_publish_concurrent(
      string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg);
  import "DPI-C" context task shuttle_c_wait_pub(
      int pub_id, longint timeout_fs, output int status);
  import "DPI-C" context function int shuttle_c_pub_done(int pub_id);
  // 发布计数查询：kind 0=目标数 1=已完成 2=排除 3=deferred 4=类型过滤
  import "DPI-C" context function int shuttle_c_pub_stat(int pub_id, int kind);
  import "DPI-C" context function string shuttle_c_pub_error(int pub_id);
  // 在途投递到达一个已退订的 id 时回告 C++（不报错，只让 C++ 把该次投递记成失败）
  import "DPI-C" context function void shuttle_c_sub_retired(int sv_sid);
  import "DPI-C" context function int shuttle_c_subscribe(
      string topic, int caps, string type_tag, int sv_sid);
  import "DPI-C" context function void shuttle_c_unsubscribe(int sv_sid);
  import "DPI-C" context function void shuttle_c_set_retained(string topic, bit retained);
  import "DPI-C" context function string shuttle_c_last_error();

  // 帧拉取协议：C++ 调 shuttle_sv_dispatch[_nb] 前把当前帧字节流设为线程上下文，
  // SV 在 export 体内经这两个标量 import 分块拉取（每块 8 字节小端序）。
  import "DPI-C" context function int shuttle_c_frame_len();
  import "DPI-C" context function longint shuttle_c_frame_qword(int index);

  // 跨语言事件：C++ 侧 shuttle::Event 注册表的 DPI 入口（shuttle_event 使用）
  import "DPI-C" context function int shuttle_c_event_create();
  import "DPI-C" context function void shuttle_c_event_set(int ev_id);
  import "DPI-C" context function void shuttle_c_event_destroy(int ev_id);

  // ── DPI：export（本包提供，供 C++ broker 回调）───────────────────────────
  // 与 weave 相同的先例：export 放在 package 内（VCS 已验证）；
  // 若目标仿真器不接受 package 内 export，将这两个入口挪到 module 中即可，协议不变。
  export "DPI-C" task shuttle_sv_dispatch;
  export "DPI-C" function shuttle_sv_dispatch_nb;
  export "DPI-C" function shuttle_sv_time_fs;

  // ── 常量：与 C++ 端 enum 严格对齐（docs/dpi_protocol.md §常量表）─────────
  // 订阅入口（shuttle_c_subscribe 的 caps 参数）：一次注册只可能是其中之一
  // （同一个对象注册两次 = 两个订阅者，才会两个值都用上）
  localparam int SHUTTLE_CAP_BLOCKING    = 1;  // blocking 入口：shuttle_subscribe
  localparam int SHUTTLE_CAP_NONBLOCKING = 2;  // nonblocking 入口：shuttle_subscribe_nb
  // publish_now 遇到入口为 blocking 的订阅者时的策略（shuttle_c_publish_now 的 now_policy 参数）
  localparam int SHUTTLE_NOW_REJECT = 0;  // 报错（不投递任何订阅者）
  localparam int SHUTTLE_NOW_DROP   = 1;  // 跳过这些订阅者（计入诊断计数）
  localparam int SHUTTLE_NOW_DEFER  = 2;  // 默认：交给本 topic 的后台串行链，稍后回调
  // 触发器种类（shuttle_c_publish_serial / _concurrent 的 trig_kind 参数）
  localparam int SHUTTLE_TRIG_NONE  = 0;  // 立即启动
  localparam int SHUTTLE_TRIG_DELAY = 1;  // trig_arg = 仿真延时（飞秒）
  localparam int SHUTTLE_TRIG_EVENT = 2;  // trig_arg = C++ 侧事件句柄 id（weave/shuttle Event）
  localparam int SHUTTLE_TRIG_PUB   = 3;  // trig_arg = 另一个 Publication 的 pub_id
  // shuttle_c_wait_pub 的 status 输出
  localparam int SHUTTLE_WAIT_OK      = 0;  // 全部完成（不代表无订阅者失败）
  localparam int SHUTTLE_WAIT_TIMEOUT = 1;
  localparam int SHUTTLE_WAIT_FAILED  = 2;  // 存在订阅者失败（详情 shuttle_c_pub_error）
                                            // 或内部错误（详情 shuttle_c_last_error）

  // ── 元数据：随投递传递，与 C++ 端 shuttle::Meta 一致 ─────────────────────
  class shuttle_meta;
    string  topic;
    string  type_tag;
    longint seq;
    longint time_fs;
    function new(string topic, string type_tag, longint seq, longint time_fs);
      this.topic   = topic;
      this.type_tag = type_tag;
      this.seq     = seq;
      this.time_fs = time_fs;
    endfunction
  endclass

  // ── 帧拉取：C++ dispatch 上下文的字节流 → SV 动态数组 ────────────────────
  // 协议：shuttle_c_frame_qword(i) 返回第 i 块 8 字节（小端序），拼接即完整帧。
  // 依赖单活跃进程不变性：仅可在 shuttle_sv_dispatch[_nb] 执行体内调用。
  function automatic void shuttle_frame_bytes(output byte b[]);
    int n = shuttle_c_frame_len();
    b = new [n];
    for (int base = 0; base < n; base += 8) begin
      longint qw = shuttle_c_frame_qword(base / 8);
      for (int j = 0; j < 8 && base + j < n; j++)
        b[base + j] = byte'(qw[j * 8 +: 8]);
    end
  endfunction

  // ── 回调接口：已有类 implements 即可接入，不占用单继承名额 ──────────────
  interface class shuttle_sink;
    // 可阻塞回调：blocking 入口的落地实现——`serial`/`concurrent` 的投递单元与 `publish_now`
    // 的 defer 段都调它（在后台仿真进程中执行，可消耗时间）
    pure virtual task invoke(input byte payload[], input shuttle_meta meta);
  endclass

  interface class shuttle_sink_nb;
    // 不可阻塞回调：nonblocking 入口的落地实现——`publish_now` 在发布者进程内同步调它，
    // `serial`/`concurrent` 也用它（编译器强制零阻塞）
    pure virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
  endclass

  // ── 订阅注册表：sub_id → 回调对象 ────────────────────────────────────────
  // DPI export 的名字必须静态，故 C++ 回调统一经 shuttle_sv_dispatch[_nb] 进入，
  // 再由本注册表按 sub_id 分发到用户对象。
  class shuttle_registry;
    static local int            next_id_  = 1;
    static local shuttle_sink    sinks_[int];
    static local shuttle_sink_nb sinks_nb_[int];
    // 已退订的 id：C++ 侧可能仍有在途投递（发布瞬间已快照订阅者集合），
    // 这些投递到达时静默丢弃（不当成 SV 侧错误），但经 shuttle_c_sub_retired 回告 C++，
    // 让该帧记为该订阅者的失败（而不是幻影般的 delivered）；未登记过的 id 仍然报错。
    static local bit            retired_[int];

    static function automatic int add(shuttle_sink s, shuttle_sink_nb snb);
      int id = next_id_++;
      if (s   != null) sinks_[id]    = s;
      if (snb != null) sinks_nb_[id] = snb;
      return id;
    endfunction

    static function automatic void remove(int id);
      sinks_.delete(id);
      sinks_nb_.delete(id);
      retired_[id] = 1;
    endfunction

    // 阻塞分发（`serial`/`concurrent`）：由 exported task shuttle_sv_dispatch 调用
    static task automatic dispatch(int id, byte payload[], shuttle_meta meta);
      if (!sinks_.exists(id)) begin
        // 退订后的在途投递：SV 侧静默丢弃（不 $error），但必须回告 C++，
        // 否则 C++ 会把这次投递记成 delivered（幻影成功）。
        if (retired_.exists(id)) begin
          shuttle_c_sub_retired(id);
          return;
        end
        $error("shuttle: 订阅者 %0d 不存在或不具备阻塞能力", id);
        return;
      end
      sinks_[id].invoke(payload, meta);
    endtask

    // 非阻塞分发（`publish_now`）：由 exported function shuttle_sv_dispatch_nb 调用
    static function automatic void dispatch_nb(int id, byte payload[], shuttle_meta meta);
      if (!sinks_nb_.exists(id)) begin
        if (retired_.exists(id)) begin
          shuttle_c_sub_retired(id);
          return;
        end
        $error("shuttle: 订阅者 %0d 不具备非阻塞能力（publish_now 不应分发至此）", id);
        return;
      end
      sinks_nb_[id].invoke_nb(payload, meta);
    endfunction
  endclass

  // ── 订阅句柄 ────────────────────────────────────────────────────────────
  class shuttle_sub;
    local int id_;
    local bit active_;
    function new(int id);
      id_ = id;
      active_ = 1;
    endfunction
    function int id();
      return id_;
    endfunction
    // 显式退订（幂等）；退订后不再收到任何投递（进行中的投递不受影响）
    function void unsubscribe();
      if (!active_) return;
      active_ = 0;
      shuttle_c_unsubscribe(id_);
      shuttle_registry::remove(id_);
    endfunction
  endclass

  // ── 订阅入口 ────────────────────────────────────────────────────────────
  // ── 订阅：入口由"调用哪个函数"决定 ──────────────────────────────────────
  // 与 C++ 的 subscribe(topic, fn) / subscribe(topic, nb(fn)) 一一对应：
  //   shuttle_subscribe    → 登记 blocking 入口（export task，可消耗仿真时间）
  //   shuttle_subscribe_nb → 登记 nonblocking 入口（export function，零延时，编译器强制）
  // 一个对象要同时拿到两个入口就注册两次（两个订阅者、每帧两次回调）；
  // type_tag 非空时与 topic 的类型绑定做校验，不匹配在 C++ 侧报错。
  // 想要"同一入口的两种行为"就建两个对象——SV 没有闭包，这等价于 C++ 的两个 lambda。
  // 入口不再从"类实现了哪些 interface class"自动推断（旧实现的 $cast 探测正是导致
  // 同一份配置在 C++ / SV 里 subscriber_count 与 `publish_now` 行为不一致的根源）。
  // 内部：两个公开注册函数的公共实现（VCS 2018.09 不接受 package 里的 local function，
  // 故不加 local，靠命名约定标为内部）
  function automatic shuttle_sub shuttle_register(string topic, shuttle_sink s,
                                                  shuttle_sink_nb snb, string type_tag,
                                                  int caps);
    int                   sid = shuttle_registry::add(s, snb);
    automatic shuttle_sub sub;
    // SV 本地编号经 sv_sid 传给 C++，dispatch 时 C++ 原样带回，两侧无需维护映射
    if (shuttle_c_subscribe(topic, caps, type_tag, sid) != 0) begin
      shuttle_registry::remove(sid);
      $error("shuttle: 订阅失败（topic=%s）：%s", topic, shuttle_c_last_error());
      return null;
    end
    sub = new(sid);
    return sub;
  endfunction

  // blocking 入口（SV：export task）：≙ C++ subscribe(topic, fn)
  function automatic shuttle_sub shuttle_subscribe(string topic, shuttle_sink cb,
                                                   string type_tag = "");
    return shuttle_register(topic, cb, null, type_tag, SHUTTLE_CAP_BLOCKING);
  endfunction

  // nonblocking 入口（SV：export function）：≙ C++ subscribe(topic, nb(fn))
  function automatic shuttle_sub shuttle_subscribe_nb(string topic, shuttle_sink_nb cb,
                                                      string type_tag = "");
    return shuttle_register(topic, null, cb, type_tag, SHUTTLE_CAP_NONBLOCKING);
  endfunction

  // ── 发布完成句柄（对应 C++ 端 shuttle::Publication）──────────────────────
  // SV 无异常机制：订阅者失败通过 first_error() 返回错误信息，由用户决定处置。
  class shuttle_handle;
    local int pub_id_;
    function new(int pub_id);
      pub_id_ = pub_id;
    endfunction
    function int id();
      return pub_id_;
    endfunction
    function bit valid();
      return pub_id_ > 0;
    endfunction
    function bit done();
      return valid() && shuttle_c_pub_done(pub_id_) != 0;
    endfunction
    // 阻塞至全部订阅者回调完成；timeout_fs = 0 表示无限等待（单位：飞秒）
    task wait_done(longint timeout_fs = 0);
      int status;
      if (!valid()) return;
      shuttle_c_wait_pub(pub_id_, timeout_fs, status);
      if (status == SHUTTLE_WAIT_TIMEOUT)
        $error("shuttle: 等待 Publication %0d 超时", pub_id_);
      else if (status == SHUTTLE_WAIT_FAILED)
        $error("shuttle: Publication %0d 存在订阅者失败：%s", pub_id_, first_error());
    endtask
    // ── 可见性计数（与 C++ Publication 同名语义）──────────────────────────
    // 本帧的目标订阅者数（发布瞬间快照，含被 defer 的部分）
    function int subscriber_count();
      return valid() ? shuttle_c_pub_stat(pub_id_, 0) : 0;
    endfunction
    // 已结束（成功或失败）的订阅者数
    function int finished();
      return valid() ? shuttle_c_pub_stat(pub_id_, 1) : 0;
    endfunction
    // 被策略排除、本帧不会投递给它们的订阅者数
    function int excluded();
      return valid() ? shuttle_c_pub_stat(pub_id_, 2) : 0;
    endfunction
    // 被推迟到后台串行链、稍后才投递的订阅者数
    function int deferred();
      return valid() ? shuttle_c_pub_stat(pub_id_, 3) : 0;
    endfunction
    // 因类型标签不匹配而跳过的订阅者数（已计入 finished）
    function int filtered();
      return valid() ? shuttle_c_pub_stat(pub_id_, 4) : 0;
    endfunction
    // 第一个失败订阅者的错误信息（空串 = 无失败）
    function string first_error();
      if (!valid()) return "";
      return shuttle_c_pub_error(pub_id_);
    endfunction
  endclass

  // ── 跨语言事件句柄（对应 C++ 端 shuttle::Event）─────────────────────────
  // 底层对象驻留在 C++ 侧的注册表里（由 C++ 持有，SV 只持 id）：
  // C++ 侧可经 shuttle::after(Event) / TRIG_EVENT 等待，SV 侧 set() 唤醒；
  // 也可作 shuttle_trigger::after_event() 的触发条件（见下）。
  class shuttle_event;
    local int id_;
    local bit active_;
    function new();
      id_ = shuttle_c_event_create();
      active_ = (id_ > 0);
      if (!active_) $error("shuttle: 创建事件失败：%s", shuttle_c_last_error());
    endfunction
    function int id();
      return id_;
    endfunction
    function bit valid();
      return active_;
    endfunction
    // 唤醒所有等待者（C++ 侧等待本事件的 Trigger / TRIG_EVENT 任务被放行）
    function void set();
      if (active_) shuttle_c_event_set(id_);
    endfunction
    // 主动释放（仿真结束可不调用，随进程消亡）
    function void close();
      if (!active_) return;
      active_ = 0;
      shuttle_c_event_destroy(id_);
    endfunction
  endclass

  // ── 延迟触发器（对应 C++ 端 shuttle::Trigger）────────────────────────────
  // 快照语义与 C++ 一致：发布调用瞬间锁定订阅者集合与消息拷贝，
  // 触发器只决定后台回调进程何时启动。
  class shuttle_trigger;
    int     kind = SHUTTLE_TRIG_NONE;
    longint arg  = 0;
    static function shuttle_trigger none();
      shuttle_trigger t = new();
      return t;
    endfunction
    // 仿真延时（飞秒）
    static function shuttle_trigger after_delay(longint fs);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_DELAY;
      t.arg  = fs;
      return t;
    endfunction
    // C++ 侧事件句柄 id（shuttle_event / C++ shuttle::Event / weave::Event id）。
    // 传 shuttle_event 时显式取 id：after_event(ev.id())
    //（类句柄与 int 只支持隐式单项转换，VCS 不接受二者构成重载）
    static function shuttle_trigger after_event(int ev_id);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_EVENT;
      t.arg  = ev_id;
      return t;
    endfunction
    // 另一个 Publication 完成（成功或失败均视为完成）
    static function shuttle_trigger after_pub(shuttle_handle h);
      shuttle_trigger t = new();
      t.kind = SHUTTLE_TRIG_PUB;
      t.arg  = h.id();
      return t;
    endfunction
  endclass

  // ── 类型化发布器 ────────────────────────────────────────────────────────
  // 类型参数 T 须满足文件头部的消息类约定（pack() + localparam type_tag）。
  class shuttle_pub #(type T = byte);
    local string topic_;
    function new(string topic);
      topic_ = topic;
    endfunction

    // 队列 → 动态数组（DPI open array 形参要求动态数组；VCS 函数返回类型不支持 unpacked 数组）
    local function automatic void q2a(const ref byte q[$], output byte a[]);
      a = new [q.size()];
      foreach (q[i]) a[i] = q[i];
    endfunction

    // ── 发布三件套：名字与 C++ 侧 publish_now / publish_serial / publish_concurrent
    //    一一对应；调度方式写在函数名里，不再有 mode 参数 ────────────────────────

    // 消息 → DPI open array 需要的动态数组（pack() 产出队列，见文件头的消息类约定）
    local function void pack_payload(T msg, output byte payload[]);
      byte q[$];
      msg.pack(q);
      q2a(q, payload);
    endfunction

    // 延迟触发器 → (trig_kind, trig_arg) 两个标量；null 表示立即启动
    local function void trig_scalars(shuttle_trigger trig, output int kind, output longint arg);
      kind = SHUTTLE_TRIG_NONE;
      arg  = 0;
      if (trig != null) begin
        kind = trig.kind;
        arg  = trig.arg;
      end
    endfunction

    // pub_id → 句柄；失败返回 null。loud=1 时额外 $error 提示。
    // `publish_now` 保持静默（reject 策略下失败是预期结果，原因走 shuttle_c_last_error()）；
    // `serial`/`concurrent` 打印，因为它们是"发了就不管"的异步接口，出错必须立刻可见。
    local function shuttle_handle wrap(int pid, string what, bit loud);
      if (pid <= 0) begin
        if (loud)
          $error("shuttle: %s 发布失败（topic=%s）：%s", what, topic_, shuttle_c_last_error());
        return null;
      end
      begin
        shuttle_handle h = new(pid);
        return h;
      end
    endfunction

    // `publish_now`：发布者栈内同步回调，全程不挂起（执行所有入口为 nonblocking 的订阅者）。
    // policy：入口为 blocking（无法在调用栈内被调用，即 shuttle_sink 的 invoke task）的
    //         那些目标怎么处置，默认 SHUTTLE_NOW_DEFER（转后台串行链，稍后回调）。
    //         — 一条注册只有一条入口，处置只看该策略，与本 topic 有无 nonblocking 订阅者无关。
    // 返回本帧句柄（与 `serial`/`concurrent` 同一类型）：nonblocking 部分返回时已全部结束，被 drop 的计入
    // excluded()，被 defer 的仍在后台链上——done()/wait_done() 覆盖这部分。
    // 失败返回 null（原因见 shuttle_c_last_error()）。
    function shuttle_handle publish_now(T msg, int policy = SHUTTLE_NOW_DEFER);
      byte payload[];
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_now(topic_, T::type_tag, payload, policy), "now", 1'b0);
    endfunction

    // `publish_serial`：后台单链顺序回调（topic 级 FIFO），立即返回完成句柄。
    function shuttle_handle publish_serial(T msg, shuttle_trigger trig = null);
      byte payload[];
      int kind;
      longint arg;
      trig_scalars(trig, kind, arg);
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_serial(topic_, T::type_tag, payload, kind, arg), "serial", 1'b1);
    endfunction

    // `publish_concurrent`：后台每订阅者一任务并发回调，立即返回完成句柄。
    function shuttle_handle publish_concurrent(T msg, shuttle_trigger trig = null);
      byte payload[];
      int kind;
      longint arg;
      trig_scalars(trig, kind, arg);
      pack_payload(msg, payload);
      return wrap(shuttle_c_publish_concurrent(topic_, T::type_tag, payload, kind, arg),
                  "concurrent", 1'b1);
    endfunction

    // retained（latched）：开启后 broker 缓存最新一帧，新订阅者立即收到
    function void set_retained(bit r);
      shuttle_c_set_retained(topic_, r);
    endfunction

  endclass

  // ── DPI export 实现：C++ broker 的回调入口 ──────────────────────────────
  // `serial`/`concurrent` 的分发：由 C++ 的 weave task 调用，
  // 本 task 在承载该 weave task 的 SV 进程中执行，可合法消耗仿真时间。
  // payload 经帧拉取协议取得（见文件头部说明）。
  task automatic shuttle_sv_dispatch(input int sub_id, input string topic, input string type_tag,
                                     input longint seq, input longint time_fs);
    shuttle_meta m = new(topic, type_tag, seq, time_fs);
    byte payload[];
    shuttle_frame_bytes(payload);
    shuttle_registry::dispatch(sub_id, payload, m);
  endtask

  // `publish_now` 的分发：在发布者的进程上下文中执行，必须零延时（function 由编译器强制）。
  // payload 经帧拉取协议取得。
  function automatic void shuttle_sv_dispatch_nb(input int sub_id, input string topic,
                                                 input string type_tag, input longint seq,
                                                 input longint time_fs);
    shuttle_meta m = new(topic, type_tag, seq, time_fs);
    byte payload[];
    shuttle_frame_bytes(payload);
    shuttle_registry::dispatch_nb(sub_id, payload, m);
  endfunction

  // 仿真时间源（C++ 端 shuttle::now_fs() 的实现）。
  // $realtime 以本包 timeunit 为单位；换算为飞秒。
  // 注意：real 仅 53bit 尾数，仿真时间很大时精度退化 ——
  // 正式实现应改为 $time × 每 tick 飞秒数（与 weave_pkg 的 tick 约定对齐）。
  function automatic longint shuttle_sv_time_fs();
    return longint'($realtime / 1s * 1e15);
  endfunction

endpackage
