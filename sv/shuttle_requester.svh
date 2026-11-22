`ifndef SHUTTLE_REQUESTER_SVH
`define SHUTTLE_REQUESTER_SVH

// ── Requester: the requester's half of request/response ────────────────
// Counterpart of shuttle::Requester in include/shuttle/requester.hpp. A request
// goes out with a correlation id allocated by shuttle_c_take_corr_id() — the
// same sequence the C++ side draws from — every reply carries that id back in
// shuttle_meta::corr_id, and the requester routes replies to the call that
// asked for them.
//
// The two DPI entries this leans on are the reason the class is not pure SV: the
// id counter and the response-channel mark are broker-level state, and an SV
// requester must not mint ids of its own or the two languages could hand the
// same one to two calls on a shared response topic.
//
// Usage:
//   shuttle_requester #(XRqReq, XRqResp) rq = new ("sv.rq.req", "sv.rq.resp");
//   if (!rq.valid()) $fatal(1, "%s", rq.last_error());
//   shuttle_call #(XRqResp) call = rq.send_now(req);
//   XRqResp reply;
//   bit ok;
//   if (call != null) call.recv(ok, reply, 1_000_000);  // 1us, package ticks
//
// Concurrency model: exactly one process runs at a time and suspends only at
// weave blocking points, so a reply published by a nonblocking responder lands
// inside the send_* call itself (queued() is already 1 when send_* returns).
// recv / wait_for / wait_all never suspend when a reply is already queued, and a
// reply already delivered is never lost to a deadline.

// ── Demux target base ──────────────────────────────────────────────────
// A subscription callback has to be a handlable object, but the reply queue
// belongs to the parameterized call, so the requester exposes one virtual hook
// that its own subclass overrides: the sink (non-parameterized) forwards into it.
class shuttle_reply_target;
  virtual function void accept_response(input byte payload[], input int unsigned corr_id);
    // Intentionally empty: shuttle_requester overrides it.
  endfunction
endclass

// Nonblocking response subscription callback: route the frame by corr_id.
// It runs on the responder's stack, so accepting is pure bookkeeping — a frame
// whose id matches no call is an expected event (cancelled or timed-out call),
// counted by the requester, never reported.
class shuttle_response_sink implements shuttle_sink_nb;
  local shuttle_reply_target link_;
  function new(shuttle_reply_target link);
    link_ = link;
  endfunction
  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
    if (link_ != null) link_.accept_response(payload, meta.corr_id);
  endfunction
endclass

// ── Call: one outstanding request and the replies belonging to it ──────
class shuttle_call #(type TResp);
  local int unsigned   corr_id_;
  local int            ev_;      // weave event: a reply arrived / the call was cancelled
  local TResp          items[$]; // replies not taken yet
  local bit            cancelled_;
  local shuttle_handle pub_;

  function new(int unsigned corr_id);
    corr_id_   = corr_id;
    ev_        = weave_sv_create_event();
    cancelled_ = 0;
  endfunction

  function int unsigned corr_id();
    return corr_id_;
  endfunction

  // The request frame: subscriber_count() (what wait_all() asks), done() and
  // wait_done() live here. Null until the request publish succeeded.
  function shuttle_handle publication();
    return pub_;
  endfunction

  function int queued();
    return items.size();
  endfunction

  function bit cancelled();
    return cancelled_;
  endfunction

  // ── internal: called by shuttle_requester ─────────────────────────────
  // Attached after the publish call: the call slot has to exist before the
  // frame goes out, because a nonblocking responder answers inside it.
  function void attach(shuttle_handle h);
    pub_ = h;
  endfunction

  // Delivery path — already routed here by corr_id.
  function void push(input byte payload[]);
    items.push_back(TResp::unpack(payload));
    weave_sv_trigger(ev_);
  endfunction

  function TResp take();
    return items.pop_front();
  endfunction

  // Blocks until a reply is queued, the call is cancelled, or the deadline
  // passes; has_deadline == 0 waits forever. The queue is checked first, so a
  // reply that is already there never turns into a timeout.
  local task automatic wait_nonempty(output bit ok, input bit has_deadline, input time deadline);
    int     status;
    longint ticks;
    ok = 0;
    forever begin
      if (items.size() > 0) begin
        ok = 1;
        return;
      end
      if (cancelled_) return;
      if (has_deadline) begin
        if ($time >= deadline) return;
        ticks = longint'(deadline - $time);  // package timeunit is 1ps: 1 tick == 1ps
      end else begin
        ticks = -1;
      end
      weave_sv_wait(ev_, ticks, -1, status);
    end
  endtask

  // Next reply. ok == 0 means the deadline passed or the call was cancelled
  // (SV has no TimeoutError to throw); timeout == 0 waits forever.
  task automatic recv(output bit ok, output TResp msg, input time timeout = 0);
    bit  has_deadline;
    time deadline;
    has_deadline = (timeout > 0);
    deadline     = has_deadline ? ($time + timeout) : 0;
    wait_nonempty(ok, has_deadline, deadline);
    if (!ok) return;
    msg = take();
  endtask

  // Up to n replies before one deadline: the whole wait is bounded, not each
  // reply — same contract as Call::wait_for on the C++ side.
  task automatic wait_for(input int n, output TResp out[$], input time timeout = 0);
    bit  ok;
    bit  has_deadline;
    time deadline;
    out.delete();
    has_deadline = (timeout > 0);
    deadline     = has_deadline ? ($time + timeout) : 0;
    while (out.size() < n) begin
      wait_nonempty(ok, has_deadline, deadline);
      if (!ok) break;
      out.push_back(take());
    end
  endtask

  // Every target is expected to answer exactly once (the request/response
  // convention), so the count is the request frame's target set, fixed when it
  // was published — a responder that never answers shows up as a timeout here,
  // not as an endless wait.
  task automatic wait_all(output TResp out[$], input time timeout = 0);
    int expected;
    expected = (pub_ != null) ? pub_.subscriber_count() : 0;
    wait_for(expected, out, timeout);
  endtask

  // Stop accepting replies: queued ones are dropped, a suspended recv / wait_*
  // returns ok == 0, and whatever still arrives is counted as stale by the
  // requester. Deterministic on purpose — a call nobody waits for any more is
  // finished explicitly, not by luck.
  function void cancel();
    if (cancelled_) return;
    cancelled_ = 1;
    items.delete();
    weave_sv_trigger(ev_);
  endfunction
endclass

// ── Requester ──────────────────────────────────────────────────────────
class shuttle_requester #(type TReq, type TResp) extends shuttle_reply_target;
  local string                    req_topic_;
  local string                    resp_topic_;
  local shuttle_publisher #(TReq) pub_;
  local shuttle_response_sink     sink_;
  local shuttle_subscription      sub_;
  local shuttle_call #(TResp)     calls_[int unsigned];
  local longint                   stale_;
  local string                    error_;

  // Both topic names are required and must differ (a responder publishes the
  // reply from inside the request callback, so one shared topic would deadlock
  // the serial chain). Failures leave the handle invalid with the reason in
  // last_error(): SV has no exception to throw at the caller, so this is the
  // loudest quiet failure available — valid() is the one check that matters.
  function new(string req_topic, string resp_topic);
    req_topic_  = req_topic;
    resp_topic_ = resp_topic;
    stale_      = 0;
    error_      = "";
    if (req_topic.len() == 0 || resp_topic.len() == 0) begin
      error_ = "shuttle: request/response needs two non-empty topic names";
      return;
    end
    if (req_topic == resp_topic) begin
      error_ = "shuttle: request and response topics must differ";
      return;
    end
    // Mark before subscribing: from here on a blocking-entry subscriber on the
    // response topic is refused (it would hold the topic's chain and delay every
    // reply on it).
    if (shuttle_c_mark_responses(resp_topic_) != 0) begin
      error_ = shuttle_c_last_error();
      return;
    end
    sink_ = new (this);
    sub_  = shuttle_subscribe_nb(resp_topic_, sink_, TResp::type_tag);
    if (sub_ == null) begin
      error_ = shuttle_c_last_error();
      return;
    end
    pub_ = new (req_topic_);
  endfunction

  function bit valid();
    return sub_ != null;
  endfunction

  function string last_error();
    return error_;
  endfunction

  function string req_topic();
    return req_topic_;
  endfunction

  function string resp_topic();
    return resp_topic_;
  endfunction

  function int inflight();
    return calls_.num();
  endfunction

  // Replies that arrived for a call nobody waits for any more (cancelled before
  // the answer, or published after the call was pruned): counted, never silently
  // dropped.
  function longint stale_responses();
    return stale_;
  endfunction

  // ── Demux (delivery path) ─────────────────────────────────────────────
  virtual function void accept_response(input byte payload[], input int unsigned corr_id);
    shuttle_call #(TResp) call;
    if (!calls_.exists(corr_id)) begin
      stale_++;
      return;
    end
    call = calls_[corr_id];
    if (call.cancelled()) begin
      stale_++;
      return;
    end
    call.push(payload);
  endfunction

  // Cancelled calls leave the table on the next send, so it stays bounded by the
  // calls still in flight. (SV has no weak references: a call the caller merely
  // forgot is not detectable, so cancel it.)
  local function void prune();
    foreach (calls_[id])
      if (calls_[id].cancelled()) calls_.delete(id);
  endfunction

  local function void pack_req(TReq req, output byte payload[]);
    byte q[$];
    req.pack(q);
    shuttle_q2a(q, payload);
  endfunction

  // Shared front half of send_*: validate, prune, allocate the id, and register
  // the slot before the frame goes out — a nonblocking responder replies inside
  // the publish call and that reply has to find its call.
  local function shuttle_call #(TResp) begin_call(output int unsigned corr_id);
    shuttle_call #(TResp) call;
    corr_id = 0;
    if (!valid()) begin
      error_ = "shuttle: empty requester handle";
      return null;
    end
    prune();
    corr_id         = int'(shuttle_c_take_corr_id());
    call            = new (corr_id);
    calls_[corr_id] = call;
    return call;
  endfunction

  // Shared back half: attach the request frame, or fail the call (null return,
  // reason in last_error()). The request may well have reached its targets
  // already — the failure is about the completion handle, not the delivery.
  local function shuttle_call #(TResp) finish(shuttle_call #(TResp) call, int pid);
    shuttle_handle h;
    if (pid <= 0) begin
      error_ = shuttle_c_last_error();
      calls_.delete(call.corr_id());
      return null;
    end
    h = new (pid);
    call.attach(h);
    return call;
  endfunction

  // Send through publish_now: nonblocking responders answer on this stack, so
  // the reply is already queued when this returns.
  function shuttle_call #(TResp) send_now(TReq req, int policy = SHUTTLE_NOW_DEFER);
    int unsigned          corr_id;
    shuttle_call #(TResp) call;
    byte                  payload[];
    call = begin_call(corr_id);
    if (call == null) return null;
    pack_req(req, payload);
    return finish(call, shuttle_c_publish_now(req_topic_, TReq::type_tag, payload, policy, corr_id));
  endfunction

  // Send through publish_serial: background chain, replies arrive later (recv /
  // wait_* suspends on the call's own event, never on the request frame).
  function shuttle_call #(TResp) send_serial(TReq req, shuttle_trigger trig = null);
    int unsigned          corr_id;
    shuttle_call #(TResp) call;
    byte                  payload[];
    int                   kind;
    longint               arg;
    string                name;
    call = begin_call(corr_id);
    if (call == null) return null;
    shuttle_trig_scalars(trig, kind, arg, name);
    pack_req(req, payload);
    return finish(call, shuttle_c_publish_serial(req_topic_, TReq::type_tag, payload, kind, arg, name,
                                                corr_id));
  endfunction

  // Send through publish_concurrent: one task per target, replies interleave.
  function shuttle_call #(TResp) send_concurrent(TReq req, shuttle_trigger trig = null);
    int unsigned          corr_id;
    shuttle_call #(TResp) call;
    byte                  payload[];
    int                   kind;
    longint               arg;
    string                name;
    call = begin_call(corr_id);
    if (call == null) return null;
    shuttle_trig_scalars(trig, kind, arg, name);
    pack_req(req, payload);
    return finish(
        call, shuttle_c_publish_concurrent(req_topic_, TReq::type_tag, payload, kind, arg, name,
                                           corr_id));
  endfunction
endclass

`endif  // SHUTTLE_REQUESTER_SVH
