`ifndef SHUTTLE_PUB_SVH
`define SHUTTLE_PUB_SVH

// ── Typed publisher ────────────────────────────────────────────────────
// The type parameter T must satisfy the message-class convention at the top
// of this file (pack() + localparam type_tag).
class shuttle_pub #(type T = byte);
  local string topic_;
  function new(string topic);
    topic_ = topic;
  endfunction

  // Queue → dynamic array (DPI open-array parameters require dynamic arrays;
  // VCS does not support unpacked arrays as function return types)
  local function automatic void q2a(const ref byte q[$], output byte a[]);
    a = new [q.size()];
    foreach (q[i]) a[i] = q[i];
  endfunction

  // ── Publish trio: names correspond 1:1 to publish_now / publish_serial /
  //    publish_concurrent on the C++ side; the scheduling flavor is in the
  //    function name — there is no mode argument ──────────────────────────

  // Message → dynamic array needed by the DPI open array (pack() produces a
  // queue; see the message-class convention at the top of this file)
  local function void pack_payload(T msg, output byte payload[]);
    byte q[$];
    msg.pack(q);
    q2a(q, payload);
  endfunction

  // Trigger → (trig_kind, trig_arg) scalars; null means start immediately
  local function void trig_scalars(shuttle_trigger trig, output int kind, output longint arg,
                                  output string name);
    kind = SHUTTLE_TRIG_NONE;
    arg  = 0;
    name = "";
    if (trig != null) begin
      kind = trig.kind;
      arg  = trig.arg;
      name = trig.edge_name;
    end
  endfunction

  // pub_id → handle; returns null on failure. loud=1 additionally prints $error.
  // `publish_now` stays silent (under the reject policy, failure is the
  // expected outcome; the reason goes through shuttle_c_last_error());
  // `serial`/`concurrent` print because they are fire-and-forget async
  // interfaces where errors must be visible immediately.
  local function shuttle_handle wrap(int pid, string what, bit loud);
    if (pid <= 0) begin
      if (loud)
        $error("shuttle: %s publish failed (topic=%s): %s", what, topic_, shuttle_c_last_error());
      return null;
    end
    begin
      shuttle_handle h = new(pid);
      return h;
    end
  endfunction

  // `publish_now`: synchronous callbacks on the publisher's stack, never
  // suspending (invokes all subscribers with a nonblocking entry).
  // policy: how to handle targets whose entry is blocking (cannot be called
  //         on the caller's stack, i.e. the shuttle_sink invoke task);
  //         defaults to SHUTTLE_NOW_DEFER (hand to the background serial
  //         chain, callback later).
  //         — one registration has exactly one entry; the policy only sees
  //         that entry and is independent of whether the topic also has
  //         nonblocking subscribers.
  // Returns this frame's handle (same type as `serial`/`concurrent`): the
  // nonblocking part has fully finished when it returns, skipped targets are
  // in skipped(), and deferred targets are still on the background chain —
  // done()/wait_done() cover that part.
  // Returns null on failure (see shuttle_c_last_error()).
  function shuttle_handle publish_now(T msg, int policy = SHUTTLE_NOW_DEFER);
    byte payload[];
    pack_payload(msg, payload);
    return wrap(shuttle_c_publish_now(topic_, T::type_tag, payload, policy, 0), "now", 1'b0);
  endfunction

  // `publish_serial`: background single-chain, in-order callbacks
  // (topic-level FIFO); returns the completion handle immediately.
  function shuttle_handle publish_serial(T msg, shuttle_trigger trig = null);
    byte payload[];
    int kind;
    longint arg;
    string name;
    trig_scalars(trig, kind, arg, name);
    pack_payload(msg, payload);
    return wrap(shuttle_c_publish_serial(topic_, T::type_tag, payload, kind, arg, name, 0),
                "serial", 1'b1);
  endfunction

  // `publish_concurrent`: background, one task per subscriber, concurrent
  // callbacks; returns the completion handle immediately.
  function shuttle_handle publish_concurrent(T msg, shuttle_trigger trig = null);
    byte payload[];
    int kind;
    longint arg;
    string name;
    trig_scalars(trig, kind, arg, name);
    pack_payload(msg, payload);
    return wrap(shuttle_c_publish_concurrent(topic_, T::type_tag, payload, kind, arg, name, 0),
                "concurrent", 1'b1);
  endfunction

  // Responder shorthand: publish `msg` as the answer to `request`. The frame inherits
  // request.corr_id, which is what lets the C++ side route it to its Call. This is the SV
  // counterpart of serve() and the *only* place correlation is visible; publish_now /
  // publish_serial / publish_concurrent stay unaware of it (they leave corr_id 0), so a
  // publisher never has to think about correlation.
  //
  // It goes out through publish_now so that a nonblocking C++ requester has the answer on
  // its stack before this call returns (no task switch). `defer` keeps it correct even if
  // a blocking subscriber slipped onto the response topic before it was marked.
  function shuttle_handle reply(T msg, shuttle_meta request);
    byte payload[];
    pack_payload(msg, payload);
    return wrap(
        shuttle_c_publish_now(topic_, T::type_tag, payload, SHUTTLE_NOW_DEFER, request.corr_id),
        "reply", 1'b1);
  endfunction

  // retained (latched): once enabled, the broker caches the latest frame
  // and new subscribers receive it immediately
  function void set_retained(bit r);
    shuttle_c_set_retained(topic_, r);
  endfunction

endclass

`endif  // SHUTTLE_PUB_SVH
