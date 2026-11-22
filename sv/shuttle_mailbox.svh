`ifndef SHUTTLE_MAILBOX_SVH
`define SHUTTLE_MAILBOX_SVH

// ── Mailbox: a subscription with a queue (mirror of shuttle::Mailbox<T>) ──
// "Wait for the next frame" instead of "let a callback handle each frame".
// A mailbox **is one subscriber**: it registers through shuttle_subscribe_nb
// (nonblocking entry — pushing into the queue never blocks), so it joins the
// target set, counts in shuttle_handle::subscriber_count() and receives
// retained replays exactly like any other subscription.
//
// Whole implementation is pure SV: the queue is an SV queue and the wakeup
// goes through weave_pkg's event primitives (weave_sv_wait / weave_sv_trigger),
// so a mailbox needs no DPI entry point at all.
//
// Ordering: queue order = callback invocation order (serial = join order,
// concurrent = this subscriber's chain order, i.e. publish order, now =
// registration order).
// Capacity: capacity == 0 is unbounded; a bounded mailbox drops the oldest or
// the newest item and counts it in overflowed() (no blocking, no exceptions).
class shuttle_mailbox_item;
  byte         data[];
  shuttle_meta meta;
  function new(input byte d[], input shuttle_meta m);
    data = d;  // dynamic array assignment copies
    meta = m;
  endfunction
endclass

// The queue, the counters and the wakeup carrier. This is also the landing
// implementation of the nonblocking entry: pushing never blocks, so it is a
// legal export function body.
class shuttle_mailbox_sink implements shuttle_sink_nb;
  local int             ev_;  // weave event id used as the wakeup carrier
  local shuttle_mailbox_item q[$];
  local int             capacity_;
  local int             overflow_;
  local bit             closed_;
  local longint         received_;
  local longint         overflowed_;

  function new(int capacity, int overflow);
    capacity_ = capacity;
    overflow_ = overflow;
    closed_   = 0;
    ev_       = weave_sv_create_event();
  endfunction

  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
    push(payload, meta);
  endfunction

  function void push(input byte payload[], input shuttle_meta meta);
    if (closed_) return;  // closed: no new frames (queued ones stay readable)
    if (capacity_ > 0 && q.size() >= capacity_) begin
      if (overflow_ == SHUTTLE_OVERFLOW_DROP_NEWEST) begin
        overflowed_++;
        return;
      end
      q.pop_front();  // DROP_OLDEST (and the default)
      overflowed_++;
    end
    begin
      shuttle_mailbox_item it = new(payload, meta);
      q.push_back(it);
    end
    received_++;
    weave_sv_trigger(ev_);  // schedules waiters, never runs them inline
  endfunction

  function bit try_recv(output byte payload[], output shuttle_meta meta);
    shuttle_mailbox_item it;
    if (q.size() == 0) begin
      payload = {};
      return 0;
    end
    it      = q.pop_front();
    payload = it.data;
    meta    = it.meta;
    return 1;
  endfunction

  // Blocking receive: suspends the calling process until a frame arrives, the
  // timeout expires (timeout == 0 means wait forever), or the mailbox is
  // closed and drained. ok == 0 reports the two latter cases.
  task automatic recv(output bit ok, output byte payload[], output shuttle_meta meta,
                      input time timeout = 0);
    int             status;
    bit             has_deadline;
    time            deadline;
    longint         ticks;
    shuttle_mailbox_item it;
    ok           = 0;
    payload      = {};
    has_deadline = (timeout > 0);
    if (has_deadline) deadline = $time + timeout;
    forever begin
      if (q.size() > 0) begin
        it      = q.pop_front();
        payload = it.data;
        meta    = it.meta;
        ok      = 1;
        return;
      end
      if (closed_) return;  // drained and closed: nothing will ever arrive
      // The deadline is checked here (not from weave_sv_wait's status) because
      // the event's "triggered in this slot" flag makes status ambiguous when
      // a push and the timeout land in the same time slot.
      if (has_deadline) begin
        if ($time >= deadline) return;
        ticks = longint'(deadline - $time);  // package timeunit is 1ps: 1 tick == 1ps
      end else begin
        ticks = -1;  // no timeout leg: wait forever
      end
      weave_sv_wait(ev_, ticks, -1, status);
    end
  endtask

  function int size();
    return q.size();
  endfunction

  function longint received();
    return received_;
  endfunction

  function longint overflowed();
    return overflowed_;
  endfunction

  function bit closed();
    return closed_;
  endfunction

  function void close();
    if (closed_) return;
    closed_ = 1;
    weave_sv_trigger(ev_);
  endfunction
endclass

class shuttle_mailbox;
  local shuttle_subscription         sub_;   // nonblocking registration
  local shuttle_mailbox_sink sink_; // owns the queue; must outlive the registration

  function new(string pattern, string type_tag = "", int capacity = 0,
               int overflow = SHUTTLE_OVERFLOW_DROP_OLDEST);
    sink_ = new(capacity, overflow);
    sub_  = shuttle_subscribe_nb(pattern, sink_, type_tag);
  endfunction

  function bit valid();
    return sub_ != null;
  endfunction

  function bit try_recv(output byte payload[], output shuttle_meta meta);
    return sink_.try_recv(payload, meta);
  endfunction

  task automatic recv(output bit ok, output byte payload[], output shuttle_meta meta,
                      input time timeout = 0);
    sink_.recv(ok, payload, meta, timeout);
  endtask

  function int size();
    return sink_.size();
  endfunction

  function longint received();
    return sink_.received();
  endfunction

  function longint overflowed();
    return sink_.overflowed();
  endfunction

  function bit closed();
    return sink_.closed();
  endfunction

  // Idempotent: unsubscribe and wake every waiter. Queued frames stay readable.
  function void close();
    if (sub_ != null) sub_.unsubscribe();
    sink_.close();
  endfunction
endclass

`endif  // SHUTTLE_MAILBOX_SVH
