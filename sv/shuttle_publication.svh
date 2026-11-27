`ifndef SHUTTLE_PUBLICATION_SVH
`define SHUTTLE_PUBLICATION_SVH

// ── Publication completion handle (corresponds to shuttle::Publication) ──
// SV has no exceptions: subscriber failures surface via first_error(), and
// the user decides how to handle them.
class shuttle_publication;
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
  // Block until all subscriber callbacks complete; timeout_fs = 0 means
  // wait forever (unit: femtoseconds)
  task wait_done(longint timeout_fs = 0);
    int status;
    if (!valid()) return;
    shuttle_c_wait_pub(pub_id_, timeout_fs, status);
    if (status == SHUTTLE_WAIT_TIMEOUT)
      trace_pub().error($sformatf("wait on Publication %0d timed out", pub_id_));
    else if (status == SHUTTLE_WAIT_FAILED) begin
      // A subscriber failure carries its own text; an internal error only has the DPI error
      // channel.
      automatic string why = first_error();
      if (why == "") why = shuttle_c_last_error();
      trace_pub().error($sformatf("wait on Publication %0d failed: %s", pub_id_, why));
    end else
      trace_pub().trace($sformatf("publication %0d done (%0d/%0d subscribers finished)",
                                  pub_id_, finished(), subscriber_count()));
  endtask
  // ── Visibility counters (same names and semantics as C++ Publication) ──
  // Target subscriber count of this frame (snapshot at publish time, including the deferred part)
  function int subscriber_count();
    return valid() ? shuttle_c_pub_stat(pub_id_, 0) : 0;
  endfunction
  // Subscribers finished (successfully or not)
  function int finished();
    return valid() ? shuttle_c_pub_stat(pub_id_, 1) : 0;
  endfunction
  // Subscribers skipped by policy, to whom this frame will not be delivered
  function int skipped();
    return valid() ? shuttle_c_pub_stat(pub_id_, 2) : 0;
  endfunction
  // Subscribers deferred to the background serial chain, delivered later
  function int deferred();
    return valid() ? shuttle_c_pub_stat(pub_id_, 3) : 0;
  endfunction
  // Subscribers unsubscribed after the snapshot, never entered the callback (already counted in finished)
  function int retired();
    return valid() ? shuttle_c_pub_stat(pub_id_, 4) : 0;
  endfunction
  // Error message of the first failed subscriber (empty = no failure)
  function string first_error();
    if (!valid()) return "";
    return shuttle_c_pub_error(pub_id_);
  endfunction
endclass

// ── Deferred trigger (corresponds to shuttle::Trigger) ─────────────────
// Snapshot semantics match C++: the subscriber set and the message copy are
// locked in at the moment of the publish call; the trigger only decides when
// the background callback process starts.
class shuttle_trigger;
  int     kind = SHUTTLE_TRIG_NONE;
  longint arg  = 0;
  // NB: not named `edge` — VCS 2018.09 rejects it as a reserved word (as with `small`).
  string  edge_name = "";
  static function shuttle_trigger none();
    shuttle_trigger t = new();
    return t;
  endfunction
  // Simulation delay (femtoseconds)
  static function shuttle_trigger after_delay(longint fs);
    shuttle_trigger t = new();
    t.kind = SHUTTLE_TRIG_DELAY;
    t.arg  = fs;
    return t;
  endfunction
  // Another publication completes (success or failure both count as done)
  static function shuttle_trigger after_pub(shuttle_publication h);
    shuttle_trigger t = new();
    t.kind = SHUTTLE_TRIG_PUB;
    t.arg  = h.id();
    return t;
  endfunction
  // A weave edge (clock/reset/any handshake) fires. Only the *name* crosses the boundary: C++
  // resolves it in weave's edge table, so the edge stays one object for both languages.
  // Edge semantics, not a latch: a fire() that happens before the frame's delivery process has
  // registered its wait is lost, and the frame stays pending until the next fire().
  static function shuttle_trigger after_edge(weave_edge e);
    shuttle_trigger t = new();
    if (e == null || e.id < 0)
      trace_trigger().error("after_edge needs a live weave_edge (pass `new(\"name\")`)");
    t.kind = SHUTTLE_TRIG_EDGE;
    t.edge_name = (e == null) ? "" : e.name;
    return t;
  endfunction
endclass

`endif  // SHUTTLE_PUBLICATION_SVH
