`ifndef SHUTTLE_DISPATCH_SVH
`define SHUTTLE_DISPATCH_SVH

// ── DPI export implementations: callback entries of the C++ broker ─────
// Dispatch of `serial`/`concurrent`: called by the C++ weave task; this task
// runs in the SV process hosting that weave task and may legally consume
// simulation time. The payload is obtained via the frame-pull protocol (see
// the header of this file).
task automatic shuttle_sv_dispatch(input int sub_id, input string topic_name,
                                   input string type_tag, input longint topic_seq,
                                   input longint time_fs, input int unsigned corr_id);
  shuttle_meta m = new(topic_name, type_tag, topic_seq, time_fs, corr_id);
  byte payload[];
  shuttle_frame_bytes(payload);
  shuttle_registry::dispatch(sub_id, payload, m);
endtask

// Dispatch of `publish_now`: runs in the publisher's process context and
// must be zero-delay (the compiler enforces this for a function). The
// payload is obtained via the frame-pull protocol.
function automatic void shuttle_sv_dispatch_nb(input int sub_id, input string topic_name,
                                               input string type_tag, input longint topic_seq,
                                               input longint time_fs,
                                               input int unsigned corr_id);
  shuttle_meta m = new(topic_name, type_tag, topic_seq, time_fs, corr_id);
  byte payload[];
  shuttle_frame_bytes(payload);
  shuttle_registry::dispatch_nb(sub_id, payload, m);
endfunction

`endif  // SHUTTLE_DISPATCH_SVH
