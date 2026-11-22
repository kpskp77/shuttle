`ifndef SHUTTLE_META_SVH
`define SHUTTLE_META_SVH

// ── Metadata: passed with each delivery, mirrors shuttle::Meta on the C++ side ──
class shuttle_meta;
  string  topic_name;
  string  type_tag;
  longint topic_seq;
  longint time_fs;
  // Correlation id of this frame, assigned by the publisher (0 = uncorrelated).
  // Echo it when publishing a response to a request (see `shuttle_publisher::publish_*`).
  int unsigned corr_id;
  function new(string topic_name, string type_tag, longint topic_seq, longint time_fs,
               int unsigned corr_id = 0);
    this.topic_name = topic_name;
    this.type_tag   = type_tag;
    this.topic_seq  = topic_seq;
    this.time_fs    = time_fs;
    this.corr_id    = corr_id;
  endfunction
endclass

// ── Frame pull: byte stream of the C++ dispatch context → SV dynamic array ──
// Protocol: shuttle_c_frame_qword(i) returns the i-th 8-byte chunk (little
// endian); concatenating them yields the complete frame.
// Relies on the single-active-process invariant: callable only inside the
// shuttle_sv_dispatch[_nb] body.
function automatic void shuttle_frame_bytes(output byte b[]);
  int n = shuttle_c_frame_len();
  b = new [n];
  for (int base = 0; base < n; base += 8) begin
    longint qw = shuttle_c_frame_qword(base / 8);
    for (int j = 0; j < 8 && base + j < n; j++)
      b[base + j] = byte'(qw[j * 8 +: 8]);
  end
endfunction

`endif  // SHUTTLE_META_SVH
