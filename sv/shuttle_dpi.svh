`ifndef SHUTTLE_DPI_SVH
`define SHUTTLE_DPI_SVH

// ── DPI: imports (provided by libshuttle.so on the C++ side) ───────────
// All are context imports: the implementations may call back into the
// exports below (dispatch / time).
import "DPI-C" context function int shuttle_c_publish_now(
    string topic, string type_tag, byte payload[], int now_policy, int unsigned corr_id = 0);
import "DPI-C" context function int shuttle_c_publish_serial(
    string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg,
    string trig_name, int unsigned corr_id = 0);
import "DPI-C" context function int shuttle_c_publish_concurrent(
    string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg,
    string trig_name, int unsigned corr_id = 0);
import "DPI-C" context task shuttle_c_wait_pub(
    int pub_id, longint timeout_fs, output int status);
import "DPI-C" context function int shuttle_c_pub_done(int pub_id);
// Publication counter query: kind 0=expected 1=finished 2=skipped-by-policy
// 3=deferred 4=retired
import "DPI-C" context function int shuttle_c_pub_stat(int pub_id, int kind);
import "DPI-C" context function string shuttle_c_pub_error(int pub_id);
// Report back to C++ when an in-flight delivery reaches an already-unsubscribed id
// (no error — just lets C++ record that delivery as retired)
import "DPI-C" context function void shuttle_c_sub_retired(int sv_sid);
import "DPI-C" context function int shuttle_c_subscribe(
    string topic, int caps, string type_tag, int sv_sid);
import "DPI-C" context function void shuttle_c_unsubscribe(int sv_sid);
import "DPI-C" context function void shuttle_c_set_retained(string topic, bit retained);
// Request/response: the corr_id allocation point.
// The correlation id sequence lives on the C++ side (the Requester's counter),
// so an SV Requester shares one id sequence with C++.
// shuttle_c_take_corr_id never returns 0.
import "DPI-C" context function int shuttle_c_take_corr_id();
import "DPI-C" context function string shuttle_c_last_error();

// Frame-pull protocol: before calling shuttle_sv_dispatch[_nb], C++ sets the
// current frame's byte stream as process-level context; SV pulls it in
// chunks inside the export body via these two scalar imports (8-byte
// little-endian chunks).
import "DPI-C" context function int shuttle_c_frame_len();
import "DPI-C" context function longint shuttle_c_frame_qword(int index);

// ── DPI: exports (provided by this package, called back by the C++ broker) ──
// Same precedent as weave: exports live inside the package (verified on
// VCS); if a target simulator rejects in-package exports, move these two
// entries into a module — the protocol is unchanged.
export "DPI-C" task shuttle_sv_dispatch;
export "DPI-C" function shuttle_sv_dispatch_nb;

`endif  // SHUTTLE_DPI_SVH
