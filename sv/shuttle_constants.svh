`ifndef SHUTTLE_CONSTANTS_SVH
`define SHUTTLE_CONSTANTS_SVH

// ── Constants: strictly aligned with the C++ enums (docs/backends.md §constants) ──
// Subscription entries (caps argument of shuttle_c_subscribe): one
// registration can only ever be one of them (the same object registered
// twice = two subscribers — only then can both values be in play)
localparam int SHUTTLE_CAP_BLOCKING    = 1;  // blocking entry: shuttle_subscribe
localparam int SHUTTLE_CAP_NONBLOCKING = 2;  // nonblocking entry: shuttle_subscribe_nb
// Policy for publish_now meeting blocking-entry subscribers (now_policy argument)
localparam int SHUTTLE_NOW_REJECT = 0;  // error (deliver to no subscriber at all)
localparam int SHUTTLE_NOW_SKIP   = 1;  // skip these subscribers: not in subscriber_count(), only in skipped()
localparam int SHUTTLE_NOW_DEFER  = 2;  // default: hand to the topic's background serial chain, callback later
// Trigger kinds (trig_kind argument of shuttle_c_publish_serial / _concurrent)
// An external edge is the one case that needs a name instead of a number: `trig_name` carries it
// (empty for every other kind). The name is the whole binding — C++ asks weave's own edge table
// for it, so no id ever crosses the DPI boundary.
localparam int SHUTTLE_TRIG_NONE  = 0;  // start immediately
localparam int SHUTTLE_TRIG_DELAY = 1;  // trig_arg = simulation delay (femtoseconds)
localparam int SHUTTLE_TRIG_PUB   = 2;  // trig_arg = the pub_id of another Publication
localparam int SHUTTLE_TRIG_EDGE  = 3;  // trig_name = name of a weave_edge (edge semantics)
// status output of shuttle_c_wait_pub
localparam int SHUTTLE_WAIT_OK      = 0;  // all completed (not necessarily failure-free)
localparam int SHUTTLE_WAIT_TIMEOUT = 1;
localparam int SHUTTLE_WAIT_FAILED  = 2;  // subscriber failure(s) present (see shuttle_c_pub_error)
                                          // or an internal error (see shuttle_c_last_error)
// Overflow policy of a bounded mailbox (mirrors shuttle::OverflowPolicy).
// `block` and `reject` have no SV counterpart: a delivery callback must not
// block, and SV has no exceptions to reject with.
localparam int SHUTTLE_OVERFLOW_DROP_OLDEST = 0;
localparam int SHUTTLE_OVERFLOW_DROP_NEWEST = 1;

`endif  // SHUTTLE_CONSTANTS_SVH
