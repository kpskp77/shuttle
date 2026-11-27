// ============================================================================
// shuttle_pkg.sv — SV-side interface of the shuttle publish-subscribe framework
//
// This file is an AGGREGATOR: the package body is split into functional
// fragments (`include`d below, one concern per file — trace / dpi / constants /
// meta / subscription / mailbox / publication / publisher / requester / ref / dispatch /
// codec), so each part can be read and reviewed on its own. Compile THIS file
// only; the fragments are not standalone compilation units. The shuttle_reflect_begin
// / shuttle_field / shuttle_reflect_end macros (sv/shuttle_reflect.svh) are
// included after `endpackage` because `define has compilation-unit scope.
//
// Depends on: weave_pkg (../weave/sv/weave_pkg.sv, compiled by the simulation
// scripts along with this file) and simlog_pkg (../simlog/sv/simlog_pkg.sv,
// same order — see sv/shuttle_trace.svh). The DPI protocol reference is
// docs/backends.md; the direction convention matches weave:
//   import (provided by C++): shuttle_c_*  — publish / subscribe registration /
//                                            completion wait / frame pull
//   export (provided by SV) : shuttle_sv_* — subscriber callback dispatch + sim time
//
// Frame-pull protocol (the dispatch payload does not go through DPI arguments):
//   before calling shuttle_sv_dispatch[_nb], C++ sets the current frame's byte
//   stream as process-level context; SV pulls it inside the export body via
//   shuttle_c_frame_len / shuttle_c_frame_payload (one DPI call per frame; the
//   payload crosses as an open array the SV side allocates, so C++ copies it
//   with a single memcpy). Relies on the simulator's single-active-process
//   invariant (the same premise as weave).
//
// Callback entry model (corresponds to shuttle::Capability on the C++ side):
//   shuttle_sink    — blocking entry (export task, may consume simulation time),
//                     registered via shuttle_subscribe
//   shuttle_sink_nb — nonblocking entry (export function, zero delay),
//                     registered via shuttle_subscribe_nb
//   One subscriber class may implement both (multiple interface-class
//   implementations, zero intrusion into existing code); which entry is used
//   is then decided by which registration function is called — for both,
//   register twice.
//
// Message class convention (no base class to inherit; used by shuttle_publisher #(T)).
// Hand-written codecs stream-pack in declaration order (big-endian byte order):
//   class Txn;
//     int unsigned addr;
//     int unsigned data;
//     localparam string type_tag = "Txn";         // must equal the C++ struct name
//     function automatic void pack(output byte q[$]);
//       q = {>>{addr, data}};   // ← must use {>>{...}} (1-bit slices, MSB first)
//     endfunction
//     static function automatic Txn unpack(byte b[]);
//       Txn t = new();
//       {>>{t.addr, t.data}} = b;
//       return t;
//     endfunction
//   endclass
// Note: a string field must first stream a 32-bit length prefix and then the
//       bytes, i.e. {>>{32'(s.len()), s}}, aligned with the C++ Codec.
// Byte-order hint: {>>{...}} yields "field declaration order + each field
//           MSB-first", exactly the C++ Writer's big-endian layout; {<<{...}}
//           reverses the entire bit stream — never mix them up.
// The recommended route is the generated codec instead — the three-piece
// shuttle_reflect_begin / shuttle_field / shuttle_reflect_end macros
// (sv/shuttle_reflect.svh) expand to this same convention plus the nested /
// string / vector codecs, byte-for-byte equal to the C++ byte layout:
//   class Txn;
//     logic [31:0] addr;                 // field type is free; the codec name
//     string       tag;                  // carries the width contract
//     `shuttle_reflect_begin(Txn)
//       `shuttle_field(addr, shuttle_u32)
//       `shuttle_field(tag, shuttle_string)
//     `shuttle_reflect_end
//   endclass
// ============================================================================
`ifndef SHUTTLE_PKG_SV
`define SHUTTLE_PKG_SV

package shuttle_pkg;

  timeunit 1ps;
  timeprecision 1ps;

  // weave provides the DPI plumbing and the thread-local current-scope guard;
  // NOTE: VCS 2018.09 does not propagate re-export, so consumers of this
  //       package must import weave_pkg themselves when they need it.
  import weave_pkg::*;

  `include "shuttle_trace.svh"
  `include "shuttle_dpi.svh"
  `include "shuttle_constants.svh"
  `include "shuttle_meta.svh"
  `include "shuttle_subscription.svh"
  `include "shuttle_mailbox.svh"
  `include "shuttle_publication.svh"
  `include "shuttle_publisher.svh"
  `include "shuttle_requester.svh"
  `include "shuttle_dispatch.svh"
  `include "shuttle_codec.svh"
  `include "shuttle_ref.svh"

endpackage : shuttle_pkg

`include "shuttle_reflect.svh"

`endif  // SHUTTLE_PKG_SV
