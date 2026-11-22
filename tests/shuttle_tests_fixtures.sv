// ============================================================================
// shuttle_tests_fixtures.sv — message classes and the both-entry subscriber shared by the suites
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → shuttle_tests_fixtures.sv →
//                shuttle_tests_runner.sv → the suite modules → shuttle_tests.sv (vcs is handed the
//                list in this order by xmake.lua); shuttle_tests.sv calls the case tasks in order.
// Run: xmake run shuttle_tests_svdpi_vcs (compiled and executed by tests/run-eda.sh).
// ============================================================================

package shuttle_tests_fixtures;
  timeunit 1ns; timeprecision 1ps;

  import shuttle_pkg::*;

  // ── Message classes: counterpart of `struct XMsg { uint32_t addr; uint32_t data; }` on the C++ side ──
  // All message classes use the generated codec (sv/shuttle_reflect.svh three-piece macros), which is
  // byte-for-byte the C++ Writer's big-endian wire order.
  // ── Message classes: counterpart of `struct XMsg { uint32_t addr; uint32_t data; }` on the C++ side ──
  // All message classes use the generated codec (sv/shuttle_reflect.svh three-piece macros), which is
  // byte-for-byte the C++ Writer's big-endian wire order (verified on VCS; see the
  // "Simulator constraints" section of docs/backends.md).
  class XMsg;
    int unsigned addr;
    int unsigned data;
    `shuttle_reflect_begin(XMsg)
      `shuttle_field(addr, shuttle_u32)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // Another type: only used by the subscribe-before-bind tag-conflict case (any type_tag different from XMsg)
  class YMsg;
    int unsigned addr;
    `shuttle_reflect_begin(YMsg)
      `shuttle_field(addr, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // ── Cross-language request/response payloads (mirror XRqReq/XRqResp in tests/shuttle_tests_xlang.cpp) ──
  // One uint32 field each; type_tag must equal the C++ struct name so that type-matched
  // delivery works across languages. The correlation id is NOT part of the payload: it travels in
  // the frame header (shuttle_meta::corr_id), so these classes hold only domain data.
  class XRqReq;
    int unsigned op;
    // The macro generates both pack and unpack; this class only ever decodes C++-side requests.
    `shuttle_reflect_begin(XRqReq)
      `shuttle_field(op, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class XRqResp;
    int unsigned data;
    // Only ever published from SV; the C++ requester decodes it.
    `shuttle_reflect_begin(XRqResp)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // ── Nested message types (mirror XPoint/XTrack in tests/shuttle_tests_xlang.cpp case7) ──
  // A track is two points plus an id. The three-piece macros recurse per field:
  // `shuttle_field(origin, XPoint)` generates XPoint::pack_obj(q, o.origin) /
  // XPoint::unpack_into(b, pos, o.origin) — the field's codec is the nested class itself, inline,
  // no length prefix, byte-for-byte the C++ Codec<XTrack> recursion into Codec<XPoint>. The
  // nested handles get field initializers so packing an unset track cannot dereference null.
  class XPoint;
    int x;
    int y;
    `shuttle_reflect_begin(XPoint)
      `shuttle_field(x, shuttle_i32)
      `shuttle_field(y, shuttle_i32)
    `shuttle_reflect_end
  endclass

  class XTrack;
    int unsigned id;
    XPoint       origin = new();
    XPoint       dest   = new();
    `shuttle_reflect_begin(XTrack)
      `shuttle_field(id, shuttle_u32)
      `shuttle_field(origin, XPoint)
      `shuttle_field(dest, XPoint)
    `shuttle_reflect_end
  endclass

  // ── Inheritance chain (mirror XHead/XMid/XChain in tests/shuttle_tests_xlang.cpp case8) ──
  // Each level lists only its own fields and delegates its base subobject with `shuttle_base,
  // the SV twin of SHUTTLE_REFLECT_DERIVED: wire order is the base encoding first, then the own
  // fields, so the chain stays byte-for-byte the C++ Codec<XChain> → Codec<XMid> → Codec<XHead>
  // delegation. A signed field at the middle level checks the sign convention across the DPI.
  class XHead;
    int unsigned seq;
    `shuttle_reflect_begin(XHead)
      `shuttle_field(seq, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class XMid extends XHead;
    shortint level;
    `shuttle_reflect_begin(XMid)
      `shuttle_base(XHead)
      `shuttle_field(level, shuttle_i16)
    `shuttle_reflect_end
  endclass

  class XChain extends XMid;
    byte flag;
    `shuttle_reflect_begin(XChain)
      `shuttle_base(XMid)
      `shuttle_field(flag, shuttle_u8)
    `shuttle_reflect_end
  endclass

  // ── Implements both entries: shuttle_subscribe registers the blocking entry, shuttle_subscribe_nb the
  //    nonblocking entry (isomorphic to C++ subscribe(fn) / subscribe(nb(fn))) ─────────
  class Rec implements shuttle_sink, shuttle_sink_nb;
    int     blk_count = 0;
    int     nb_count  = 0;
    int     order[$];   // received addr sequence (ordering assertion)
    longint seqs[$];    // received seq sequence (monotonicity assertion)
    XMsg    last;

    virtual task invoke(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      blk_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.topic_seq);
      #(1ns);  // consuming simulation time inside a blocking callback is a legal usage
    endtask

    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      XMsg m = XMsg::unpack(payload);
      nb_count++;
      last = m;
      order.push_back(int'(m.addr));
      seqs.push_back(meta.topic_seq);
    endfunction
  endclass

endpackage
