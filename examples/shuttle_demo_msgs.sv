// ============================================================================
// shuttle_demo_msgs.sv — message classes shared by the SV demo modules
//
// Compile order: weave_pkg.sv (../weave/sv) → shuttle_pkg.sv (../../sv) → this file →
//                shuttle_demo_*.sv → shuttle_demo.sv
//
// Only wire types live here, one class per message shape; the sinks that decode them belong to
// the module that demonstrates them. The three-piece macros (sv/shuttle_reflect.svh) generate the
// whole message-class convention:
//   · localparam string type_tag = "UartTx" — must equal the C++ struct name, validated at publish time;
//   · pack()/unpack() byte-for-byte equal to the C++ Writer's big-endian wire order;
//   · each shuttle_field is a (field, codec) pair; the codec name carries the width/encoding
//     contract while the field keeps whatever SV type you prefer (see sv/shuttle_codec.svh).
// ============================================================================

package shuttle_demo_msgs;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;

  // ── Counterpart of `struct UartTx { uint32_t addr; uint32_t data }` on the C++ side ──
  class UartTx;
    int unsigned addr;
    int unsigned data;
    `shuttle_reflect_begin(UartTx)
      `shuttle_field(addr, shuttle_u32)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  // ── Nested message class: counterpart of `struct UartBurst` in shuttle_demo_msgs.hpp ──
  // `shuttle_field(first, UartTx)` makes the nested class itself the field codec: the macro
  // generates UartTx::pack_obj(q, o.first) / UartTx::unpack_into(b, pos, o.first) — the same
  // recursion Codec<UartBurst> → Codec<UartTx> runs on the C++ side, inline, no length prefix.
  class UartBurst;
    byte   count;
    UartTx first = new();
    UartTx last  = new();
    `shuttle_reflect_begin(UartBurst)
      `shuttle_field(count, shuttle_u8)
      `shuttle_field(first, UartTx)
      `shuttle_field(last, UartTx)
    `shuttle_reflect_end
  endclass

  // ── Inherited message classes: counterpart of UartHeader/UartFrame/UartTagged in shuttle_demo_msgs.hpp ──
  // `shuttle_base(T)` encodes the direct base subobject through T's own codec, so each level lists
  // only its own fields — the SV twin of SHUTTLE_REFLECT_DERIVED. Wire order is base first, then
  // own fields, which is exactly the flat listing; here the inherited fields are written once.
  class UartHeader;
    int unsigned addr;
    `shuttle_reflect_begin(UartHeader)
      `shuttle_field(addr, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class UartFrame extends UartHeader;
    int unsigned data;
    `shuttle_reflect_begin(UartFrame)
      `shuttle_base(UartHeader)
      `shuttle_field(data, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class UartTagged extends UartFrame;
    byte flags;
    `shuttle_reflect_begin(UartTagged)
      `shuttle_base(UartFrame)
      `shuttle_field(flags, shuttle_u8)
    `shuttle_reflect_end
  endclass

endpackage
