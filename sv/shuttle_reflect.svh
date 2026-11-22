// shuttle_reflect_begin/shuttle_field/shuttle_reflect_end — the SV-side
// message-codec generator; see sv/shuttle_codec.svh for the codec surface and
// wire format these macros generate against.
`ifndef SHUTTLE_REFLECT_SVH
`define SHUTTLE_REFLECT_SVH

// ── Three-piece reflection: generate the codec inside a message class ────
//
//   class Txn;
//     logic [31:0] addr;
//     string       tag;
//     `shuttle_reflect_begin(Txn)
//       `shuttle_field(addr, shuttle_u32)
//       `shuttle_field(tag, shuttle_string)
//     `shuttle_reflect_end
//   endclass
//
// BEGIN opens a single dual-mode `__codec` (packing switch) and emits the
// message-class convention shuttle_pub#(T) expects (type_tag / pack / unpack)
// plus the codec-surface entry points used when this class is a nested field
// of another message (pack_obj / unpack_into). Each FIELD is one (field,
// codec) pair; the codec token must name a class exposing the uniform codec
// (sv/shuttle_codec.svh builtins or another reflected message).
//
// Why one dual-mode function instead of separate pack/unpack lists: SV has no
// variadic macros, so a per-arity codec body macro would cap field counts
// (REFLECT1..8 used to); per-field FIELD macros have no arity at all. The
// `packing` bit keeps every field written once and lets the compiler fold the
// dead branch. Multi-line macro bodies: every line but the last ends with a
// backslash (VCS 2018 note in sv/shuttle_codec.svh).
//
// __codec formals are a fixed convention shared by the three macros:
//   q       ref byte q[$]        pack target (unused while unpacking)
//   b       const ref byte b[]   frame source (unused while packing)
//   pos     ref int pos          unpack cursor (monotonic)
//   o       ref T o              target object (allocated if null)
//   packing bit                  1 = pack this field, 0 = unpack it
`define shuttle_reflect_begin(T) \
  localparam string type_tag = `"T`"; \
  function automatic void pack(output byte q[$]); \
    byte q_[$]; \
    T::pack_obj(q_, this); \
    q = q_; \
  endfunction \
  static function automatic void pack_obj(ref byte q[$], input T o); \
    byte b_[]; \
    int pos_ = 0; \
    T::__codec(q, b_, pos_, o, 1'b1); \
  endfunction \
  static function automatic void unpack_into(const ref byte b[], ref int pos, ref T o); \
    byte q_[$]; \
    if (o == null) o = new(); \
    T::__codec(q_, b, pos, o, 1'b0); \
  endfunction \
  static function automatic T unpack(const ref byte b[]); \
    T o = new(); \
    int pos = 0; \
    T::unpack_into(b, pos, o); \
    return o; \
  endfunction \
  static function automatic void __codec(ref byte q[$], const ref byte b[], ref int pos, ref T o, \
                                         input bit packing);

`define shuttle_field(f, k) \
  if (packing) begin \
    k::pack_obj(q, o.f); \
  end else begin \
    k::unpack_into(b, pos, o.f); \
  end

// `shuttle_base(T) — encode/decode the direct base subobject as a unit through T's own codec,
// instead of repeating the inherited field names. Chains compose: each level names its direct
// base, and that base may itself use `shuttle_base. Requires T to carry the macro-generated
// static surface (pack_obj / unpack_into), i.e. T must use this three-piece reflection too.
// The unpack side goes through a T-typed handle copy because VCS 2018 ref formals demand an
// exact type match (a derived handle is rejected); generated entry points construct `o` before
// __codec runs, so writing through the handle copy lands on the same object.
// Usage:
//   class Base; ... `shuttle_reflect_begin(Base) ... `shuttle_reflect_end  endclass
//   class Derived extends Base;
//     `shuttle_reflect_begin(Derived)
//     `shuttle_base(Base)
//     `shuttle_field(data, shuttle_u32)
//     `shuttle_reflect_end
//   endclass
`define shuttle_base(T) \
  if (packing) begin \
    T::pack_obj(q, o); \
  end else begin \
    T base_ = o; \
    T::unpack_into(b, pos, base_); \
  end

`define shuttle_reflect_end \
  endfunction

`endif  // SHUTTLE_REFLECT_SVH
