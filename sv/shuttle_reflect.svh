// SHUTTLE_REFLECTn(T, f1, k1, ..., fn, kn) — see sv/shuttle_codec.svh for the
// codec surface and wire format these macros generate against.
`ifndef SHUTTLE_REFLECT_SVH
`define SHUTTLE_REFLECT_SVH

// ── SHUTTLE_REFLECT1..8: generate the codec inside a message class ──────
// `SHUTTLE_REFLECTn(Type, f1, k1, ..., fn, kn) — n = field count. The macro
// expands to the message-class convention (type_tag / pack / unpack) plus the
// codec surface (pack_obj / unpack_into) used when this class appears as a
// nested field of another message. Multi-line macro bodies: every line but
// the last ends with a backslash (VCS note 4).
`define SHUTTLE_DETAIL_CODEC_BODY(T, PK, UK) \
  localparam string type_tag = `"T`"; \
  function automatic void pack(output byte q[$]); \
    byte q_[$]; \
    T::pack_obj(q_, this); \
    q = q_; \
  endfunction \
  static function automatic void pack_obj(ref byte q[$], input T o); \
    PK \
  endfunction \
  static function automatic void unpack_into(const ref byte b[], ref int pos, ref T o); \
    if (o == null) o = new(); \
    UK \
  endfunction \
  static function automatic T unpack(const ref byte b[]); \
    T o = new(); \
    int pos = 0; \
    T::unpack_into(b, pos, o); \
    return o; \
  endfunction

`define SHUTTLE_REFLECT1(T, f1, k1) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1);, k1::unpack_into(b, pos, o.f1);)

`define SHUTTLE_REFLECT2(T, f1, k1, f2, k2) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2);)

`define SHUTTLE_REFLECT3(T, f1, k1, f2, k2, f3, k3) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3);)

`define SHUTTLE_REFLECT4(T, f1, k1, f2, k2, f3, k3, f4, k4) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3); \
                             k4::pack_obj(q, o.f4);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3); k4::unpack_into(b, pos, o.f4);)

`define SHUTTLE_REFLECT5(T, f1, k1, f2, k2, f3, k3, f4, k4, f5, k5) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3); \
                             k4::pack_obj(q, o.f4); k5::pack_obj(q, o.f5);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3); k4::unpack_into(b, pos, o.f4); \
                             k5::unpack_into(b, pos, o.f5);)

`define SHUTTLE_REFLECT6(T, f1, k1, f2, k2, f3, k3, f4, k4, f5, k5, f6, k6) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3); \
                             k4::pack_obj(q, o.f4); k5::pack_obj(q, o.f5); k6::pack_obj(q, o.f6);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3); k4::unpack_into(b, pos, o.f4); \
                             k5::unpack_into(b, pos, o.f5); k6::unpack_into(b, pos, o.f6);)

`define SHUTTLE_REFLECT7(T, f1, k1, f2, k2, f3, k3, f4, k4, f5, k5, f6, k6, f7, k7) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3); \
                             k4::pack_obj(q, o.f4); k5::pack_obj(q, o.f5); k6::pack_obj(q, o.f6); \
                             k7::pack_obj(q, o.f7);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3); k4::unpack_into(b, pos, o.f4); \
                             k5::unpack_into(b, pos, o.f5); k6::unpack_into(b, pos, o.f6); \
                             k7::unpack_into(b, pos, o.f7);)

`define SHUTTLE_REFLECT8(T, f1, k1, f2, k2, f3, k3, f4, k4, f5, k5, f6, k6, f7, k7, f8, k8) \
  `SHUTTLE_DETAIL_CODEC_BODY(T, k1::pack_obj(q, o.f1); k2::pack_obj(q, o.f2); k3::pack_obj(q, o.f3); \
                             k4::pack_obj(q, o.f4); k5::pack_obj(q, o.f5); k6::pack_obj(q, o.f6); \
                             k7::pack_obj(q, o.f7); k8::pack_obj(q, o.f8);, \
                             k1::unpack_into(b, pos, o.f1); k2::unpack_into(b, pos, o.f2); \
                             k3::unpack_into(b, pos, o.f3); k4::unpack_into(b, pos, o.f4); \
                             k5::unpack_into(b, pos, o.f5); k6::unpack_into(b, pos, o.f6); \
                             k7::unpack_into(b, pos, o.f7); k8::unpack_into(b, pos, o.f8);)

`endif  // SHUTTLE_REFLECT_SVH
