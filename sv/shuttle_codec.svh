// SV-side message codec: the C++-style message reflection counterpart for SystemVerilog.
//
// Byte layout: byte-for-byte the C++ side (include/shuttle/codec.hpp) —
//   · fields encoded in declaration order; no names, no padding;
//   · integers big-endian by width (u8/u16/u32/u64, signed = two's complement);
//   · float/double: IEEE754 bit pattern, big-endian (shortreal/real);
//   · bool: u8 0/1; string: u32 big-endian byte count + bytes;
//   · vector: u32 big-endian element count + elements in order;
//   · nested class: expanded recursively, inline, NO length prefix.
//
// Usage (inside the message class; per-arity suffix = field count — SV has no
// variadic macros; each field is a (field, codec) pair):
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
// The macros generate the message-class convention shuttle_publisher #(T) expects:
//   localparam string type_tag = "Txn";   // must equal the C++ struct name
//   function automatic void pack(output byte q[$]);
//   static function automatic Txn unpack(const ref byte b[]);   // whole-frame (sinks)
// plus the codec-surface entry points used when this class appears as a
// nested field of another message:
//   static function automatic void pack_obj(ref byte q[$], Txn o);
//   static function automatic void unpack_into(const ref byte b[], ref int pos, Txn o);
//
// Every codec (builtin below or macro-generated) exposes the same surface:
//   static function void pack_obj(ref byte q[$], input <t> v);       // copy-in: any
//   static function void unpack_into(const ref byte b[], ref int pos, output <t> v);
// convertible actual type is accepted (assignment semantics), so the codec
// name carries the width/encoding contract while the field keeps whatever
// SV type the testbench prefers (logic[31:0], int, bit[31:0], ...). The
// (field, codec) pair in the macro resolves nesting by name at macro-
// expansion time — the manual analogue of C++ Codec<B> recursing into
// Codec<A>.
//
// VCS notes (2018.09, verified by probes in build/):
// 1. `ref` formals demand the EXACT type (Error-[IRPC] for logic[31:0] vs
//    int unsigned, signed vs unsigned, even casts); `input`/`output` formals
//    copy and therefore accept any assignment-compatible actual. All value
//    formals here are input/output; ref appears only on the class object of
//    unpack_into, whose actual is exact by construction.
// 2. An untyped formal is NOT generic on VCS — it silently defaults to a
//    1-bit logic (a field's LSB). Never rely on untyped formals.
// 3. Functions cannot return unpacked arrays, so vector codecs fill a queue
//    via an output formal instead of returning it.
// 4. Multi-line macro bodies need a backslash on every line but the last.
//
// Vector fields: SV cannot return a queue from a function and an untyped
// formal with a queue dimension gets a 1-bit element (VCS note 2), so vector
// codecs take a TYPED queue — declare the field to match exactly (e.g.
// `int unsigned samples[$]` with shuttle_vec_u32, `Inner items[$]` with a
// hand-written shim codec for a custom class):
//   class shuttle_vec_Inner;
//     static function void pack_obj(ref byte q[$], input Inner v[$]);
//       shuttle_u32::pack_obj(q, v.size());
//       foreach (v[i]) Inner::pack_obj(q, v[i]);
//     endfunction
//     static function void unpack_into(const ref byte b[], ref int pos, output Inner v[$]);
//       int unsigned n;
//       shuttle_u32::unpack_into(b, pos, n);
//       v.delete();
//       repeat (n) begin
//         Inner e = new();
//         Inner::unpack_into(b, pos, e);
//         v.push_back(e);
//       end
//     endfunction
//   endclass
//
// Enum fields: SV macros cannot emit the required `e = my_enum'(...)` cast, so
// provide a one-line per-enum shim codec (same shape as the vector shim above):
//   class my_e_codec;
//     static function void pack_obj(ref byte q[$], input int unsigned v);
//       shuttle_u32::pack_obj(q, v);
//     endfunction
//     static function void unpack_into(const ref byte b[], ref int pos, output my_e v);
//       int unsigned t;
//       shuttle_u32::unpack_into(b, pos, t);
//       v = my_e'(t);
//     endfunction
//   endclass
// This file is `include`d INSIDE package shuttle_pkg (see sv/shuttle_pkg.sv);
// the three-piece macros it documents live in sv/shuttle_reflect.svh
// (included after `endpackage`, since `define has compilation-unit scope).
// Use via `import shuttle_pkg::*;` alongside the rest of the framework.
`ifndef SHUTTLE_CODEC_SVH
`define SHUTTLE_CODEC_SVH

  // ── Scalars ───────────────────────────────────────────────────────────
  // pack reads the low width bits of the value (copy-in truncates exactly
  // like a C++ conversion); unpack produces the masked width and the output
  // copy-back narrows/widens or reinterprets two's complement — matching the
  // C++ encode/decode of the same field types.
  class shuttle_u8;
    static function void pack_obj(ref byte q[$], input byte v);
      q.push_back(v);
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output byte v);
      v = b[pos];
      pos += 1;
    endfunction
  endclass

  class shuttle_u16;
    static function void pack_obj(ref byte q[$], input shortint unsigned v);
      q.push_back(byte'(v >> 8));
      q.push_back(byte'(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output shortint unsigned v);
      v = (32'(b[pos]) & 'hff) << 8 | (32'(b[pos+1]) & 'hff);
      pos += 2;
    endfunction
  endclass

  class shuttle_u32;
    static function void pack_obj(ref byte q[$], input int unsigned v);
      q.push_back(byte'(v >> 24));
      q.push_back(byte'(v >> 16));
      q.push_back(byte'(v >> 8));
      q.push_back(byte'(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output int unsigned v);
      v = (32'(b[pos]) & 'hff) << 24 | (32'(b[pos+1]) & 'hff) << 16 |
          (32'(b[pos+2]) & 'hff) << 8 | (32'(b[pos+3]) & 'hff);
      pos += 4;
    endfunction
  endclass

  class shuttle_u64;
    static function void pack_obj(ref byte q[$], input longint unsigned v);
      for (int i = 7; i >= 0; i--) q.push_back(byte'(v >> (8 * i)));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output longint unsigned v);
      v = 0;
      for (int i = 0; i < 8; i++) v = v << 8 | (64'(b[pos+i]) & 'hff);
      pos += 8;
    endfunction
  endclass

  class shuttle_i8;
    static function void pack_obj(ref byte q[$], input byte v);
      q.push_back(v);
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output byte v);
      v = b[pos];
      pos += 1;
    endfunction
  endclass

  class shuttle_i16;
    static function void pack_obj(ref byte q[$], input shortint v);
      q.push_back(byte'(v >> 8));
      q.push_back(byte'(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output shortint v);
      v = (32'(b[pos]) & 'hff) << 8 | (32'(b[pos+1]) & 'hff);
      pos += 2;
    endfunction
  endclass

  class shuttle_i32;
    static function void pack_obj(ref byte q[$], input int v);
      q.push_back(byte'(v >> 24));
      q.push_back(byte'(v >> 16));
      q.push_back(byte'(v >> 8));
      q.push_back(byte'(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output int v);
      v = (32'(b[pos]) & 'hff) << 24 | (32'(b[pos+1]) & 'hff) << 16 |
          (32'(b[pos+2]) & 'hff) << 8 | (32'(b[pos+3]) & 'hff);
      pos += 4;
    endfunction
  endclass

  class shuttle_i64;
    static function void pack_obj(ref byte q[$], input longint v);
      for (int i = 7; i >= 0; i--) q.push_back(byte'(v >> (8 * i)));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output longint v);
      v = 0;
      for (int i = 0; i < 8; i++) v = v << 8 | (64'(b[pos+i]) & 'hff);
      pos += 8;
    endfunction
  endclass

  class shuttle_f32;
    static function void pack_obj(ref byte q[$], input shortreal v);
      shuttle_u32::pack_obj(q, $shortrealtobits(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output shortreal v);
      int unsigned t;
      shuttle_u32::unpack_into(b, pos, t);
      v = $bitstoshortreal(t);
    endfunction
  endclass

  class shuttle_f64;
    static function void pack_obj(ref byte q[$], input real v);
      shuttle_u64::pack_obj(q, $realtobits(v));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output real v);
      longint unsigned t;
      shuttle_u64::unpack_into(b, pos, t);
      v = $bitstoreal(t);
    endfunction
  endclass

  class shuttle_bool;
    static function void pack_obj(ref byte q[$], input bit v);
      shuttle_u8::pack_obj(q, v ? 8'd1 : 8'd0);
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output bit v);
      byte t;
      shuttle_u8::unpack_into(b, pos, t);
      v = t != 0;
    endfunction
  endclass

  class shuttle_string;
    static function void pack_obj(ref byte q[$], input string v);
      shuttle_u32::pack_obj(q, v.len());
      for (int i = 0; i < v.len(); i++) q.push_back(v.getc(i));
    endfunction
    static function void unpack_into(const ref byte b[], ref int pos, output string v);
      int unsigned n;
      shuttle_u32::unpack_into(b, pos, n);
      if (n > b.size() - pos) $fatal(1, "shuttle_codec: string length out of bounds");
      v = "";
      for (int i = 0; i < n; i++) v = {v, b[pos+i]};
      pos += n;
    endfunction
  endclass

  // ── Vectors (u32 element-count prefix + elements; C++ std::vector<T>) ──
  // Queue formals are typed (VCS note 2); declare the field to match exactly.
`define SHUTTLE_DETAIL_VEC_CODEC(NAME, ELEM_T, VECT, ELEM_CODEC) \
  class NAME; \
    static function void pack_obj(ref byte q[$], input VECT v[$]); \
      shuttle_u32::pack_obj(q, v.size()); \
      foreach (v[i]) ELEM_CODEC::pack_obj(q, v[i]); \
    endfunction \
    static function void unpack_into(const ref byte b[], ref int pos, output VECT v[$]); \
      int unsigned n; \
      shuttle_u32::unpack_into(b, pos, n); \
      v.delete(); \
      repeat (n) begin \
        ELEM_T e; \
        ELEM_CODEC::unpack_into(b, pos, e); \
        v.push_back(e); \
      end \
    endfunction \
  endclass

  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_u8,     byte,              byte,              shuttle_u8)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_u16,    shortint unsigned, shortint unsigned, shuttle_u16)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_u32,    int unsigned,      int unsigned,      shuttle_u32)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_u64,    longint unsigned,  longint unsigned,  shuttle_u64)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_i8,     byte,              byte,              shuttle_i8)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_i16,    shortint,          shortint,          shuttle_i16)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_i32,    int,               int,               shuttle_i32)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_i64,    longint,           longint,           shuttle_i64)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_f32,    shortreal,         shortreal,         shuttle_f32)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_f64,    real,              real,              shuttle_f64)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_bool,   bit,               bit,               shuttle_bool)
  `SHUTTLE_DETAIL_VEC_CODEC(shuttle_vec_string, string,            string,            shuttle_string)


`endif  // SHUTTLE_CODEC_SVH
