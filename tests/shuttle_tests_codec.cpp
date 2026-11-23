// Codec semantics self-checks: wire format (big-endian, length prefixes, declaration order)
//
// The golden-byte assertions are the anchor of the cross-language contract: the expected
// bytes in this file must match byte for byte the "Wire format" section of docs/backends.md
// and the SV-side examples/shuttle_demo.sv ({>>{...}} packing); any change on either side fails
// here first.

// Suite entry: void shuttle_tests_codec() is called first by tests/shuttle_tests.cpp (wire format
// before any broker state exists); the [PASS] label is the function name.

#include <array>
#include <cstdint>
#include <shuttle/codec.hpp>
#include <shuttle/detail/abi.hpp>
#include <shuttle/meta.hpp>
#include <stdexcept>
#include <string>
#include <vector>

#include "shuttle_tests_runner.hpp"

using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── Test message types (SHUTTLE_REFLECT can only be used at global scope: it expands into
//    `namespace shuttle { template <> struct Codec<T> }`, which requires reopening the global
//    shuttle namespace)
struct UartTx {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(UartTx, addr, data)

struct Inner {
  std::uint16_t a = 0;
  std::int8_t b = 0;
};
SHUTTLE_REFLECT(Inner, a, b)

enum class Mode : std::uint8_t { idle = 1, run = 2 };

// Covering the codec subset: scalars, enums, string, vector, fixed-width arrays, nested structs
struct Wide {
  bool flag = false;
  std::uint8_t u8 = 0;
  std::int16_t i16 = 0;
  std::uint64_t u64 = 0;
  float f32 = 0.0f;
  double f64 = 0.0;
  Mode mode = Mode::idle;
  std::string name;
  std::vector<std::uint16_t> vec;
  std::array<std::uint8_t, 3> arr{};
  Inner nested;
};
SHUTTLE_REFLECT(Wide, flag, u8, i16, u64, f32, f64, mode, name, vec, arr, nested)

// Three levels of nesting (used by case10): the codec recursion has no depth limit and every
// level expands inline, field order = declaration order, no per-level headers
struct NestL1 {
  std::int8_t v = 0;
};
SHUTTLE_REFLECT(NestL1, v)

struct NestL2 {
  std::uint16_t tag = 0;
  NestL1 a;
  NestL1 b;
};
SHUTTLE_REFLECT(NestL2, tag, a, b)

struct NestL3 {
  std::uint8_t head = 0;
  NestL2 body;
  NestL1 tail;
};
SHUTTLE_REFLECT(NestL3, head, body, tail)

// Inheritance chain registered via SHUTTLE_REFLECT_DERIVED (used by case12): each level
// delegates its base subobject to the base's codec instead of repeating field names
struct InhBase {
  std::uint32_t seq = 0;
};
struct InhMid : InhBase {
  std::uint32_t payload = 0;
};
struct InhDeep : InhMid {
  std::uint16_t flag = 0;
};
SHUTTLE_REFLECT(InhBase, seq)
SHUTTLE_REFLECT_DERIVED(InhMid, InhBase, payload)
SHUTTLE_REFLECT_DERIVED(InhDeep, InhMid, flag)

// A message type declared inside a namespace (used by case13). SHUTTLE_REFLECT can only run at
// global scope, so the type is registered through a global unqualified alias: the macro's first
// argument doubles as the type tag, which makes the alias spelling "NsTx" the thing an SV class
// must declare as type_tag. Reflecting the qualified name instead keeps the namespace in the tag
// ("codec_ns::QualifiedTx"), a string no SV class can carry.
namespace codec_ns {
struct NsTx {
  std::uint32_t addr = 0;
  std::uint8_t code = 0;
};
struct QualifiedTx {
  std::uint16_t v = 0;
};
}  // namespace codec_ns

using NsTx = codec_ns::NsTx;
SHUTTLE_REFLECT(NsTx, addr, code)
SHUTTLE_REFLECT(codec_ns::QualifiedTx, v)

// A type with no codec (used by the static-capability probe)
struct NotReflected {
  int x = 0;
};

// More fields than the old 16-entry dispatch table (used by case11): the FOR_EACH cascade now
// covers up to 64
struct Many {
  std::uint32_t f00 = 0;
  std::uint32_t f01 = 0;
  std::uint32_t f02 = 0;
  std::uint32_t f03 = 0;
  std::uint32_t f04 = 0;
  std::uint32_t f05 = 0;
  std::uint32_t f06 = 0;
  std::uint32_t f07 = 0;
  std::uint32_t f08 = 0;
  std::uint32_t f09 = 0;
  std::uint32_t f10 = 0;
  std::uint32_t f11 = 0;
  std::uint32_t f12 = 0;
  std::uint32_t f13 = 0;
  std::uint32_t f14 = 0;
  std::uint32_t f15 = 0;
  std::uint32_t f16 = 0;
  std::uint32_t f17 = 0;
  std::uint32_t f18 = 0;
  std::uint32_t f19 = 0;
  std::uint32_t f20 = 0;
  std::uint32_t f21 = 0;
  std::uint32_t f22 = 0;
  std::uint32_t f23 = 0;
};
SHUTTLE_REFLECT(Many, f00, f01, f02, f03, f04, f05, f06, f07, f08, f09, f10, f11, f12, f13, f14,
                f15, f16, f17, f18, f19, f20, f21, f22, f23)

namespace {

using bytes = std::vector<std::byte>;

// Build the expected byte stream from hex literals: {0x00,0x12,...}
bytes hex(std::initializer_list<int> v) {
  bytes out;
  out.reserve(v.size());
  for (int b : v) out.push_back(static_cast<std::byte>(static_cast<unsigned>(b)));
  return out;
}

bool same(const bytes& a, const bytes& b) {
  return a == b;
}

std::string dump(const bytes& b) {
  std::string s;
  char buf[8];
  for (std::size_t i = 0; i < b.size(); ++i) {
    std::snprintf(buf, sizeof buf, "%02x ", static_cast<unsigned>(b[i]));
    s += buf;
  }
  return s;
}

void expect_bytes(const bytes& actual, const bytes& wanted, const char* what) {
  if (!same(actual, wanted)) {
    std::string msg = what;
    msg += ": actual=[" + dump(actual) + "] wanted=[" + dump(wanted) + "]";
    throw std::logic_error(msg);
  }
}

}  // namespace

void shuttle_tests_codec() {
  // case1: golden vector — byte-for-byte counterpart of the SV {>>{32'h12, 32'h3c}}
  run_case(__func__, "case1", [] {
    UartTx tx{0x12, 0x3C};
    expect_bytes(shuttle::encode(tx), hex({0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x3C}),
                 "UartTx big-endian wire order");
  });

  // case2: big-endian writing of integers of each width (two's complement for negatives)
  run_case(__func__, "case2", [] {
    expect_bytes(shuttle::encode<std::uint16_t>(0xABCD), hex({0xAB, 0xCD}), "u16 big-endian");
    expect_bytes(shuttle::encode<std::int8_t>(-1), hex({0xFF}), "i8 two's complement");
    expect_bytes(shuttle::encode<std::int16_t>(-2), hex({0xFF, 0xFE}),
                 "i16 two's complement, big-endian");
    expect_bytes(shuttle::encode<std::uint32_t>(1), hex({0x00, 0x00, 0x00, 0x01}),
                 "u32 big-endian");
    expect_bytes(shuttle::encode<bool>(true), hex({0x01}), "bool single byte");
    expect_bytes(shuttle::encode<bool>(false), hex({0x00}), "bool false");
  });

  // case3: floats in big-endian IEEE754
  run_case(__func__, "case3", [] {
    expect_bytes(shuttle::encode<float>(1.0f), hex({0x3F, 0x80, 0x00, 0x00}), "float 1.0");
    expect_bytes(shuttle::encode<double>(1.0),
                 hex({0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}), "double 1.0");
  });

  // case4: uint32 length prefixes for string / vector
  run_case(__func__, "case4", [] {
    expect_bytes(shuttle::encode<std::string>("hi"), hex({0x00, 0x00, 0x00, 0x02, 'h', 'i'}),
                 "string length prefix");
    expect_bytes(shuttle::encode<std::string>(""), hex({0x00, 0x00, 0x00, 0x00}), "empty string");
    expect_bytes(shuttle::encode(std::vector<std::uint32_t>{1, 2}),
                 hex({0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}),
                 "vector element-count prefix");
    expect_bytes(shuttle::encode(std::vector<std::uint8_t>{}), hex({0x00, 0x00, 0x00, 0x00}),
                 "empty vector");
    // fixed-width arrays have no length prefix
    expect_bytes(shuttle::encode(std::array<std::uint8_t, 3>{0x0A, 0x0B, 0x0C}),
                 hex({0x0A, 0x0B, 0x0C}), "array without prefix");
  });

  // case5: enums encode as their underlying type (1 byte here)
  run_case(__func__, "case5", [] {
    expect_bytes(shuttle::encode(Mode::run), hex({0x02}), "enum per base type");
    expect_eq(static_cast<int>(shuttle::decode<Mode>(hex({0x01}))), 1, "enum round trip");
  });

  // case6: nested structs expand recursively (field order = declaration order)
  run_case(__func__, "case6", [] {
    Inner in{0x0102, -3};
    expect_bytes(shuttle::encode(in), hex({0x01, 0x02, 0xFD}), "nested struct flattened");
  });

  // case7: full-type round trip (nested included)
  run_case(__func__, "case7", [] {
    Wide w;
    w.flag = true;
    w.u8 = 0x7F;
    w.i16 = -1234;
    w.u64 = 0x0123456789ABCDEFull;
    w.f32 = -2.5f;
    w.f64 = 3.141592653589793;
    w.mode = Mode::run;
    w.name = "uart-\xC3\xBCnicode";  // UTF-8 multibyte
    w.vec = {0x1111, 0x2222};
    w.arr = {1, 2, 3};
    w.nested = Inner{0x0405, -6};

    const bytes enc = shuttle::encode(w);
    const Wide back = shuttle::decode<Wide>(enc);
    expect(back.flag == w.flag, "bool round trip");
    expect_eq(back.u8, w.u8, "u8 round trip");
    expect_eq(back.i16, w.i16, "i16 round trip");
    expect(back.u64 == w.u64, "u64 round trip");
    expect(back.f32 == w.f32, "float round trip");
    expect(back.f64 == w.f64, "double round trip");
    expect(back.mode == w.mode, "enum round trip");
    expect(back.name == w.name, "string round trip");
    expect(back.vec == w.vec, "vector round trip");
    expect(back.arr == w.arr, "array round trip");
    expect(back.nested.a == w.nested.a && back.nested.b == w.nested.b, "nested struct round trip");
    const std::size_t wanted = 1 /*flag*/ + 1 /*u8*/ + 2 /*i16*/ + 8 /*u64*/ + 4 /*f32*/ +
                               8 /*f64*/ + 1 /*mode*/ + (4 + w.name.size()) /*string*/ +
                               (4 + w.vec.size() * 2) /*vector*/ + 3 /*array*/ + (2 + 1) /*nested*/;
    expect_eq(static_cast<long long>(enc.size()), static_cast<long long>(wanted),
              "encoded length equals the sum of field widths");
  });

  // case8: Reader out-of-bounds reads must throw DecodeError (no out-of-bounds memory reads)
  run_case(__func__, "case8", [] {
    bool threw = false;
    try {
      (void)shuttle::decode<std::uint32_t>(hex({0x00, 0x01}));
    } catch (const shuttle::DecodeError&) {
      threw = true;
    }
    expect(threw, "a short byte stream throws DecodeError");

    threw = false;
    try {
      (void)shuttle::decode<std::string>(
          hex({0x00, 0x00, 0x00, 0x05, 'a'}));  // declares 5, actually 1
    } catch (const shuttle::DecodeError&) {
      threw = true;
    }
    expect(threw, "an overreaching length prefix throws DecodeError");
  });

  // case9: type tags and capability probe
  run_case(__func__, "case9", [] {
    expect(shuttle::type_tag_of<UartTx>() == "UartTx",
           "type tag taken from the SHUTTLE_REFLECT name");
    expect(shuttle::type_tag_of<Wide>() == "Wide", "tag of the second type");
    expect(shuttle::has_codec_v<std::uint32_t>, "scalars have a codec");
    expect(shuttle::has_codec_v<std::string>, "string has a codec");
    expect(!shuttle::has_codec_v<NotReflected>, "an unreflected type has no codec");
    expect(!shuttle::has_codec_v<int*>, "pointers have no codec");
    // an unreflected type provides no encode/decode function pointers (cross-language delivery is
    // rejected)
    const shuttle::detail::TypeOps* ops = shuttle::detail::type_ops<NotReflected>();
    expect(ops->encode == nullptr && ops->decode == nullptr,
           "without a codec, encode/decode are null");
  });

  // case10: three levels of nesting — flattened inline at every level, decoded back field by field
  run_case(__func__, "case10", [] {
    NestL3 in;
    in.head = 0x7F;
    in.body.tag = 0xBEEF;
    in.body.a.v = -5;
    in.body.b.v = 3;
    in.tail.v = -1;
    // 1 + (2 + 1 + 1) + 1 = 6 bytes, no per-level headers
    expect_bytes(shuttle::encode(in), hex({0x7F, 0xBE, 0xEF, 0xFB, 0x03, 0xFF}),
                 "three-level nesting flattens inline");
    const NestL3 back = shuttle::decode<NestL3>(shuttle::encode(in));
    expect(back.head == in.head && back.body.tag == in.body.tag && back.body.a.v == in.body.a.v &&
               back.body.b.v == in.body.b.v && back.tail.v == in.tail.v,
           "three-level nesting round trip");
  });

  // case11: more than 16 fields — the FOR_EACH dispatch table covers up to 64
  run_case(__func__, "case11", [] {
    Many m{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
    const bytes enc = shuttle::encode(m);
    expect_eq(static_cast<long long>(enc.size()), 24 * 4, "24 fields x 4 bytes");
    const Many back = shuttle::decode<Many>(enc);
    expect_bytes(shuttle::encode(back), enc, "24-field round trip is byte-stable");
  });

  // case12: inheritance chain via SHUTTLE_REFLECT_DERIVED — each level's wire bytes are the
  // base prefix plus its own fields; the chain composes through Codec<Base>
  run_case(__func__, "case12", [] {
    InhDeep d;
    d.seq = 1;
    d.payload = 0xdeadbeef;
    d.flag = 0x2a;
    const bytes enc = shuttle::encode(d);
    expect_eq(static_cast<long long>(enc.size()), 10, "4 + 4 + 2 bytes, base first");
    InhBase b;
    b.seq = 1;
    const bytes encb = shuttle::encode(b);
    expect(enc.size() >= encb.size() && std::equal(encb.begin(), encb.end(), enc.begin()),
           "InhBase encoding is a prefix of InhDeep");
    const InhDeep back = shuttle::decode<InhDeep>(enc);
    expect_eq(static_cast<long long>(back.seq), 1, "inherited seq");
    expect_eq(static_cast<long long>(back.payload), 0xdeadbeef, "mid-level payload");
    expect_eq(static_cast<long long>(back.flag), 0x2a, "own flag");
  });

  // case13: a type declared inside a namespace — the global unqualified alias fixes the tag; the
  // qualified spelling keeps the namespace in the tag, which is why the alias is the usable route
  run_case(__func__, "case13", [] {
    expect(shuttle::type_tag_of<codec_ns::NsTx>() == "NsTx",
           "the tag is the alias spelling, not the namespace-qualified name");
    expect(shuttle::has_codec_v<codec_ns::NsTx>, "the alias registered the original type");
    NsTx tx{0x01020304, 0xAB};
    expect_bytes(shuttle::encode(tx), hex({0x01, 0x02, 0x03, 0x04, 0xAB}),
                 "a namespaced type encodes like any other struct");
    const NsTx back = shuttle::decode<codec_ns::NsTx>(shuttle::encode(tx));
    expect_eq(static_cast<long long>(back.addr), static_cast<long long>(tx.addr),
              "namespaced round trip: addr");
    expect_eq(static_cast<long long>(back.code), static_cast<long long>(tx.code),
              "namespaced round trip: code");
    expect(shuttle::type_tag_of<codec_ns::QualifiedTx>() == "codec_ns::QualifiedTx",
           "reflecting the qualified name keeps the namespace in the tag");
    expect_bytes(shuttle::encode(codec_ns::QualifiedTx{0x1234}), hex({0x12, 0x34}),
                 "the qualified spelling still encodes normally");
  });

  report(__func__);
}
