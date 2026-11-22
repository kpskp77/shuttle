#pragma once

// Message codec: the unified binary wire format for cross-language payloads, plus the C++-side
// serialization entry points.
//
// Wire format (semantics aligned with SV streaming operators; identical field order):
//   · fields are encoded in declaration order — no field names, no padding, no alignment;
//   · integers/floats are written big-endian (matches the SV-side {>>{...}}
//     stream: declaration order, each field most-significant byte first);
//   · string / vector<T>: uint32 big-endian length prefix + elements in order;
//   · enum: encoded as its underlying integer type (the SV-side enum base type must have
//     the same width);
//   · nested structs are expanded recursively.
//
// Note: typed delivery within pure C++ does not go through this layer (zero copy
// shared_ptr<const T>); encoding/decoding is triggered only for cross-language delivery, byte
// subscribers (subscribe_bytes), or messages originating from SV.
//
// User types gain codec support via SHUTTLE_REFLECT(Type, field1, field2, ...); unregistered types
// still work on pure-C++ topics (zero-copy path).

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <shuttle/exception.hpp>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>

namespace shuttle {

// ── Writer: big-endian byte-stream writer ──────────────────────────────
class Writer {
public:
  void u8(std::uint8_t v) { buf_.push_back(static_cast<std::byte>(v)); }
  // Wider integers are written with a single size()/resize() pair: one capacity
  // check and one tail update instead of the recursive chain of push_backs.
  void u16(std::uint16_t v) { put_be(v, 2); }
  void u32(std::uint32_t v) { put_be(v, 4); }
  void u64(std::uint64_t v) { put_be(v, 8); }
  void raw(const void* p, std::size_t n) {  // append verbatim (string bytes)
    const auto* b = static_cast<const std::byte*>(p);
    buf_.insert(buf_.end(), b, b + n);
  }

  // Pre-size the buffer when the encoded length is known up front (strings,
  // arithmetic vectors): avoids repeated growth for large payloads.
  void reserve(std::size_t n) { buf_.reserve(n); }
  [[nodiscard]] std::size_t size() const noexcept { return buf_.size(); }

  [[nodiscard]] const std::vector<std::byte>& buf() const { return buf_; }
  std::vector<std::byte> take() { return std::move(buf_); }

private:
  void put_be(std::uint64_t v, std::size_t n) {
    const std::size_t at = buf_.size();
    buf_.resize(at + n);
    for (std::size_t i = 0; i < n; ++i)
      buf_[at + i] = static_cast<std::byte>(v >> (8 * (n - 1 - i)));
  }

  std::vector<std::byte> buf_;
};

// ── Reader: bounds-checked big-endian byte-stream reader ───────────────
// Any out-of-bounds read throws DecodeError; undefined data is never read.
class Reader {
public:
  explicit Reader(const std::vector<std::byte>& buf) : p_(buf.data()), n_(buf.size()) {}
  Reader(const std::byte* p, std::size_t n) : p_(p), n_(n) {}

  // One take() per integer (one bounds check) instead of one per byte.
  std::uint8_t u8() { return std::to_integer<std::uint8_t>(*take(1)); }
  std::uint16_t u16() {
    const std::byte* b = take(2);
    return static_cast<std::uint16_t>((std::to_integer<std::uint16_t>(b[0]) << 8) |
                                      std::to_integer<std::uint16_t>(b[1]));
  }
  std::uint32_t u32() {
    const std::byte* b = take(4);
    return (std::to_integer<std::uint32_t>(b[0]) << 24) |
           (std::to_integer<std::uint32_t>(b[1]) << 16) |
           (std::to_integer<std::uint32_t>(b[2]) << 8) | std::to_integer<std::uint32_t>(b[3]);
  }
  std::uint64_t u64() {
    const std::byte* b = take(8);
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | std::to_integer<std::uint64_t>(b[i]);
    return v;
  }
  void raw(void* out, std::size_t n) {
    const std::byte* b = take(n);
    std::memcpy(out, b, n);
  }

  [[nodiscard]] bool eof() const { return pos_ == n_; }
  // Bytes left — lets length-prefixed decoders reject an impossible length before
  // allocating anything for it.
  [[nodiscard]] std::size_t remaining() const noexcept { return n_ - pos_; }

private:
  const std::byte* take(std::size_t n) {
    if (n > n_ - pos_) throw DecodeError("shuttle: decode out of bounds (byte stream too short)");
    const std::byte* r = p_ + pos_;
    pos_ += n;
    return r;
  }

  const std::byte* p_;
  std::size_t n_;
  std::size_t pos_ = 0;
};

// ── Codec<T>: per-type codec capability ────────────────────────────────
// Primary template: available = false, meaning the type is not cross-language capable (still usable
// on pure-C++ topics). Specializations provide available = true + encode/decode.
template <typename T, typename Enable = void>
struct Codec {
  static constexpr bool available = false;
};

template <typename T>
inline constexpr bool has_codec_v = Codec<T>::available;

// bool: u8 (0/1)
template <>
struct Codec<bool> {
  static constexpr bool available = true;
  static void encode(Writer& w, const bool& v) { w.u8(v ? 1 : 0); }
  static bool decode(Reader& r) { return r.u8() != 0; }
};

// integers (except bool): big-endian by width
template <typename T>
struct Codec<T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>>> {
  static constexpr bool available = true;
  using U = std::make_unsigned_t<T>;
  static void encode(Writer& w, const T& v) {
    const U u = static_cast<U>(v);
    if constexpr (sizeof(T) == 1)
      w.u8(static_cast<std::uint8_t>(u));
    else if constexpr (sizeof(T) == 2)
      w.u16(static_cast<std::uint16_t>(u));
    else if constexpr (sizeof(T) == 4)
      w.u32(static_cast<std::uint32_t>(u));
    else if constexpr (sizeof(T) == 8)
      w.u64(static_cast<std::uint64_t>(u));
    else
      static_assert(sizeof(T) <= 8, "shuttle: unsupported integer width");
  }
  static T decode(Reader& r) {
    U u;
    if constexpr (sizeof(T) == 1)
      u = r.u8();
    else if constexpr (sizeof(T) == 2)
      u = r.u16();
    else if constexpr (sizeof(T) == 4)
      u = r.u32();
    else
      u = r.u64();
    return static_cast<T>(u);
  }
};

// floating point: IEEE754 bit pattern, big-endian
template <typename T>
struct Codec<T, std::enable_if_t<std::is_floating_point_v<T>>> {
  static constexpr bool available = true;
  static void encode(Writer& w, const T& v) {
    if constexpr (sizeof(T) == 4) {
      std::uint32_t u;
      std::memcpy(&u, &v, 4);
      w.u32(u);
    } else {
      std::uint64_t u;
      std::memcpy(&u, &v, 8);
      w.u64(u);
    }
  }
  static T decode(Reader& r) {
    if constexpr (sizeof(T) == 4) {
      const std::uint32_t u = r.u32();
      T v;
      std::memcpy(&v, &u, 4);
      return v;
    } else {
      const std::uint64_t u = r.u64();
      T v;
      std::memcpy(&v, &u, 8);
      return v;
    }
  }
};

// std::string: u32 length prefix + UTF-8 bytes
template <>
struct Codec<std::string> {
  static constexpr bool available = true;
  static void encode(Writer& w, const std::string& v) {
    if (v.size() > UINT32_MAX) throw PublishError("shuttle: string exceeds uint32 length limit");
    w.reserve(w.size() + 4 + v.size());  // length prefix + payload in one growth step
    w.u32(static_cast<std::uint32_t>(v.size()));
    w.raw(v.data(), v.size());
  }
  static std::string decode(Reader& r) {
    const std::uint32_t n = r.u32();
    // A corrupt length must not trigger a huge allocation before the bounds
    // check: every string byte is exactly one byte on the wire.
    if (n > r.remaining())
      throw DecodeError("shuttle: decode out of bounds (string length exceeds remaining bytes)");
    std::string v(n, '\0');
    r.raw(v.data(), n);
    return v;
  }
};

// enum: encoded as its underlying integer type (the SV-side base type width must match)
template <typename T>
struct Codec<T, std::enable_if_t<std::is_enum_v<T>>> {
  static constexpr bool available = true;
  using U = std::underlying_type_t<T>;
  static void encode(Writer& w, const T& v) { Codec<U>::encode(w, static_cast<U>(v)); }
  static T decode(Reader& r) { return static_cast<T>(Codec<U>::decode(r)); }
};

// std::vector<T>: u32 element-count prefix + elements in order
template <typename T, typename Alloc>
struct Codec<std::vector<T, Alloc>> {
  static constexpr bool available = Codec<T>::available;
  static void encode(Writer& w, const std::vector<T, Alloc>& v) {
    static_assert(Codec<T>::available, "shuttle: vector element type is not encodable");
    if (v.size() > UINT32_MAX)
      throw PublishError("shuttle: vector exceeds uint32 element-count limit");
    // Fixed-width elements have a known wire size: grow the buffer once.
    if constexpr (std::is_arithmetic_v<T>) w.reserve(w.size() + 4 + v.size() * sizeof(T));
    w.u32(static_cast<std::uint32_t>(v.size()));
    for (const T& e : v) Codec<T>::encode(w, e);
  }
  static std::vector<T, Alloc> decode(Reader& r) {
    const std::uint32_t n = r.u32();
    std::vector<T, Alloc> v;
    // Cap the reservation by the bytes actually available: a corrupt count cannot
    // demand a multi-gigabyte allocation (the per-element read still bounds-checks).
    v.reserve(std::min<std::size_t>(n, r.remaining()));
    for (std::uint32_t i = 0; i < n; ++i) v.push_back(Codec<T>::decode(r));
    return v;
  }
};

// std::array<T, N>: elements in order, no length prefix
template <typename T, std::size_t N>
struct Codec<std::array<T, N>> {
  static constexpr bool available = Codec<T>::available;
  static void encode(Writer& w, const std::array<T, N>& v) {
    static_assert(Codec<T>::available, "shuttle: array element type is not encodable");
    for (const T& e : v) Codec<T>::encode(w, e);
  }
  static std::array<T, N> decode(Reader& r) {
    std::array<T, N> v{};
    for (auto& e : v) e = Codec<T>::decode(r);
    return v;
  }
};

// ── Top-level encode/decode entry points ────────────────────────────────
template <typename T>
std::vector<std::byte> encode(const T& v) {
  static_assert(has_codec_v<T>,
                "shuttle: type has no codec registered; use SHUTTLE_REFLECT to register it (safe "
                "to ignore on pure-C++ topics)");
  Writer w;
  Codec<T>::encode(w, v);
  return w.take();
}

template <typename T>
T decode(const std::vector<std::byte>& b) {
  static_assert(has_codec_v<T>, "shuttle: type has no codec registered");
  Reader r(b);
  T v = Codec<T>::decode(r);
  if (!r.eof()) throw DecodeError("shuttle: trailing extra bytes after decode");
  return v;
}

// ── Type tags ───────────────────────────────────────────────────────────
// Priority: Codec<T>::tag() (generated by SHUTTLE_REFLECT, the type's name string) > typeid name.
// Cross-language topics must use a type with tag() (the SV-side localparam type_tag must equal it).
template <typename T, typename = void>
struct TypeTag {
  static std::string get() { return typeid(T).name(); }
};
template <typename T>
struct TypeTag<T, std::void_t<decltype(Codec<T>::tag())>> {
  static std::string get() { return Codec<T>::tag(); }
};
template <typename T>
std::string type_tag_of() {
  return TypeTag<T>::get();
}

}  // namespace shuttle

// ── SHUTTLE_REFLECT: generate a Codec specialization for a user type ────
// Usage (at global scope):
//   struct Txn { uint32_t addr; uint32_t data; std::string tag; };
//   SHUTTLE_REFLECT(Txn, addr, data, tag)
// Fields are encoded in declaration order, up to 64; member types must belong to the codec subset
// (integers / floats / bool / string / enum / vector / array / already-registered nested structs).

#define SHUTTLE_DETAIL_CAT_(a, b) a##b
#define SHUTTLE_DETAIL_CAT(a, b) SHUTTLE_DETAIL_CAT_(a, b)

#define SHUTTLE_DETAIL_FE1(m, t, a) m(t, a)
#define SHUTTLE_DETAIL_FE2(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE1(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE3(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE2(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE4(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE3(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE5(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE4(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE6(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE5(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE7(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE6(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE8(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE7(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE9(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE8(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE10(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE9(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE11(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE10(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE12(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE11(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE13(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE12(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE14(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE13(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE15(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE14(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE16(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE15(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE17(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE16(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE18(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE17(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE19(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE18(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE20(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE19(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE21(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE20(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE22(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE21(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE23(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE22(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE24(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE23(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE25(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE24(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE26(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE25(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE27(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE26(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE28(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE27(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE29(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE28(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE30(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE29(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE31(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE30(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE32(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE31(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE33(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE32(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE34(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE33(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE35(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE34(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE36(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE35(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE37(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE36(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE38(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE37(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE39(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE38(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE40(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE39(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE41(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE40(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE42(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE41(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE43(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE42(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE44(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE43(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE45(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE44(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE46(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE45(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE47(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE46(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE48(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE47(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE49(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE48(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE50(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE49(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE51(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE50(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE52(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE51(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE53(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE52(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE54(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE53(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE55(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE54(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE56(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE55(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE57(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE56(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE58(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE57(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE59(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE58(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE60(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE59(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE61(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE60(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE62(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE61(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE63(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE62(m, t, __VA_ARGS__)
#define SHUTTLE_DETAIL_FE64(m, t, a, ...) m(t, a) SHUTTLE_DETAIL_FE63(m, t, __VA_ARGS__)

#define SHUTTLE_DETAIL_COUNT_(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, \
                              _16, _17, _18, _19, _20, _21, _22, _23, _24, _25, _26, _27, _28,  \
                              _29, _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, _40, _41,  \
                              _42, _43, _44, _45, _46, _47, _48, _49, _50, _51, _52, _53, _54,  \
                              _55, _56, _57, _58, _59, _60, _61, _62, _63, _64, N, ...)         \
  N
#define SHUTTLE_DETAIL_COUNT(...)                                                                \
  SHUTTLE_DETAIL_COUNT_(__VA_ARGS__, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, \
                        49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32,  \
                        31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14,  \
                        13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

#define SHUTTLE_DETAIL_FOR_EACH(m, t, ...) \
  SHUTTLE_DETAIL_CAT(SHUTTLE_DETAIL_FE, SHUTTLE_DETAIL_COUNT(__VA_ARGS__))(m, t, __VA_ARGS__)

#define SHUTTLE_DETAIL_ENC(t, f) ::shuttle::Codec<std::decay_t<decltype(obj.f)>>::encode(w, obj.f);
#define SHUTTLE_DETAIL_DEC(t, f) obj.f = ::shuttle::Codec<std::decay_t<decltype(obj.f)>>::decode(r);

#define SHUTTLE_REFLECT(Type, ...)                                   \
  namespace shuttle {                                                \
  template <>                                                        \
  struct Codec<Type> {                                               \
    static constexpr bool available = true;                          \
    static const char* tag() { return #Type; }                       \
    static void encode(::shuttle::Writer& w, const Type& obj) {      \
      SHUTTLE_DETAIL_FOR_EACH(SHUTTLE_DETAIL_ENC, Type, __VA_ARGS__) \
    }                                                                \
    static Type decode(::shuttle::Reader& r) {                       \
      Type obj{};                                                    \
      SHUTTLE_DETAIL_FOR_EACH(SHUTTLE_DETAIL_DEC, Type, __VA_ARGS__) \
      return obj;                                                    \
    }                                                                \
  };                                                                 \
  }
