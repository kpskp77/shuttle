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
// User types need NO registration: any aggregate whose fields all have codecs is
// cross-language capable out of the box (macro-free reflection, see reflect.hpp).
// Requirements: aggregate (no base classes / user constructors / private fields),
// every field default-constructible + assignable, no reference members.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <shuttle/exception.hpp>
#include <shuttle/reflect.hpp>
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

// std::array<T, N> is itself an aggregate: keep it on the explicit specialization
// below (the aggregate codec would otherwise compete with it ambiguously).
template <typename T>
struct is_std_array : std::false_type {};
template <typename T, std::size_t N>
struct is_std_array<std::array<T, N>> : std::true_type {};

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

// ── Transitive encodability of aggregates ──────────────────────────────
// Codec<T>::available must be FALSE (not a hard error) when any field lacks a
// codec, so pure-C++ topics with arbitrary payload types keep compiling. The
// probe mirrors the arity detection, but with a ubiq that only "converts" to
// encodable types: T{probe × field-count} is well-formed iff every field is
// encodable, recursively (aggregates specialize encodable below; every other
// type defers to its Codec specialization).
template <typename T, typename = void>
struct encodable : std::bool_constant<Codec<T>::available> {};

struct codec_ubiq {
  template <typename Type, std::enable_if_t<encodable<Type>::value, int> = 0>
  operator Type() const noexcept;  // intentionally undefined: SFINAE probe only
};

template <typename T, std::size_t... I>
auto encodable_probe(std::index_sequence<I...>)
    -> decltype(T{(static_cast<void>(I), codec_ubiq{})...}, std::true_type{});
template <typename T, std::size_t... I>
std::false_type encodable_probe(...);

template <typename T, std::size_t N>
struct encodable_arity_ok : decltype(encodable_probe<T>(std::make_index_sequence<N>{})) {};

template <typename T>
struct encodable<T, std::enable_if_t<std::is_aggregate_v<T> && !is_std_array<T>::value>>
    : std::bool_constant<encodable_arity_ok<T, detail::arity<T, 32>::value>::value> {};

// Aggregate message types: fields encoded in declaration order via macro-free
// reflection (reflect.hpp) — no registration step, fields beyond 32 are a
// compile error, and the type tag is the source-level name ("my::Txn"),
// matching the SV-side localparam type_tag convention. available is false
// when any field lacks a codec (see encodable above).
template <typename T>
struct Codec<T, std::enable_if_t<std::is_aggregate_v<T> && !is_std_array<T>::value>> {
  static constexpr bool available = encodable<T>::value;
  static std::string tag() { return std::string(detail::type_name<T>()); }
  static void encode(Writer& w, const T& obj) {
    static_assert(available, "shuttle: message has a field with no codec");
    detail::for_each_field(obj, [&w](const auto& f) { Codec<std::decay_t<decltype(f)>>::encode(w, f); });
  }
  static T decode(Reader& r) {
    static_assert(available, "shuttle: message has a field with no codec");
    T obj{};
    detail::for_each_field(obj, [&r](auto& f) { f = Codec<std::decay_t<decltype(f)>>::decode(r); });
    return obj;
  }
};

// ── Top-level encode/decode entry points ────────────────────────────────
template <typename T>
std::vector<std::byte> encode(const T& v) {
  static_assert(has_codec_v<T>,
                "shuttle: type is not cross-language capable (must be an aggregate of encodable "
                "fields; pure-C++ topics still work without a codec)");
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
// Aggregates get their source-level name ("my::Txn") from the codec; anything
// else falls back to the (mangled) typeid name. Cross-language topics must use
// a type whose tag matches the SV-side localparam type_tag.
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
