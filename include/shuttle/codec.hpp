#pragma once

// 消息编解码：跨语言载荷的统一二进制格式与 C++ 侧序列化入口。
//
// 线上格式（与 SV streaming operators 的语义对齐，字段顺序一致）：
//   · 字段按声明顺序依次编码，无字段名、无填充、无对齐；
//   · 整数/浮点按大端写入（对应 SV 侧 {>>{...}} 流：字段声明序、每字段高位在前）；
//   · string / vector<T> 以 uint32 长度前缀（大端）+ 元素依次编码；
//   · enum 按其底层整数类型编码（SV 侧 enum 的 base type 位宽必须一致）；
//   · 嵌套 struct 递归展开。
//
// 注意：纯 C++ 的类型化投递不经过本层（零拷贝 shared_ptr<const T>）；
// 只有跨语言投递、字节订阅者（subscribe_bytes）或消息来自 SV 侧时才触发编解码。
//
// 用户类型通过 SHUTTLE_REFLECT(Type, field1, field2, ...) 获得编解码能力；
// 未注册的类型仍可在纯 C++ topic 上使用（零拷贝路径）。

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>

#include <shuttle/exception.hpp>

namespace shuttle {

// ── Writer：大端字节流写入 ───────────────────────────────────────────────
class Writer {
public:
  void u8(std::uint8_t v) { buf_.push_back(static_cast<std::byte>(v)); }
  void u16(std::uint16_t v) {
    u8(static_cast<std::uint8_t>(v >> 8));
    u8(static_cast<std::uint8_t>(v));
  }
  void u32(std::uint32_t v) {
    u16(static_cast<std::uint16_t>(v >> 16));
    u16(static_cast<std::uint16_t>(v));
  }
  void u64(std::uint64_t v) {
    u32(static_cast<std::uint32_t>(v >> 32));
    u32(static_cast<std::uint32_t>(v));
  }
  void raw(const void* p, std::size_t n) {  // 原样追加（string 字节）
    const auto* b = static_cast<const std::byte*>(p);
    buf_.insert(buf_.end(), b, b + n);
  }

  [[nodiscard]] const std::vector<std::byte>& buf() const { return buf_; }
  std::vector<std::byte> take() { return std::move(buf_); }

private:
  std::vector<std::byte> buf_;
};

// ── Reader：带边界检查的大端字节流读取 ───────────────────────────────────
// 任何越界读取抛出 DecodeError，不会读到未定义数据。
class Reader {
public:
  explicit Reader(const std::vector<std::byte>& buf) : p_(buf.data()), n_(buf.size()) {}
  Reader(const std::byte* p, std::size_t n) : p_(p), n_(n) {}

  std::uint8_t u8() { return static_cast<std::uint8_t>(take(1)[0]); }
  std::uint16_t u16() {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(u8()) << 8) | u8());
  }
  std::uint32_t u32() {
    return (static_cast<std::uint32_t>(u16()) << 16) | u16();
  }
  std::uint64_t u64() {
    return (static_cast<std::uint64_t>(u32()) << 32) | u32();
  }
  void raw(void* out, std::size_t n) {
    const std::byte* b = take(n);
    std::memcpy(out, b, n);
  }

  [[nodiscard]] bool eof() const { return pos_ == n_; }

private:
  const std::byte* take(std::size_t n) {
    if (n > n_ - pos_) throw DecodeError("shuttle: 解码越界（字节流长度不足）");
    const std::byte* r = p_ + pos_;
    pos_ += n;
    return r;
  }

  const std::byte* p_;
  std::size_t n_;
  std::size_t pos_ = 0;
};

// ── Codec<T>：类型的编解码能力 ───────────────────────────────────────────
// 主模板：available = false，表示该类型不可跨语言（纯 C++ topic 仍可用）。
// 各特化提供 available = true + encode/decode。
template <typename T, typename Enable = void>
struct Codec {
  static constexpr bool available = false;
};

template <typename T>
inline constexpr bool has_codec_v = Codec<T>::available;

// bool：u8（0/1）
template <>
struct Codec<bool> {
  static constexpr bool available = true;
  static void encode(Writer& w, const bool& v) { w.u8(v ? 1 : 0); }
  static bool decode(Reader& r) { return r.u8() != 0; }
};

// 整数（除 bool）：按位宽大端
template <typename T>
struct Codec<T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>>> {
  static constexpr bool available = true;
  using U = std::make_unsigned_t<T>;
  static void encode(Writer& w, const T& v) {
    const U u = static_cast<U>(v);
    if constexpr (sizeof(T) == 1) w.u8(static_cast<std::uint8_t>(u));
    else if constexpr (sizeof(T) == 2) w.u16(static_cast<std::uint16_t>(u));
    else if constexpr (sizeof(T) == 4) w.u32(static_cast<std::uint32_t>(u));
    else if constexpr (sizeof(T) == 8) w.u64(static_cast<std::uint64_t>(u));
    else static_assert(sizeof(T) <= 8, "shuttle: 不支持的整数位宽");
  }
  static T decode(Reader& r) {
    U u;
    if constexpr (sizeof(T) == 1) u = r.u8();
    else if constexpr (sizeof(T) == 2) u = r.u16();
    else if constexpr (sizeof(T) == 4) u = r.u32();
    else u = r.u64();
    return static_cast<T>(u);
  }
};

// 浮点：IEEE754 位模式大端
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

// std::string：u32 长度前缀 + UTF-8 字节
template <>
struct Codec<std::string> {
  static constexpr bool available = true;
  static void encode(Writer& w, const std::string& v) {
    if (v.size() > UINT32_MAX) throw PublishError("shuttle: string 超过 uint32 长度上限");
    w.u32(static_cast<std::uint32_t>(v.size()));
    w.raw(v.data(), v.size());
  }
  static std::string decode(Reader& r) {
    const std::uint32_t n = r.u32();
    std::string v(n, '\0');
    r.raw(v.data(), n);
    return v;
  }
};

// enum：按底层整数类型编码（SV 侧 base type 位宽必须一致）
template <typename T>
struct Codec<T, std::enable_if_t<std::is_enum_v<T>>> {
  static constexpr bool available = true;
  using U = std::underlying_type_t<T>;
  static void encode(Writer& w, const T& v) { Codec<U>::encode(w, static_cast<U>(v)); }
  static T decode(Reader& r) { return static_cast<T>(Codec<U>::decode(r)); }
};

// std::vector<T>：u32 个数前缀 + 元素依次编码
template <typename T, typename Alloc>
struct Codec<std::vector<T, Alloc>> {
  static constexpr bool available = Codec<T>::available;
  static void encode(Writer& w, const std::vector<T, Alloc>& v) {
    static_assert(Codec<T>::available, "shuttle: vector 元素类型不可编码");
    if (v.size() > UINT32_MAX) throw PublishError("shuttle: vector 超过 uint32 个数上限");
    w.u32(static_cast<std::uint32_t>(v.size()));
    for (const T& e : v) Codec<T>::encode(w, e);
  }
  static std::vector<T, Alloc> decode(Reader& r) {
    const std::uint32_t n = r.u32();
    std::vector<T, Alloc> v;
    v.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) v.push_back(Codec<T>::decode(r));
    return v;
  }
};

// std::array<T, N>：元素依次编码，无长度前缀
template <typename T, std::size_t N>
struct Codec<std::array<T, N>> {
  static constexpr bool available = Codec<T>::available;
  static void encode(Writer& w, const std::array<T, N>& v) {
    static_assert(Codec<T>::available, "shuttle: array 元素类型不可编码");
    for (const T& e : v) Codec<T>::encode(w, e);
  }
  static std::array<T, N> decode(Reader& r) {
    std::array<T, N> v{};
    for (auto& e : v) e = Codec<T>::decode(r);
    return v;
  }
};

// ── 顶层编解码入口 ───────────────────────────────────────────────────────
template <typename T>
std::vector<std::byte> encode(const T& v) {
  static_assert(has_codec_v<T>,
                "shuttle: 该类型未注册编解码能力，请使用 SHUTTLE_REFLECT 注册（纯 C++ topic 可忽略）");
  Writer w;
  Codec<T>::encode(w, v);
  return w.take();
}

template <typename T>
T decode(const std::vector<std::byte>& b) {
  static_assert(has_codec_v<T>, "shuttle: 该类型未注册编解码能力");
  Reader r(b);
  T v = Codec<T>::decode(r);
  if (!r.eof()) throw DecodeError("shuttle: 解码后字节流存在尾部多余字节");
  return v;
}

// ── 类型标签 ─────────────────────────────────────────────────────────────
// 优先级：Codec<T>::tag()（SHUTTLE_REFLECT 生成，取类型名字符串）> typeid 名。
// 跨语言 topic 必须使用带 tag() 的类型（SV 侧 localparam type_tag 与之相等）。
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

// ── SHUTTLE_REFLECT：为用户类型生成 Codec 特化 ───────────────────────────
// 用法（全局作用域）：
//   struct Txn { uint32_t addr; uint32_t data; std::string tag; };
//   SHUTTLE_REFLECT(Txn, addr, data, tag)
// 字段按声明顺序编码，最多 16 个；成员类型须属于编解码子集
// （整数/浮点/bool/string/enum/vector/array/已注册嵌套 struct）。

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

#define SHUTTLE_DETAIL_COUNT_(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, \
                              _16, N, ...)                                                    \
  N
#define SHUTTLE_DETAIL_COUNT(...)                                                              \
  SHUTTLE_DETAIL_COUNT_(__VA_ARGS__, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

#define SHUTTLE_DETAIL_FOR_EACH(m, t, ...) \
  SHUTTLE_DETAIL_CAT(SHUTTLE_DETAIL_FE, SHUTTLE_DETAIL_COUNT(__VA_ARGS__))(m, t, __VA_ARGS__)

#define SHUTTLE_DETAIL_ENC(t, f) \
  ::shuttle::Codec<std::decay_t<decltype(obj.f)>>::encode(w, obj.f);
#define SHUTTLE_DETAIL_DEC(t, f) \
  obj.f = ::shuttle::Codec<std::decay_t<decltype(obj.f)>>::decode(r);

#define SHUTTLE_REFLECT(Type, ...)                                          \
  namespace shuttle {                                                       \
  template <>                                                               \
  struct Codec<Type> {                                                      \
    static constexpr bool available = true;                                 \
    static const char* tag() { return #Type; }                              \
    static void encode(::shuttle::Writer& w, const Type& obj) {             \
      SHUTTLE_DETAIL_FOR_EACH(SHUTTLE_DETAIL_ENC, Type, __VA_ARGS__)        \
    }                                                                       \
    static Type decode(::shuttle::Reader& r) {                              \
      Type obj{};                                                           \
      SHUTTLE_DETAIL_FOR_EACH(SHUTTLE_DETAIL_DEC, Type, __VA_ARGS__)        \
      return obj;                                                           \
    }                                                                       \
  };                                                                        \
  }
