#pragma once

// Macro-free aggregate reflection for message codecs (C++17).
//
// Minimal reimplementation of the Boost.PFR idea, tuned to shuttle's needs:
// a message type is reflectable iff it is an aggregate whose every field is
// default-constructible and assignable (the same requirements the decode
// path already has: T obj{} + per-field assignment). Reference and const
// members are not supported (the arity probe cannot initialize them), and
// non-aggregate types (bases, private fields, unions, user constructors)
// simply stay non-reflectable — Codec<T>::available keeps returning false
// for them. Boost.PFR itself is the reference for the ubiq-arity trick.
//
// Field count: ubiq converts to any type, so the brace-initialization
// T{ubiq{}, ... (N times)} is well-formed exactly while N does not exceed
// the field count (aggregate init value-initializes any remaining fields,
// which is why default-constructibility is required). Taking the LARGEST
// well-formed N is exact: providing more initializers than fields is ill-
// formed, and mixed default member initializers cannot undercount because
// supplying extra probes stays valid. (Boost.PFR forbids NSDMI because its
// counting scheme differs; the largest-valid-brace-count used here is
// immune for default-constructible fields.)
//
// Field visitation uses structured bindings; one branch per arity up to 32
// fields (the arity_search bound below). Field order is declaration order —
// exactly the wire-format convention.
//
// type_name<T>() recovers the type's source-level name (e.g. "my::Txn")
// from __PRETTY_FUNCTION__ at compile time — the macro-free replacement
// for stringizing #Type. GCC and Clang are supported.

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace shuttle {
namespace detail {

// ── Field-count detection ───────────────────────────────────────────────
struct ubiq {
  template <typename Type>
  operator Type() const noexcept;  // intentionally undefined: SFINAE probe only
};

template <typename T, std::size_t... I>
auto init_probe(std::index_sequence<I...>)
    -> decltype(T{(static_cast<void>(I), ubiq{})...}, std::true_type{});
template <typename T, std::size_t... I>
std::false_type init_probe(...);

template <typename T, std::size_t N>
struct arity_ok : decltype(init_probe<T>(std::make_index_sequence<N>{})) {};

template <typename T, std::size_t N>
struct arity_search {
  // N is tried first; on failure the search continues at N-1. Both arms are
  // instantiated (template ternary), which bounds recursion depth at MAX.
  static constexpr std::size_t value =
      arity_ok<T, N>::value ? N : arity_search<T, N - 1>::value;
};
template <typename T>
struct arity_search<T, 0> : std::integral_constant<std::size_t, 0> {};

// Largest N in [0, MAX] for which T{ubiq{} ... N} is well-formed.
template <typename T, std::size_t MAX>
struct arity : std::integral_constant<std::size_t, arity_search<T, MAX>::value> {};

// ── Field visitation ────────────────────────────────────────────────────
template <typename T, typename F>
constexpr void for_each_field(T&& obj, F&& f) {
  using U = std::remove_reference_t<T>;
  static_assert(std::is_aggregate_v<U>,
                "shuttle: message types must be aggregates (no base classes, no user "
                "constructors, no private fields) to be codec-reflectable");
  constexpr std::size_t N = arity<U, 32>::value;
  static_assert(!arity_ok<U, 33>::value,
                "shuttle: message has more than 32 fields (raise the branch count in "
                "include/shuttle/reflect.hpp)");
  static_assert(N > 0 || std::is_empty_v<U>,
                "shuttle: could not reflect the message type (fields beyond the supported "
                "count, or a non-default-constructible/reference field without trailing "
                "default member initializers)");
  if constexpr (N == 0) {
    return;
  }
  if constexpr (N == 1) {
    auto&& [f0] = obj;
    f(f0);
  }
  else if constexpr (N == 2) {
    auto&& [f0, f1] = obj;
    f(f0);
    f(f1);
  }
  else if constexpr (N == 3) {
    auto&& [f0, f1, f2] = obj;
    f(f0);
    f(f1);
    f(f2);
  }
  else if constexpr (N == 4) {
    auto&& [f0, f1, f2, f3] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
  }
  else if constexpr (N == 5) {
    auto&& [f0, f1, f2, f3, f4] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
  }
  else if constexpr (N == 6) {
    auto&& [f0, f1, f2, f3, f4, f5] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
  }
  else if constexpr (N == 7) {
    auto&& [f0, f1, f2, f3, f4, f5, f6] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
  }
  else if constexpr (N == 8) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
  }
  else if constexpr (N == 9) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
  }
  else if constexpr (N == 10) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
  }
  else if constexpr (N == 11) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
  }
  else if constexpr (N == 12) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
  }
  else if constexpr (N == 13) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
  }
  else if constexpr (N == 14) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
  }
  else if constexpr (N == 15) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
  }
  else if constexpr (N == 16) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
  }
  else if constexpr (N == 17) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
  }
  else if constexpr (N == 18) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
  }
  else if constexpr (N == 19) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
  }
  else if constexpr (N == 20) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
  }
  else if constexpr (N == 21) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
  }
  else if constexpr (N == 22) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
  }
  else if constexpr (N == 23) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
  }
  else if constexpr (N == 24) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
  }
  else if constexpr (N == 25) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
  }
  else if constexpr (N == 26) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
  }
  else if constexpr (N == 27) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
  }
  else if constexpr (N == 28) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
    f(f27);
  }
  else if constexpr (N == 29) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
    f(f27);
    f(f28);
  }
  else if constexpr (N == 30) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
    f(f27);
    f(f28);
    f(f29);
  }
  else if constexpr (N == 31) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
    f(f27);
    f(f28);
    f(f29);
    f(f30);
  }
  else if constexpr (N == 32) {
    auto&& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30, f31] = obj;
    f(f0);
    f(f1);
    f(f2);
    f(f3);
    f(f4);
    f(f5);
    f(f6);
    f(f7);
    f(f8);
    f(f9);
    f(f10);
    f(f11);
    f(f12);
    f(f13);
    f(f14);
    f(f15);
    f(f16);
    f(f17);
    f(f18);
    f(f19);
    f(f20);
    f(f21);
    f(f22);
    f(f23);
    f(f24);
    f(f25);
    f(f26);
    f(f27);
    f(f28);
    f(f29);
    f(f30);
    f(f31);
  }
  else {
    static_assert(N <= 32, "shuttle: message has more than 32 fields");
  }
}

// ── Compile-time type names ─────────────────────────────────────────────
template <typename T>
constexpr std::string_view type_name_raw() {
#if defined(__clang__) || defined(__GNUC__)
  return __PRETTY_FUNCTION__;
#else
  static_assert(!std::is_same_v<T, T>, "shuttle: type_name needs __PRETTY_FUNCTION__ (GCC/Clang)");
#endif
}

// "… [with T = my::Txn; std::string_view = …]" (GCC) / "… [T = my::Txn]" (Clang).
template <typename T>
constexpr std::string_view type_name() {
  constexpr std::string_view raw = type_name_raw<T>();
  constexpr std::string_view key = "T = ";
  const std::size_t begin = raw.find(key);
  static_assert(begin != std::string_view::npos, "shuttle: cannot parse __PRETTY_FUNCTION__");
  const std::size_t start = begin + key.size();
  std::size_t end = start;
  while (end < raw.size() && raw[end] != ';' && raw[end] != ']') ++end;
  return raw.substr(start, end - start);
}

}  // namespace detail
}  // namespace shuttle
