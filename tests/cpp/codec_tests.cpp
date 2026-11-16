// 编解码语义自检：线上格式（大端、长度前缀、按声明序）与各类型往返。
//
// 黄金字节断言是跨语言契约的锚点：本文件里的期望字节与 docs/dpi_protocol.md 的
// "线上格式"一节、以及 SV 侧 examples/sv/shuttle_demo.sv（{>>{...}} 打包）必须逐字节
// 一致，任何一侧改动都会在这里先炸。

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <shuttle/codec.hpp>
#include <shuttle/detail/abi.hpp>
#include <shuttle/meta.hpp>
#include <weave/test.hpp>

#include "case_runner.hpp"

using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── 测试用消息类型（SHUTTLE_REFLECT 只能在全局作用域使用：它展开出的
//    `namespace shuttle { template <> struct Codec<T> }` 需要重开全局的 shuttle 命名空间）
struct UartTx {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(UartTx, addr, data)

struct Inner {
  std::uint16_t a = 0;
  std::int8_t   b = 0;
};
SHUTTLE_REFLECT(Inner, a, b)

enum class Mode : std::uint8_t { idle = 1, run = 2 };

// 覆盖编码子集：标量、枚举、string、vector、定宽数组、嵌套 struct
struct Wide {
  bool                          flag = false;
  std::uint8_t                  u8 = 0;
  std::int16_t                  i16 = 0;
  std::uint64_t                 u64 = 0;
  float                         f32 = 0.0f;
  double                        f64 = 0.0;
  Mode                          mode = Mode::idle;
  std::string                   name;
  std::vector<std::uint16_t>    vec;
  std::array<std::uint8_t, 3>   arr{};
  Inner                         nested;
};
SHUTTLE_REFLECT(Wide, flag, u8, i16, u64, f32, f64, mode, name, vec, arr, nested)

// 无法编码的类型（用于静态能力探测）
struct NotReflected {
  int x = 0;
};

namespace {

using bytes = std::vector<std::byte>;

// 按十六进制字面量构造期望字节流：{0x00,0x12,...}
bytes hex(std::initializer_list<int> v) {
  bytes out;
  out.reserve(v.size());
  for (int b : v) out.push_back(static_cast<std::byte>(static_cast<unsigned>(b)));
  return out;
}

bool same(const bytes& a, const bytes& b) { return a == b; }

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

WEAVE_TEST(codec_tests) {
  // case1：黄金向量 —— 与 SV {>>{32'h12, 32'h3c}} 逐字节对等
  run_case(__func__, "case1", [] {
    UartTx tx{0x12, 0x3C};
    expect_bytes(shuttle::encode(tx), hex({0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x3C}),
                 "UartTx 大端线序");
  });

  // case2：各宽度整数的大端写法（含负数的补码）
  run_case(__func__, "case2", [] {
    expect_bytes(shuttle::encode<std::uint16_t>(0xABCD), hex({0xAB, 0xCD}), "u16 大端");
    expect_bytes(shuttle::encode<std::int8_t>(-1), hex({0xFF}), "i8 补码");
    expect_bytes(shuttle::encode<std::int16_t>(-2), hex({0xFF, 0xFE}), "i16 补码大端");
    expect_bytes(shuttle::encode<std::uint32_t>(1), hex({0x00, 0x00, 0x00, 0x01}), "u32 大端");
    expect_bytes(shuttle::encode<bool>(true), hex({0x01}), "bool 单字节");
    expect_bytes(shuttle::encode<bool>(false), hex({0x00}), "bool false");
  });

  // case3：浮点按 IEEE754 大端
  run_case(__func__, "case3", [] {
    expect_bytes(shuttle::encode<float>(1.0f), hex({0x3F, 0x80, 0x00, 0x00}), "float 1.0");
    expect_bytes(shuttle::encode<double>(1.0), hex({0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
                 "double 1.0");
  });

  // case4：string / vector 的 uint32 长度前缀
  run_case(__func__, "case4", [] {
    expect_bytes(shuttle::encode<std::string>("hi"), hex({0x00, 0x00, 0x00, 0x02, 'h', 'i'}),
                 "string 长度前缀");
    expect_bytes(shuttle::encode<std::string>(""), hex({0x00, 0x00, 0x00, 0x00}), "空 string");
    expect_bytes(shuttle::encode(std::vector<std::uint32_t>{1, 2}),
                 hex({0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}),
                 "vector 个数前缀");
    expect_bytes(shuttle::encode(std::vector<std::uint8_t>{}), hex({0x00, 0x00, 0x00, 0x00}),
                 "空 vector");
    // 定宽数组无长度前缀
    expect_bytes(shuttle::encode(std::array<std::uint8_t, 3>{0x0A, 0x0B, 0x0C}),
                 hex({0x0A, 0x0B, 0x0C}), "array 无前缀");
  });

  // case5：enum 按底层类型编码（此处 1 字节）
  run_case(__func__, "case5", [] {
    expect_bytes(shuttle::encode(Mode::run), hex({0x02}), "enum 按 base type");
    expect_eq(static_cast<int>(shuttle::decode<Mode>(hex({0x01}))), 1, "enum 往返");
  });

  // case6：嵌套 struct 递归展开（字段序 = 声明序）
  run_case(__func__, "case6", [] {
    Inner in{0x0102, -3};
    expect_bytes(shuttle::encode(in), hex({0x01, 0x02, 0xFD}), "嵌套 struct 展平");
  });

  // case7：全类型往返（含嵌套）
  run_case(__func__, "case7", [] {
    Wide w;
    w.flag = true;
    w.u8 = 0x7F;
    w.i16 = -1234;
    w.u64 = 0x0123456789ABCDEFull;
    w.f32 = -2.5f;
    w.f64 = 3.141592653589793;
    w.mode = Mode::run;
    w.name = "uart-中文";  // UTF-8 多字节
    w.vec = {0x1111, 0x2222};
    w.arr = {1, 2, 3};
    w.nested = Inner{0x0405, -6};

    const bytes enc = shuttle::encode(w);
    const Wide back = shuttle::decode<Wide>(enc);
    expect(back.flag == w.flag, "bool 往返");
    expect_eq(back.u8, w.u8, "u8 往返");
    expect_eq(back.i16, w.i16, "i16 往返");
    expect(back.u64 == w.u64, "u64 往返");
    expect(back.f32 == w.f32, "float 往返");
    expect(back.f64 == w.f64, "double 往返");
    expect(back.mode == w.mode, "enum 往返");
    expect(back.name == w.name, "string 往返");
    expect(back.vec == w.vec, "vector 往返");
    expect(back.arr == w.arr, "array 往返");
    expect(back.nested.a == w.nested.a && back.nested.b == w.nested.b, "嵌套 struct 往返");
    const std::size_t wanted = 1 /*flag*/ + 1 /*u8*/ + 2 /*i16*/ + 8 /*u64*/ + 4 /*f32*/ +
                               8 /*f64*/ + 1 /*mode*/ + (4 + w.name.size()) /*string*/ +
                               (4 + w.vec.size() * 2) /*vector*/ + 3 /*array*/ +
                               (2 + 1) /*nested*/;
    expect_eq(static_cast<long long>(enc.size()), static_cast<long long>(wanted),
              "编码长度与字段宽度之和一致");
  });

  // case8：Reader 越界必须抛 DecodeError（不得读出越界内存）
  run_case(__func__, "case8", [] {
    bool threw = false;
    try {
      (void)shuttle::decode<std::uint32_t>(hex({0x00, 0x01}));
    } catch (const shuttle::DecodeError&) {
      threw = true;
    }
    expect(threw, "短字节流应抛 DecodeError");

    threw = false;
    try {
      (void)shuttle::decode<std::string>(hex({0x00, 0x00, 0x00, 0x05, 'a'}));  // 声明 5 实际 1
    } catch (const shuttle::DecodeError&) {
      threw = true;
    }
    expect(threw, "长度前缀超界应抛 DecodeError");
  });

  // case9：类型标签与能力探测
  run_case(__func__, "case9", [] {
    expect(shuttle::type_tag_of<UartTx>() == "UartTx", "类型标签取自 SHUTTLE_REFLECT 名");
    expect(shuttle::type_tag_of<Wide>() == "Wide", "第二个类型的标签");
    expect(shuttle::has_codec_v<std::uint32_t>, "标量有 codec");
    expect(shuttle::has_codec_v<std::string>, "string 有 codec");
    expect(!shuttle::has_codec_v<NotReflected>, "未反射类型无 codec");
    expect(!shuttle::has_codec_v<int*>, "指针无 codec");
    // 未反射类型不提供 encode/decode 函数指针（跨语言投递会被拒绝）
    const shuttle::detail::TypeOps* ops = shuttle::detail::type_ops<NotReflected>();
    expect(ops->encode == nullptr && ops->decode == nullptr, "无 codec 时 encode/decode 为空");
  });

  report(__func__);
}
