#pragma once

// Message types of the C++ half of the uart demo: the mirror of examples/shuttle_demo_msgs.sv.
//
// Every demo subject file that uses these uart message types (publish / nested / inherit /
// mailbox / requester / edge) includes this header, so the message types are declared once and
// cannot drift between the files; shuttle_demo_namespace.cpp declares its own namespaced type
// instead. Member types must belong to the codec subset (see include/shuttle/codec.hpp).

#include <cstdint>
#include <shuttle/shuttle.hpp>

// ── Message type ─────────────────────────────────────────────────────────
// User-defined struct; member types must belong to the codec subset (see
// include/shuttle/codec.hpp). SHUTTLE_REFLECT generates the serialization
// support (required for cross-language delivery) and the type tag "UartTx";
// the SV side must define a message class with the same fields in the same
// order and a matching type_tag (see examples/shuttle_demo.sv).
struct UartTx {
  std::uint32_t addr;
  std::uint32_t data;
};
SHUTTLE_REFLECT(UartTx, addr, data)

// Nested message type: SHUTTLE_REFLECT recurses per field, so Codec<UartBurst> delegates to
// Codec<UartTx> for the two transaction fields. The byte layout is the fields in declaration
// order with the nested structs expanded inline (no length prefix); the SV mirror is class
// UartBurst in examples/shuttle_demo.sv, whose `shuttle_field(first, UartTx)` entries make the
// nested class itself the field codec.
struct UartBurst {
  std::uint8_t count;
  UartTx first;
  UartTx last;
};
SHUTTLE_REFLECT(UartBurst, count, first, last)

// Inherited message types: SHUTTLE_REFLECT_DERIVED registers a derived struct by delegating its
// base subobject to Codec<Base>, so each level lists only its own fields and chains compose level
// by level. The byte layout is the base encoding followed by the own fields — identical to
// listing every field flat, but the inherited field names are written once, at the level that
// declares them. The SV mirror is class UartHeader/UartFrame/UartTagged in
// examples/shuttle_demo.sv, which uses `shuttle_base for the same delegation.
struct UartHeader {
  std::uint32_t addr;
};
struct UartFrame : UartHeader {
  std::uint32_t data;
};
struct UartTagged : UartFrame {
  std::uint8_t flags;
};
SHUTTLE_REFLECT(UartHeader, addr)
SHUTTLE_REFLECT_DERIVED(UartFrame, UartHeader, data)
SHUTTLE_REFLECT_DERIVED(UartTagged, UartFrame, flags)
