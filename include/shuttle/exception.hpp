#pragma once

// Exception hierarchy: follows the same cross-DSO discipline as weave — each exception type's
// destructor is declared in this header and defined in src/ (key function), so the vtable/typeinfo
// are emitted exactly once in libshuttle.so and a cross-DSO catch(const X&) matches correctly by
// type_info pointer comparison.

#include <stdexcept>

namespace shuttle {

// Base class of all framework exceptions
class ShuttleError : public std::runtime_error {
public:
  explicit ShuttleError(const char* what_arg) : std::runtime_error(what_arg) {}
  explicit ShuttleError(const std::string& what_arg) : std::runtime_error(what_arg) {}
  ~ShuttleError() override;  // key function: defined in src/exception.cpp
};

// Publish/delivery path failures: topic type-binding conflict, encoding failure, frame not
// materialized as a C++ object (type unbound), codec length limit exceeded
class PublishError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~PublishError() override;
};

// Capability mismatch: `publish_now` met a subscriber whose entry is blocking (may consume
// simulation time) (thrown under NowPolicy::reject)
class CapabilityError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~CapabilityError() override;
};

// Topic type-binding conflict: the same topic declared with two different type tags
class TypeMismatchError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~TypeMismatchError() override;
};

// Syntax error in a topic / subscription pattern: a wildcard appeared in a name used for publish,
// type declaration, or retained; or in a subscription pattern ">" is not the last level, an empty
// level appears, or a wildcard is embedded inside a plain level
class TopicPatternError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~TopicPatternError() override;
};

// Decoding failure: byte stream too short, trailing extra bytes, length prefix out of bounds, etc.
class DecodeError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~DecodeError() override;
};

}  // namespace shuttle
