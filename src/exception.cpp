// exception.cpp — key-function destructor definitions for the exception types
//
// The destructors in exception.hpp are all out-of-line declarations (the key
// function convention, same as weave: it guarantees catch-type matching is
// dispatched by the primary type across DSO boundaries). The definitions live
// here.
#include "shuttle/exception.hpp"

namespace shuttle {

ShuttleError::~ShuttleError() = default;
PublishError::~PublishError() = default;
CapabilityError::~CapabilityError() = default;
TypeMismatchError::~TypeMismatchError() = default;
TopicPatternError::~TopicPatternError() = default;
DecodeError::~DecodeError() = default;
MailboxClosed::~MailboxClosed() = default;
MailboxOverflowError::~MailboxOverflowError() = default;

}  // namespace shuttle
