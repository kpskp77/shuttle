#pragma once

// shuttle — publish-subscribe framework for EDA verification environments, built on weave.
//
// Feature overview:
//   · Three publish flavors: `now` synchronous callback (zero suspension) /
//     `serial` background in-order callback / `concurrent` background
//     concurrent callback. `serial`/`concurrent` do not block the publisher
//     by default, provide a Publication handle to await completion, and
//     support deferred triggering (Trigger);
//   · Cross-language: C++ and SV (DPI) publish/subscribe on equal footing,
//     messages encoded in a unified binary format (codec.hpp);
//   · Entry model: each subscription declares one entry (blocking /
//     nonblocking), deciding whether the callback may run on the publisher's
//     call stack — nonblocking may, blocking only from background scheduling;
//     `serial`/`concurrent` deliver to both entries, `publish_now` only to
//     nonblocking;
//   · Hierarchical topic names + wildcard subscription + retained (latched);
//   · Mailbox (mailbox.hpp): a subscription with a queue — "wait for the next
//     frame" with an optional timeout, the building block for request/response;
//   · Requester/Call/serve (requester.hpp): request/response sugar on top of the
//     mailbox — correlation id in the payload, reply demultiplexing, timeout;
//   · Zero-copy delivery within C++; encoding/decoding happens only for
//     cross-language delivery, byte subscribers (subscribe_bytes), or frames
//     originating from SV.
//
// Backends: svdpi (full cross-language feature set) / systemc (automatically
// degrades to pure C++; API unchanged).
// The DPI protocol reference is docs/backends.md.

#include <shuttle/broker.hpp>
#include <shuttle/codec.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/mailbox.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/publisher.hpp>
#include <shuttle/requester.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
