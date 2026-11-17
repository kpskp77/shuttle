// Mailbox demo: "wait for the next frame" instead of "let a callback handle each
// frame" — a subscription with a queue attached to it (see include/shuttle/mailbox.hpp).
//
// The subscription callback only copies the frame into the queue, so it runs on
// the nonblocking entry: `publish_now` fills a mailbox synchronously on the
// publisher's stack, and `serial`/`concurrent` fill it from their background
// units. Consuming the frames is then your own process line's business, which is
// what makes "collect N frames, then decide" possible at all.
//
// This is the C++ half of the demo; sv/shuttle_demo.sv shows the SV API
// (shuttle_mailbox, pure SV — no DPI entry).
//
// Expected output (values) is documented in examples/README.md.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

// ── Message type ─────────────────────────────────────────────────────────
// The payload field is called `data`, not `value`: MailboxItem exposes the payload
// as `.value`, so `item->value.data` stays readable.
struct Sample {
  std::uint32_t ch = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(Sample, ch, data)

WEAVE_TEST(mailbox_demo) {
  // ── A mailbox is an ordinary subscriber ────────────────────────────────
  // It joins the target set of every matching publication (so it shows up in
  // Publication::subscriber_count() and in topic subscriber counts) and honours
  // wildcards and retained replay like any other subscription. There is no
  // broker-side mailbox state and no third entry kind.
  auto box = shuttle::mailbox<Sample>("mb.demo");
  shuttle::Publisher<Sample> pub("mb.demo");
  for (std::uint32_t v = 1; v <= 3; ++v)
    pub.publish_now(Sample{0, v}, shuttle::NowPolicy::skip);
  std::printf("[mailbox] publish_now filled it synchronously: size=%zu received=%llu\n", box.size(),
              static_cast<unsigned long long>(box.received()));
  if (box.size() != 3) throw std::logic_error("mailbox_demo: publish_now must fill the queue inline");

  // ── The pull side: drain in your own process line ──────────────────────
  // recv(timeout) returns nullopt when nothing arrives in time and never loses a
  // frame (a late frame stays queued), so a drain loop is safe.
  std::size_t drained = 0;
  std::uint32_t sum = 0;
  while (auto item = box.recv(1_ms)) {
    ++drained;
    sum += item->value.data;
  }
  std::printf("[mailbox] drained %zu frame(s) (sum=%u), then recv(1ms) returned empty\n", drained,
              sum);
  if (drained != 3 || sum != 6) throw std::logic_error("mailbox_demo: lost frames while draining");

  // ── Mailbox and Publication answer different questions ─────────────────
  // Publication::wait() = "every subscriber's callback has finished";
  // Mailbox::recv()     = "a frame arrived". Request/response usually needs both.
  pub.publish_serial(Sample{0, 7}).wait();
  auto next = box.recv();  // already queued by the time wait() returned
  std::printf("[mailbox] recv() got data=%u seq=%llu\n", next.value.data,
              static_cast<unsigned long long>(next.meta.seq));
  if (next.value.data != 7) throw std::logic_error("mailbox_demo: recv() returned the wrong frame");

  // ── Bounded mailbox: the queue stops growing, the losses are counted ───
  // max_pending == 0 (the default) is unbounded. A bounded mailbox counts frames
  // that reached it and were then dropped in overflowed() — separate from
  // Subscription::dropped(), which counts frames that never reached it at all.
  auto capped = shuttle::mailbox<Sample>(
      "mb.cap", shuttle::SubOptions{2, shuttle::OverflowPolicy::drop_oldest});
  shuttle::Publisher<Sample> cap_pub("mb.cap");
  for (std::uint32_t v = 1; v <= 5; ++v) cap_pub.publish_now(Sample{0, v}, shuttle::NowPolicy::skip);
  auto kept_first = capped.try_recv();
  auto kept_second = capped.try_recv();
  // received() counts frames *accepted into the queue*, so under drop_oldest it
  // includes the ones that were later evicted (5 accepted, 3 evicted here);
  // overflowed() is the eviction count. Do not derive one from the other.
  std::printf("[capped] drop_oldest: capacity=2 received=%llu overflowed=%llu survivors=%u,%u\n",
              static_cast<unsigned long long>(capped.received()),
              static_cast<unsigned long long>(capped.overflowed()), kept_first->value.data,
              kept_second->value.data);
  if (capped.size() != 0 || capped.received() != 5 || capped.overflowed() != 3 ||
      kept_first->value.data != 4 || kept_second->value.data != 5)
    throw std::logic_error("mailbox_demo: drop_oldest accounting is wrong");

  // drop_newest instead refuses the incoming frame, so the two counters differ:
  // with the same capacity and the same burst, received() == 2 but overflowed() == 3,
  // and the survivors are the *oldest* two.
  auto newest = shuttle::mailbox<Sample>(
      "mb.cap2", shuttle::SubOptions{2, shuttle::OverflowPolicy::drop_newest});
  shuttle::Publisher<Sample> cap2_pub("mb.cap2");
  for (std::uint32_t v = 1; v <= 5; ++v) cap2_pub.publish_now(Sample{0, v}, shuttle::NowPolicy::skip);
  auto newest_first = newest.try_recv();
  auto newest_second = newest.try_recv();
  std::printf("[capped] drop_newest: capacity=2 received=%llu overflowed=%llu survivors=%u,%u\n",
              static_cast<unsigned long long>(newest.received()),
              static_cast<unsigned long long>(newest.overflowed()), newest_first->value.data,
              newest_second->value.data);
  if (newest.received() != 2 || newest.overflowed() != 3 || newest_first->value.data != 1 ||
      newest_second->value.data != 2)
    throw std::logic_error("mailbox_demo: drop_newest accounting is wrong");

  // ── OverflowPolicy::reject: the overflow becomes a normal delivery Failure ──
  // No new accounting channel: the frame did not reach this subscriber, so it is
  // reported like any other callback failure.
  auto strict = shuttle::mailbox<Sample>(
      "mb.reject", shuttle::SubOptions{1, shuttle::OverflowPolicy::reject});
  shuttle::Publisher<Sample> strict_pub("mb.reject");
  strict_pub.publish_serial(Sample{0, 1}).wait();  // fills the only slot
  auto rejected_pub = strict_pub.publish_serial(Sample{0, 2});
  rejected_pub.wait();
  for (const auto& f : rejected_pub.failures()) {
    std::string text = "<non-std exception>";
    try {
      if (f.error) std::rethrow_exception(f.error);
    } catch (const std::exception& e) {
      text = e.what();
    } catch (...) {
    }
    std::printf("[reject] subscriber %s -> %s\n", f.subscriber.c_str(), text.c_str());
  }
  if (rejected_pub.failures().size() != 1)
    throw std::logic_error("mailbox_demo: reject must surface as one delivery Failure");

  // ── OverflowPolicy::block cannot be honoured ───────────────────────────
  // The callback may run inside publish_now; blocking the deliverer there would
  // deadlock the publisher. Rejected at construction, not at delivery time.
  try {
    auto bad = shuttle::mailbox<Sample>(
        "mb.block", shuttle::SubOptions{1, shuttle::OverflowPolicy::block});
    (void)bad;
    throw std::logic_error("mailbox_demo: a bounded mailbox accepted OverflowPolicy::block");
  } catch (const std::invalid_argument& e) {
    std::printf("[block] rejected at construction: %s\n", e.what());
  }

  // ── Copies share one queue ─────────────────────────────────────────────
  auto alias = box;
  pub.publish_now(Sample{0, 9}, shuttle::NowPolicy::skip);
  auto taken = alias.try_recv();
  std::printf("[alias] a copy reads the same queue (data=%u, %zu left in the original)\n",
              taken->value.data, box.size());
  if (!taken || taken->value.data != 9 || box.size() != 0)
    throw std::logic_error("mailbox_demo: copies must share one queue");

  // ── close(): queued frames stay readable, then MailboxClosed ───────────
  pub.publish_now(Sample{0, 11}, shuttle::NowPolicy::skip);
  box.close();
  auto after_close = box.try_recv();  // still readable: close() stops new frames only
  std::string closed_text;
  try {
    (void)box.recv(1_ms);
  } catch (const shuttle::MailboxClosed& e) {
    closed_text = e.what();
  }
  std::printf("[close] drains to %u, then recv() throws: %s\n", after_close->value.data,
              closed_text.c_str());
  if (!after_close || closed_text.empty())
    throw std::logic_error("mailbox_demo: close() must keep queued frames readable");

  // ── Minimal request/response without a correlation id ──────────────────
  // The pattern from mailbox.hpp's header comment: one request topic, one reply
  // topic, one responder, one request outstanding at a time. This is enough for a
  // fixed pair of components — reach for shuttle::Requester (requester.hpp) once
  // several requests can be in flight, because then the replies need a rid to be
  // told apart.
  auto replies = shuttle::mailbox<Sample>("mb.resp");
  shuttle::Publisher<Sample> resp_pub("mb.resp");
  auto responder = shuttle::subscribe<Sample>("mb.req", [&](const Sample& s, const shuttle::Meta&) {
    resp_pub.publish_serial(Sample{s.ch, s.data * 2});
  });
  shuttle::Publisher<Sample> req_pub("mb.req");
  req_pub.publish_serial(Sample{3, 21}).wait();  // the responder's callback has returned
  auto reply = replies.recv(1_ms);
  if (!reply) throw std::logic_error("mailbox_demo: no reply arrived");
  std::printf("[rpc] request data=21 -> reply ch=%u data=%u\n", reply->value.ch, reply->value.data);
  if (reply->value.data != 42) throw std::logic_error("mailbox_demo: wrong reply");
}
