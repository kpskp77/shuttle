// Request/response demo: two components that never share a C++ object with each
// other — the device models register responders at construction time, the test
// driver (a separate component, created later) sends queries and collects the
// answers.
//
// This is the pattern to use when the responder side is decoupled from the
// requester side: the rendezvous is the framework's, not the application's —
//   · responder: serve<Req, Resp>(request_topic, reply_topic, handler) fills in
//     the correlation id (rid) from the request and publishes the reply;
//   · requester: Requester<Req, Resp>(request_topic, reply_topic) allocates the
//     rid, writes it into the request payload, and demultiplexes the replies
//     back to the call that waited for them.
//
// Same public headers as any user; built for both backends (systemc locally,
// svdpi inside the simulator).
//
// Expected output (values) is documented in examples/README.md.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

// ── Message types ────────────────────────────────────────────────────────
// The first field of both messages is the correlation id: RidField (the default
// traits of Requester/serve) reads and writes it, so the requester can pair each
// reply with its request without the responder knowing anything about it.
struct RegReq {
  std::uint32_t rid = 0;
  std::uint32_t addr = 0;
};
SHUTTLE_REFLECT(RegReq, rid, addr)

struct RegResp {
  std::uint32_t rid = 0;
  std::uint32_t status = 0;
  std::uint32_t value = 0;
};
SHUTTLE_REFLECT(RegResp, rid, status, value)

// ── Responder component: a register block ────────────────────────────────
// It only knows its own address window and its own topics. It does not know who
// asks, how many askers there are, or when. Declaring the subscription in the
// constructor is the usual shape: the component is wired up at init and stays
// registered for the whole simulation.
class RegisterBlock {
public:
  RegisterBlock(std::string request_topic, std::string reply_topic, std::uint32_t base)
      : request_topic_(std::move(request_topic)),
        reply_topic_(std::move(reply_topic)),
        base_(base),
        // serve() registers on the blocking entry (a responder usually consumes
        // simulation time) and echoes the request's rid into the reply.
        responder_(shuttle::serve<RegReq, RegResp>(
            request_topic_, reply_topic_, [this](const RegReq& req, const shuttle::Meta&) {
              RegResp resp;
              const bool hit = (req.addr >= base_ && req.addr < base_ + 0x100);
              resp.status = hit ? 0 : 1;  // 0 = decoded, 1 = outside this window
              resp.value = hit ? 0x1000 + (req.addr - base_) : 0;
              return resp;
            })) {}

  ~RegisterBlock() = default;  // the Subscription member unsubscribes

  [[nodiscard]] const std::string& request_topic() const { return request_topic_; }
  [[nodiscard]] const std::string& reply_topic() const { return reply_topic_; }

private:
  // Declaration order matters: `responder_` is destroyed first, so the callback
  // can never run after `this` is gone.
  std::string request_topic_;
  std::string reply_topic_;
  std::uint32_t base_;
  shuttle::Subscription responder_;
};

// ── Requester component: a test driver ───────────────────────────────────
// It never sees the responders: it publishes on the request topic and waits for
// replies on the reply topic.
class TestDriver {
public:
  TestDriver(std::string request_topic, std::string reply_topic)
      : client_(std::move(request_topic), std::move(reply_topic)) {}

  [[nodiscard]] shuttle::Requester<RegReq, RegResp>& client() { return client_; }

  // wait_all() uses the "every target replies once" convention, i.e. it waits
  // for publication().subscriber_count() replies. That is the responder count
  // only when nothing else subscribes to the request topic (see the observer
  // case below, and use wait_for(n) when the two differ).
  [[nodiscard]] std::vector<RegResp> query(std::uint32_t addr, weave::duration timeout) {
    auto call = client_.send_serial(RegReq{0, addr});
    auto replies = call.wait_all(timeout);
    std::printf("[driver] addr=0x%02x rid=%u -> %zu reply(ies), %zu target(s)\n", addr, call.rid(),
                replies.size(), call.publication().subscriber_count());
    for (const auto& r : replies)
      std::printf("[driver]   status=%u value=0x%05x (rid=%u)\n", r.status, r.value, r.rid);
    return replies;
  }

private:
  shuttle::Requester<RegReq, RegResp> client_;
};

WEAVE_TEST(request_response_demo) {
  // ── Two responder components, one topic pair ──────────────────────────
  // Each block covers a different address window; both are targets of every
  // request, so a query gets one reply per block.
  RegisterBlock low_block("reg.req", "reg.resp", 0x0000);
  RegisterBlock high_block("reg.req", "reg.resp", 0x0100);

  // The driver is created afterwards — it shares no object with the blocks.
  TestDriver driver("reg.req", "reg.resp");

  // ── One request, two replies ───────────────────────────────────────────
  auto replies = driver.query(0x0004, 1_ms);
  if (replies.size() != 2)
    throw std::logic_error("request_response_demo: expected one reply per responder");
  // The two blocks answered the same request with different verdicts; the rid is
  // identical in both replies (it is the request's, echoed by serve()).
  if (replies[0].status + replies[1].status != 1)
    throw std::logic_error("request_response_demo: exactly one block should decode the address");
  if (replies[0].rid != replies[1].rid)
    throw std::logic_error("request_response_demo: the replies must carry the request's rid");

  // ── Two requests in flight at once ─────────────────────────────────────
  // Nothing serialises the calls: send both, then wait for each. The reply
  // demultiplexing is what keeps them apart, so the answers stay paired with
  // their request even though they travel on the same reply topic.
  auto call_a = driver.client().send_serial(RegReq{0, 0x0006});
  auto call_b = driver.client().send_serial(RegReq{0, 0x0102});
  if (call_a.rid() == call_b.rid())
    throw std::logic_error("request_response_demo: each request needs its own rid");

  auto replies_b = call_b.wait_all(1_ms);
  auto replies_a = call_a.wait_all(1_ms);  // deliberately taken out of order
  if (replies_a.size() != 2 || replies_b.size() != 2)
    throw std::logic_error("request_response_demo: both calls should get both replies");
  // Call A asked for 0x0006 (low window): the low block answers 0x1006, the high
  // block reports "outside". Demultiplexing must not swap the two answers.
  std::uint32_t a_decoded = 0, b_decoded = 0;
  for (const auto& r : replies_a)
    if (r.status == 0) a_decoded = r.value;
  for (const auto& r : replies_b)
    if (r.status == 0) b_decoded = r.value;
  if (a_decoded != 0x1006 || b_decoded != 0x1002)
    throw std::logic_error("request_response_demo: replies were not paired with their request");
  std::printf("[driver] demux ok: A=0x%05x B=0x%05x\n", a_decoded, b_decoded);

  // ── A subscriber that only observes ────────────────────────────────────
  // It counts requests on the request topic and answers nothing. It is still a
  // target of the request, so subscriber_count() becomes 3 while only two
  // replies will ever arrive — wait_all() would burn its whole timeout waiting
  // for the observer. wait_for(<responders>) is the right tool here.
  std::size_t observed = 0;
  auto observer = shuttle::subscribe<RegReq>(
      "reg.req", [&](const RegReq&, const shuttle::Meta&) { ++observed; });
  auto observed_call = driver.client().send_serial(RegReq{0, 0x0007});
  auto observed_replies = observed_call.wait_for(2, 1_ms);
  observer.unsubscribe();
  std::printf("[driver] observer: %zu reply(ies), %zu target(s), observed=%zu\n",
              observed_replies.size(), observed_call.publication().subscriber_count(), observed);
  if (observed_replies.size() != 2 || observed != 1 ||
      observed_call.publication().subscriber_count() != 3)
    throw std::logic_error("request_response_demo: the observer must not eat replies");

  // ── Nobody answers: the timeout returns an empty result, no hang ───────
  shuttle::Requester<RegReq, RegResp> lonely("reg.orphan.req", "reg.orphan.resp");
  auto orphan = lonely.send_serial(RegReq{0, 0x42});
  auto nothing = orphan.wait_all(1_ms);
  std::printf("[driver] no responder: %zu reply(ies), subscribers=%zu\n", nothing.size(),
              orphan.publication().subscriber_count());
  if (!nothing.empty())
    throw std::logic_error("request_response_demo: an orphan request cannot receive replies");

  // ── A responder whose handler throws is reported, not swallowed ────────
  auto broken = shuttle::serve<RegReq, RegResp>(
      "reg.broken.req", "reg.broken.resp", [](const RegReq&, const shuttle::Meta&) -> RegResp {
        throw std::runtime_error("device is not responding");
      });
  shuttle::Requester<RegReq, RegResp> broken_client("reg.broken.req", "reg.broken.resp");
  auto broken_call = broken_client.send_serial(RegReq{0, 0x1});
  broken_call.publication().wait();
  auto broken_replies = broken_call.wait_all(1_ms);
  for (const auto& f : broken_call.publication().failures()) {
    std::string text = "<non-std exception>";
    try {
      if (f.error) std::rethrow_exception(f.error);
    } catch (const std::exception& e) {
      text = e.what();
    } catch (...) {
    }
    std::printf("[driver] responder %s failed: %s\n", f.subscriber.c_str(), text.c_str());
  }
  if (!broken_replies.empty() || broken_call.publication().failures().size() != 1)
    throw std::logic_error("request_response_demo: a throwing responder must surface as a failure");
  (void)broken;
}
