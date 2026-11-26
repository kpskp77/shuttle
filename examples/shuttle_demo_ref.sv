// ============================================================================
// shuttle_demo_ref.sv — objects crossing the broker by reference
//
// Compile order: weave_pkg.sv → shuttle_pkg.sv → this file.
// The C++ twin is examples/shuttle_demo_ref.cpp: it mirrors the encoded form with `struct
// RefId { uint32_t object; }` and forwards the frame untouched — a reference is data in the
// encoding, only the owning side resolves it. Both frames here declare the tag "RefId"
// explicitly: a cross-language peer passes the mirror's spelling (the SV default $typename would
// carry the declaring scope, e.g. "shuttle_demo_ref.Sample").
//
// What this module shows:
//   · a publisher, a mailbox and a callback entry over one object — the same trio the value path
//     has, with no id in sight;
//   · the object survives the round trip through C++ and resolves back to the *same* handle, with
//     its state (a queue the encoding could never carry) intact;
//   · `free(obj)` is the only lifetime verb, and an id nobody holds is a loud error, never a
//     silent alias;
//   · request/response carries a reference by declaring it as a field through `shuttle_ref_codec`:
//     the ordinary `shuttle_requester` / `shuttle_serve` pair sends an object next to its data, and
//     the responder reads that object — no reference flavour of either class exists.
// ============================================================================

module shuttle_demo_ref;
  timeunit 1ns; timeprecision 1ps;
  import shuttle_pkg::*;

  // The object: a plain class, no reflection, free to hold anything the encoding cannot.
  class Sample;
    int unsigned seq;
    int unsigned hist[$];
    function new(int unsigned seq_ = 0);
      seq = seq_;
    endfunction
  endclass

  // Callback form of the same delivery: invoke_ref_nb receives the resolved handle, inline.
  class SampleWatch implements shuttle_ref_sink_nb #(Sample);
    int    count = 0;
    Sample last;
    virtual function void invoke_ref_nb(input Sample value, input shuttle_meta meta);
      count++;
      last = value;
    endfunction
  endclass

  // The C++ interposer announces itself with a retained frame on "ref.demo.ready"; this sink only
  // counts frames — the payload is a reference this side never resolves (no object behind it).
  class ReadyWatch implements shuttle_sink_nb;
    int count = 0;
    virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
      count++;
    endfunction
  endclass

  // Request/response with a reference in the request. A reflected message may declare an object
  // field whose codec is shuttle_ref_codec (through a typedef, because `shuttle_field` takes a
  // codec name), so the request carries 4 encoded bytes and the responder sees the same handle.
  typedef shuttle_ref_codec #(Sample) SampleLink;

  class SampleAsk;
    Sample       obj;
    logic [31:0] mode;
    `shuttle_reflect_begin(SampleAsk)
      `shuttle_field(obj, SampleLink)
      `shuttle_field(mode, shuttle_u32)
    `shuttle_reflect_end
  endclass

  class SampleAnswer;
    logic [31:0] mode;
    int          seq;
    `shuttle_reflect_begin(SampleAnswer)
      `shuttle_field(mode, shuttle_u32)
      `shuttle_field(seq, shuttle_i32)
    `shuttle_reflect_end
  endclass

  // Reads the object the request carried: possible only because the field resolved through the
  // table to the sender's handle, not to a copy.
  class SampleEcho implements shuttle_responder #(SampleAsk, SampleAnswer);
    virtual function SampleAnswer respond(input SampleAsk req, input shuttle_meta meta);
      SampleAnswer r = new ();
      r.mode = req.mode;
      r.seq  = (req.obj != null) ? req.obj.seq + req.obj.hist.size() : -1;
      return r;
    endfunction
  endclass

  task automatic run();
    automatic shuttle_ref_publisher #(Sample)    pub = new ("ref.demo.req", "RefId");
    automatic shuttle_ref_mailbox #(Sample)      box = new ("ref.demo.resp", "RefId");
    automatic SampleWatch                        watch = new ();
    automatic ReadyWatch                         ready = new ();
    automatic shuttle_ref_subscription #(Sample) hit =
        shuttle_ref_subscription #(Sample)::nonblocking("ref.demo.req", watch, "RefId");
    automatic shuttle_subscription               hello = shuttle_subscribe_nb("ref.demo.ready",
                                                                              ready, "RefId");
    automatic Sample                             obj = new (3);
    automatic Sample                             got;
    automatic shuttle_meta                       meta;
    automatic shuttle_publication                h;
    automatic shuttle_ref_publisher #(Sample)    fin = new ("ref.demo.done", "RefId");

    // The echo topic must have a subscriber before the request goes out; the C++ retained frame
    // says it is there, and bounds the handshake so a broken demo fails instead of hanging.
    for (int i = 0; i < 5000 && ready.count == 0; i++) #(10us);
    if (ready.count == 0) $error("reference demo: the C++ interposer never announced itself");

    obj.hist.push_back(1);
    h = pub.publish_serial(obj);
    if (h != null) h.wait_done();

    for (int i = 0; i < 5000 && box.size() == 0; i++) #(10us);
    if (!box.try_recv(got, meta)) $error("reference demo: the echoed reference never came back");
    if (got != obj) $error("reference demo: the echo resolved to another object");
    if (got != null && got.hist.size() != 1) $error("reference demo: the object lost its state");
    if (watch.count != 1 || watch.last != obj)
      $error("reference demo: the callback entry saw another object");
    if (box.unresolved() != 0) $error("reference demo: unknown id after the round trip");

    // Request/response carrying a reference: the ordinary shuttle_requester / shuttle_serve pair
    // does the work, because the reference is just a field of the request message. The responder
    // is nonblocking, so the answer is already queued when send_now returns.
    begin
      automatic SampleEcho                                   echo_h = new ();
      automatic shuttle_serve #(SampleAsk, SampleAnswer)     svc    = shuttle_serve #(SampleAsk,
          SampleAnswer)::nonblocking("ref.demo.ask", "ref.demo.answer", echo_h);
      automatic shuttle_requester #(SampleAsk, SampleAnswer) rq     = new ("ref.demo.ask",
          "ref.demo.answer");
      automatic SampleAsk                 ask    = new ();
      automatic shuttle_call #(SampleAnswer) call;
      automatic SampleAnswer              answer;
      automatic bit                       ok;

      if (!svc.valid()) $error("reference demo: responder: %0s", svc.last_error());
      if (!rq.valid()) $error("reference demo: requester: %0s", rq.last_error());
      ask.obj  = obj;
      ask.mode = 32'd9;
      call     = rq.send_now(ask);
      if (call == null) $error("reference demo: the reference request was not published");
      call.recv(ok, answer, 1_000_000);  // 1us
      if (ok != 1 || answer == null || answer.mode != 32'd9)
        $error("reference demo: the reference request got no answer");
      if (answer.seq != int'(obj.seq + obj.hist.size()))
        $error("reference demo: the responder saw another object");
      svc.subscription().unsubscribe();
    end

    // Tell the C++ interposer the round trip is over, so it can return: whoever forwards a
    // reference must stay subscribed for as long as the request is in flight.
    fin.publish_now(obj);

    // Drop the binding: from here the object is unreachable by reference, which is the only
    // lifetime rule the user has to remember.
    pub.free(obj);
  endtask

endmodule
