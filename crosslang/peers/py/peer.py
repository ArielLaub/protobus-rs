#!/usr/bin/env python
"""
The Python participant of the protobus cross-language suites (protobus-go and
protobus-cpp, which run this file unchanged).

    python peer.py server   serve the interop services; prints READY
    python peer.py client   run the client scenario against PEER_TARGET's
                            services; prints PASS/FAIL lines, then DONE

Runs on the protobus-py checkout on PYTHONPATH. The broker is
PROTOBUS_TEST_AMQP; the schema is crosslang/proto/interop.proto. Behaviour
mirrors crosslang/gopeer and peers/ts/peer.js exactly.
"""

import asyncio
import os
import signal
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from protobus import Context, EventListener, MessageService, RemoteError, RetryOptions, ServiceProxy
from protobus.errors import HandledError
from protobus.logger import LogLevel, set_level

AMQP = os.environ["PROTOBUS_TEST_AMQP"]
PROTO_DIR = Path(os.environ.get("PROTOBUS_TEST_PROTO_DIR", Path(__file__).resolve().parents[2] / "proto"))
TARGET = os.environ.get("PEER_TARGET", "py")
LANG = "py"
PROTO = (PROTO_DIR / "interop.proto").read_text()

set_level(LogLevel.Debug if os.environ.get("PROTOBUS_TEST_LOG") else LogLevel.Error)


def canonical():
    return {
        "amount": 10**30,
        "as_of": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "big": 9007199254740993,
        "tags": ["a", "b"],
        "counts": {"x": 1, "y": 2},
        "balances": {"k": 2**200},
        "parts": [1, 2, 3],
        "kind": "KIND_FUTURE",
        "inner": {"name": "root", "value": 7, "children": [{"name": "leaf", "value": 8, "children": []}]},
        "ubig": 18446744073709551615,
        "blob": bytes([0, 1, 255]),
        "ratio": 0.5,
        "flag": True,
        "neg": -5,
        "before_epoch": datetime(1969, 7, 20, 20, 17, 40, tzinfo=timezone.utc),
        "zero": 0,
    }


def service(name, methods, **options):
    attrs = {
        "service_name": name,
        "proto_file_name": "",
        "Proto": PROTO,
        **methods,
    }
    cls = type(f"S_{name.replace('.', '_')}", (MessageService,), attrs)
    return lambda ctx: cls(ctx, **options)


# ---- server ------------------------------------------------------------------

produced = {"yielded": 0, "stopped_early": False, "finished": False}


async def add(self, data, actor, correlation_id):
    return {"sum": (data.get("a") or 0) + (data.get("b") or 0)}


async def tick(self, data, actor, correlation_id, context=None):
    if data.get("emit_nothing"):
        return
    produced.update(yielded=0, stopped_early=False, finished=False)
    fail_at = data.get("fail_at") or 0
    for i in range(data.get("count") or 0):
        if fail_at and i >= fail_at:
            if data.get("unhandled"):
                raise RuntimeError("stream broke")
            raise HandledError(f"deliberate failure at chunk {i}", code="TEST_FAIL")
        if context is not None and context.signal.aborted:
            produced["stopped_early"] = True
            return
        yield {"seq": i, "payload": f"chunk-{i}"}
        produced["yielded"] += 1
        if data.get("delay_ms"):
            await asyncio.sleep(data["delay_ms"] / 1000)
            if context is not None and context.signal.aborted:
                produced["stopped_early"] = True
                return
    produced["finished"] = True


async def produced_(self, data, actor, correlation_id):
    return dict(produced)


async def whoami(self, data, actor, correlation_id, context):
    return {"actor": actor or "", "message_id": context.message_id, "routing_key": context.routing_key, "lang": LANG}


COUNTER = {"add": add, "tick": tick, "produced": produced_, "whoami": whoami}


async def serve():
    ctx = Context()
    await ctx.init(AMQP, proto_dirs=[])

    async def balance(self, data, actor, correlation_id):
        if data.get("account") == "boom":
            raise HandledError("no such account", code="NOT_FOUND")
        if data.get("account") == "crash":
            raise RuntimeError("kaboom")
        return canonical()

    async def echo(self, data, actor, correlation_id):
        return data

    async def fail(self, data, actor, correlation_id, context):
        await ctx.publish_event("interop.Attempted", {"lang": LANG, "message_id": context.message_id}, "EVENT.attempted")
        raise RuntimeError(f"flaky {LANG}")

    services = [
        service("interop.Counter", COUNTER)(ctx),
        service("interop.Counter.inst1", COUNTER)(ctx),
        service("interop.Wallet", {"balance": balance, "echo": echo}, retry=RetryOptions(max_retries=0))(ctx),
        service("interop.Flaky", {"fail": fail}, retry=RetryOptions(max_retries=3, retry_delay_ms=100), max_concurrent=4)(ctx),
    ]
    for s in services:
        await s.init()
    listener = service(f"interop.Listener.{LANG}", {}, retry=RetryOptions(max_retries=0))(ctx)
    await listener.init()

    async def on_ping(event, event_type, topic):
        await ctx.publish_event("interop.Ping", {"id": f"pong:{event['id']}", "n": event["n"] + 1, "from": LANG}, f"EVENT.pong.{LANG}")

    await listener.subscribe_event("interop.Ping", on_ping, f"EVENT.ping.{LANG}")
    print("READY", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await ctx.close()


# ---- client ------------------------------------------------------------------

failures = []


async def check(name, fn):
    try:
        await fn()
        print(f"PASS {name}", flush=True)
    except Exception:
        failures.append(name)
        print(f"FAIL {name}: " + traceback.format_exc().replace("\n", " | "), flush=True)


def eq(got, want, what):
    if got != want:
        raise AssertionError(f"{what}: got {got!r}, want {want!r}")


async def client():
    ctx = Context()
    await ctx.init(AMQP, proto_dirs=[str(PROTO_DIR)])
    counter = ServiceProxy(ctx, "interop.Counter")
    await counter.init()
    inst = ServiceProxy(ctx, "interop.Counter.inst1")
    await inst.init()
    wallet = ServiceProxy(ctx, "interop.Wallet")
    await wallet.init()

    async def unary():
        eq(await counter.add({"a": 2, "b": 3}), {"sum": 5}, "add")

    async def priority():
        eq(await counter.add({"a": 1, "b": 1}, priority=2), {"sum": 2}, "add")

    async def stream_in_order():
        got = [c async for c in counter.tick({"count": 5})]
        eq([c["seq"] for c in got], [0, 1, 2, 3, 4], "seq")
        eq([c["payload"] for c in got], [f"chunk-{i}" for i in range(5)], "payload")

    async def empty_stream():
        eq([c async for c in counter.tick({"emit_nothing": True})], [], "chunks")

    async def handled_mid_stream():
        got = []
        try:
            async for c in counter.tick({"count": 10, "fail_at": 2}):
                got.append(c)
            raise AssertionError("expected an error")
        except RemoteError as err:
            eq(err.code, "TEST_FAIL", "code")
            assert "deliberate failure at chunk 2" in err.message, err.message
        eq(len(got), 2, "chunks before the error")

    async def unhandled_mid_stream():
        try:
            async for _ in counter.tick({"count": 10, "fail_at": 1, "unhandled": True}):
                pass
            raise AssertionError("expected an error")
        except RemoteError as err:
            eq(err.message, "stream broke", "message")

    async def cancellation():
        n = 0
        async with counter.tick({"count": 500, "delay_ms": 10}) as stream:
            async for _ in stream:
                n += 1
                if n == 3:
                    break
        p = None
        for _ in range(100):
            p = await counter.produced({})
            if p["stopped_early"]:
                break
            await asyncio.sleep(0.05)
        assert p["stopped_early"] and not p["finished"] and p["yielded"] < 500, p

    async def custom_types():
        eq(await wallet.balance({"account": "acc"}), canonical(), "balance")

    async def echo_round_trip():
        eq(await wallet.echo(canonical()), canonical(), "echo")

    async def handled():
        try:
            await wallet.balance({"account": "boom"})
            raise AssertionError("expected an error")
        except RemoteError as err:
            eq((err.code, err.message), ("NOT_FOUND", "no such account"), "error")

    async def unhandled():
        try:
            await wallet.balance({"account": "crash"})
            raise AssertionError("expected an error")
        except RemoteError as err:
            eq(err.message, "kaboom", "message")

    async def unimplemented():
        try:
            await counter.unimplemented({})
            raise AssertionError("expected an error")
        except RemoteError as err:
            eq(err.code, "PROTOCOL_ERROR", "code")

    async def metadata():
        who = await counter.whoami({}, "client-py", message_id="mid-py-1")
        eq(who, {"actor": "client-py", "message_id": "mid-py-1", "routing_key": "REQUEST.interop.Counter.whoami", "lang": TARGET}, "who")

    async def instance():
        who = await inst.whoami({})
        eq(who["routing_key"], "REQUEST.interop.Counter.inst1.whoami", "routing key")

    async def events():
        listener = EventListener(ctx.connection, ctx.factory)
        await listener.init(None, "")
        got = asyncio.get_running_loop().create_future()

        async def on_pong(event, event_type, topic):
            if not got.done():
                got.set_result(event)

        await listener.subscribe("interop.Ping", on_pong, f"EVENT.pong.{TARGET}")
        await listener.start()
        await ctx.publish_event("interop.Ping", {"id": "py-1", "n": 2**70, "from": LANG}, f"EVENT.ping.{TARGET}")
        event = await asyncio.wait_for(got, 10)
        eq(event, {"id": "pong:py-1", "n": 2**70 + 1, "from": TARGET}, "pong")
        await listener.close()

    for name, fn in [
        ("unary", unary), ("priority on a plain queue", priority), ("stream in order", stream_in_order),
        ("empty stream", empty_stream), ("mid-stream handled error", handled_mid_stream),
        ("mid-stream unhandled error", unhandled_mid_stream), ("cancellation reaches the producer", cancellation),
        ("custom types, defaults and maps", custom_types), ("echo round trip", echo_round_trip),
        ("handled error", handled), ("unhandled error", unhandled), ("unimplemented method", unimplemented),
        ("call metadata", metadata), ("instance routing", instance), ("events both ways", events),
    ]:
        await check(name, fn)
    print("DONE", flush=True)
    await ctx.close()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    try:
        asyncio.run(serve() if sys.argv[1:2] == ["server"] else client())
    except KeyboardInterrupt:
        sys.exit(0)
