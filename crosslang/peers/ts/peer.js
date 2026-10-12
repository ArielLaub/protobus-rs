#!/usr/bin/env node
/**
 * The TypeScript participant of the protobus cross-language suites
 * (protobus-go and protobus-cpp, which run this file unchanged).
 *
 *   node peer.js server   serve the interop services; prints READY
 *   node peer.js client   run the client scenario against PEER_TARGET's
 *                         services; prints PASS/FAIL lines, then DONE
 *
 * Runs on the BUILT TypeScript protobus checkout at PROTOBUS_TS. The broker
 * is PROTOBUS_TEST_AMQP; the schema is crosslang/proto/interop.proto.
 * Behaviour mirrors crosslang/gopeer and peers/py/peer.py exactly.
 */
'use strict';
const path = require('path');
const fs = require('fs');

const TS = process.env.PROTOBUS_TS || path.resolve(__dirname, '../../../../protobus');
const AMQP = process.env.PROTOBUS_TEST_AMQP;
const PROTO_DIR = process.env.PROTOBUS_TEST_PROTO_DIR || path.resolve(__dirname, '../../proto');
const TARGET = process.env.PEER_TARGET || 'ts';
const LANG = 'ts';

const lib = (p) => require(path.join(TS, 'dist/lib', p));
const Context = lib('context').default;
const MessageService = lib('message_service').default;
const ServiceProxy = lib('service_proxy').default;
const EventListener = lib('event_listener').default;
const { HandledError } = lib('errors');
const { setLevel, LogLevel } = lib('logger');
setLevel(process.env.PROTOBUS_TEST_LOG ? LogLevel.Debug : LogLevel.Error);

const PROTO = fs.readFileSync(path.join(PROTO_DIR, 'interop.proto')).toString();

// Enums are given by NAME, as decoding returns them. Before TypeScript
// protobus 2.5.0 a name was encoded as 0 (protobus#40), which is why this
// peer once sent numbers.
function canonical() {
    return {
        amount: 10n ** 30n,
        as_of: new Date(Date.UTC(2020, 0, 1)),
        big: '9007199254740993',
        tags: ['a', 'b'],
        counts: { x: 1, y: 2 },
        balances: { k: 2n ** 200n },
        parts: [1n, 2n, 3n],
        kind: 'KIND_FUTURE',
        inner: { name: 'root', value: 7n, children: [{ name: 'leaf', value: 8n, children: [] }] },
        ubig: '18446744073709551615',
        blob: Buffer.from([0, 1, 255]),
        ratio: 0.5,
        flag: true,
        neg: -5,
        before_epoch: new Date(Date.UTC(1969, 6, 20, 20, 17, 40)),
        zero: 0,
    };
}

// ---- server ------------------------------------------------------------------

const produced = { yielded: 0, stopped_early: false, finished: false };

function service(name, methods, options) {
    return class extends MessageService {
        constructor(ctx) { super(ctx, options || {}); Object.assign(this, methods); }
        get ServiceName() { return name; }
        get ProtoFileName() { return ''; }
        get Proto() { return PROTO; }
    };
}

const counterMethods = {
    async add(req) { return { sum: (req.a || 0) + (req.b || 0) }; },
    async *tick(req, _actor, _id, context) {
        if (req.emit_nothing) return;
        produced.yielded = 0; produced.stopped_early = false; produced.finished = false;
        for (let i = 0; i < (req.count || 0); i++) {
            if (req.fail_at && i >= req.fail_at) {
                if (req.unhandled) throw new Error('stream broke');
                throw new HandledError(`deliberate failure at chunk ${i}`, 'TEST_FAIL');
            }
            if (context && context.signal && context.signal.aborted) { produced.stopped_early = true; return; }
            yield { seq: i, payload: `chunk-${i}` };
            produced.yielded += 1;
            if (req.delay_ms) {
                await new Promise((r) => setTimeout(r, req.delay_ms));
                if (context && context.signal && context.signal.aborted) { produced.stopped_early = true; return; }
            }
        }
        produced.finished = true;
    },
    async produced() { return { ...produced }; },
    async whoami(_req, actor, _id, context) {
        return { actor: actor || '', message_id: context.messageId, routing_key: context.routingKey, lang: LANG };
    },
};

async function serve() {
    const ctx = new Context();
    await ctx.init(AMQP, []);
    const Counter = service('interop.Counter', counterMethods);
    const CounterInst = service('interop.Counter.inst1', counterMethods);
    const Wallet = service('interop.Wallet', {
        async balance(req) {
            if (req.account === 'boom') throw new HandledError('no such account', 'NOT_FOUND');
            if (req.account === 'crash') throw new Error('kaboom');
            return canonical();
        },
        // Returned exactly as decoded: re-encoding a decoded message must not
        // change it, enums included.
        async echo(req) { return req; },
    }, { retry: { maxRetries: 0 } });
    const Flaky = service('interop.Flaky', {
        async fail(_req, _actor, _id, context) {
            await ctx.publishEvent('interop.Attempted', { lang: LANG, message_id: context.messageId }, 'EVENT.attempted');
            throw new Error(`flaky ${LANG}`);
        },
    }, { retry: { maxRetries: 3, retryDelayMs: 100 }, maxConcurrent: 4 });
    const Listener = service(`interop.Listener.${LANG}`, {}, { retry: { maxRetries: 0 } });

    for (const S of [Counter, CounterInst, Wallet, Flaky]) {
        await new S(ctx).init();
    }
    const listener = new Listener(ctx);
    await listener.init();
    await listener.subscribeEvent('interop.Ping', async (event) => {
        await ctx.publishEvent('interop.Ping', { id: `pong:${event.id}`, n: event.n + 1n, from: LANG }, `EVENT.pong.${LANG}`);
    }, `EVENT.ping.${LANG}`);

    process.stdout.write('READY\n');
    const stop = () => { ctx.connection.disconnect().finally(() => process.exit(0)); };
    process.on('SIGTERM', stop);
    process.on('SIGINT', stop);
}

// ---- client ------------------------------------------------------------------

const results = [];
async function check(name, fn) {
    try {
        await fn();
        process.stdout.write(`PASS ${name}\n`);
    } catch (err) {
        process.stdout.write(`FAIL ${name}: ${(err && (err.stack || err.message)) || err}`.split('\n').join(' | ') + '\n');
        results.push(name);
    }
}

function assert(cond, msg) { if (!cond) throw new Error(msg); }
// Order-insensitive for object keys: protobuf does not order map entries.
function canon(x) {
    if (typeof x === 'bigint') return `${x}n`;
    if (Buffer.isBuffer(x)) return `buf:${[...x].join(',')}`;
    if (x instanceof Date) return `date:${x.toISOString()}`;
    if (Array.isArray(x)) return x.map(canon);
    if (x && typeof x === 'object') {
        const out = {};
        for (const k of Object.keys(x).sort()) out[k] = canon(x[k]);
        return out;
    }
    return x;
}
function eq(a, b, what) {
    const norm = (v) => JSON.stringify(canon(v));
    const ja = norm(a); const jb = norm(b);
    assert(ja === jb, `${what}: got ${ja}, want ${jb}`);
}
async function expectError(promise, check) {
    try { await promise; } catch (err) { check(err); return; }
    throw new Error('expected an error');
}

async function client() {
    const ctx = new Context();
    await ctx.init(AMQP, []);
    ctx.factory.parse(PROTO, 'interop.Counter');
    const counter = new ServiceProxy(ctx, 'interop.Counter'); await counter.init();
    const inst = new ServiceProxy(ctx, 'interop.Counter.inst1'); await inst.init();
    const wallet = new ServiceProxy(ctx, 'interop.Wallet'); await wallet.init();

    await check('unary', async () => eq(await counter.add({ a: 2, b: 3 }), { sum: 5 }, 'add'));
    await check('priority on a plain queue', async () => {
        eq(await counter.add({ a: 1, b: 1 }, undefined, true, undefined, { priority: 2 }), { sum: 2 }, 'add');
    });
    await check('stream in order', async () => {
        const got = [];
        for await (const c of counter.tick({ count: 5 })) got.push(c);
        eq(got.map((c) => c.seq), [0, 1, 2, 3, 4], 'seq');
        eq(got.map((c) => c.payload), ['chunk-0', 'chunk-1', 'chunk-2', 'chunk-3', 'chunk-4'], 'payload');
    });
    await check('empty stream', async () => {
        const got = [];
        for await (const c of counter.tick({ emit_nothing: true })) got.push(c);
        eq(got.length, 0, 'chunks');
    });
    await check('mid-stream handled error', async () => {
        const got = [];
        await expectError((async () => { for await (const c of counter.tick({ count: 10, fail_at: 2 })) got.push(c); })(), (err) => {
            eq(err.code, 'TEST_FAIL', 'code');
            assert(/deliberate failure at chunk 2/.test(err.message), err.message);
        });
        eq(got.length, 2, 'chunks before the error');
    });
    await check('mid-stream unhandled error', async () => {
        await expectError((async () => { for await (const _c of counter.tick({ count: 10, fail_at: 1, unhandled: true })); })(), (err) => {
            eq(err.message, 'stream broke', 'message');
        });
    });
    await check('cancellation reaches the producer', async () => {
        let n = 0;
        for await (const _c of counter.tick({ count: 500, delay_ms: 10 })) { if (++n === 3) break; }
        let p;
        for (let i = 0; i < 100; i++) {
            p = await counter.produced({});
            if (p.stopped_early) break;
            await new Promise((r) => setTimeout(r, 50));
        }
        assert(p.stopped_early === true && p.finished === false && p.yielded < 500, `produced ${JSON.stringify(p)}`);
    });
    await check('custom types, defaults and maps', async () => eq(await wallet.balance({ account: 'acc' }), canonical(), 'balance'));
    await check('echo round trip', async () => eq(await wallet.echo(canonical()), canonical(), 'echo'));
    await check('handled error', async () => {
        await expectError(wallet.balance({ account: 'boom' }), (err) => {
            eq(err.code, 'NOT_FOUND', 'code'); eq(err.message, 'no such account', 'message');
        });
    });
    await check('unhandled error', async () => {
        await expectError(wallet.balance({ account: 'crash' }), (err) => eq(err.message, 'kaboom', 'message'));
    });
    await check('unimplemented method', async () => {
        await expectError(counter.unimplemented({}), (err) => eq(err.code, 'PROTOCOL_ERROR', 'code'));
    });
    await check('call metadata', async () => {
        const who = await counter.whoami({}, 'client-ts', true, undefined, { messageId: 'mid-ts-1' });
        eq(who, { actor: 'client-ts', message_id: 'mid-ts-1', routing_key: 'REQUEST.interop.Counter.whoami', lang: TARGET }, 'who');
    });
    await check('instance routing', async () => {
        const who = await inst.whoami({});
        eq(who.routing_key, 'REQUEST.interop.Counter.inst1.whoami', 'routing key');
    });
    await check('events both ways', async () => {
        const listener = new EventListener(ctx.connection, ctx.factory);
        await listener.init(undefined, '');
        const got = new Promise((resolve) => {
            listener.subscribe('interop.Ping', async (event) => resolve(event), `EVENT.pong.${TARGET}`);
        });
        await listener.start();
        await ctx.publishEvent('interop.Ping', { id: 'ts-1', n: 2n ** 70n, from: LANG }, `EVENT.ping.${TARGET}`);
        const event = await Promise.race([got, new Promise((_r, rej) => setTimeout(() => rej(new Error('no pong')), 10000))]);
        eq(event, { id: 'pong:ts-1', n: 2n ** 70n + 1n, from: TARGET }, 'pong');
        await listener.close();
    });

    process.stdout.write('DONE\n');
    await ctx.connection.disconnect();
    process.exit(results.length ? 1 : 0);
}

const mode = process.argv[2];
(mode === 'server' ? serve() : client()).catch((err) => {
    process.stderr.write(`${err && err.stack || err}\n`);
    process.exit(2);
});
