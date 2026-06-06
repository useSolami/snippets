import { builder, SubscriptionBuilder, CommitmentLevel } from "solami";
import bs58 from "bs58";

const PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const CREATE_DISCRIMINATOR = [24, 30, 200, 40, 5, 28, 7, 119];
const CREATE_V2_DISCRIMINATOR = [214, 144, 76, 236, 95, 139, 49, 180];

const PENDING_SLOT_GRACE = 32;
const BLOCK_TIME_CACHE_LIMIT = 4000;

const token = process.env.GRPC_X_TOKEN;

let b = builder().withGrpc(token);
if (process.env.GRPC_ENDPOINT) {
  b = b.grpcUrl(process.env.GRPC_ENDPOINT);
}
const client = await b.build();
const grpc = client.grpc();
console.log(`connected to ${grpc.url}, subscribing to pump.fun transactions + block meta`);

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.PROCESSED)
  .transactions("pump_txs", {
    vote: false,
    failed: false,
    accountInclude: [PUMPFUN_PROGRAM_ID],
    accountExclude: [],
    accountRequired: [],
    signature: undefined,
  })
  .blocksMeta("meta")
  .build();

const pumpBytes = bs58.decode(PUMPFUN_PROGRAM_ID);
const decoder = new TextDecoder("utf-8", { fatal: true });

const blockTimes = new Map();
const pending = new Map();
let latestSlot = 0;

const stream = await grpc.subscribe(request);
console.log("subscribed, watching for pump.fun create instructions...");

stream.on("data", (update) => {
  if (update.transaction) {
    const events = handleTx(update.transaction, pumpBytes);
    for (const ev of events) {
      latestSlot = Math.max(latestSlot, ev.slot);
      const bt = blockTimes.get(ev.slot);
      if (bt !== undefined) emit(ev, bt);
      else {
        if (!pending.has(ev.slot)) pending.set(ev.slot, []);
        pending.get(ev.slot).push(ev);
      }
    }
    flushStalePending();
  } else if (update.blockMeta) {
    const slot = Number(update.blockMeta.slot);
    latestSlot = Math.max(latestSlot, slot);
    if (update.blockMeta.blockTime) {
      const bt = Number(update.blockMeta.blockTime.timestamp);
      blockTimes.set(slot, bt);
      const evs = pending.get(slot);
      if (evs) {
        pending.delete(slot);
        for (const ev of evs) emit(ev, bt);
      }
      pruneBlockTimes();
    }
  }
});
stream.on("error", (e) => {
  console.error(`stream error: ${e.message ?? e}`);
  process.exit(1);
});
stream.on("end", () => {
  console.log("stream closed");
  process.exit(0);
});

function emit(ev, blockTime) {
  const now = Math.floor(Date.now() / 1000);
  const ageSecs = blockTime != null ? now - blockTime : null;
  const createdAt =
    blockTime != null ? new Date(blockTime * 1000).toISOString().replace(/\.\d{3}Z$/, "Z") : null;

  console.log(
    `pump.fun create slot=${ev.slot} sig=${ev.sig} variant=${ev.variant} ` +
      `location=${ev.location} name=${JSON.stringify(ev.name)} symbol=${JSON.stringify(ev.symbol)} ` +
      `uri=${ev.uri} mint=${ev.mint} bondingCurve=${ev.bondingCurve} ` +
      `associatedBondingCurve=${ev.associatedBondingCurve} user=${ev.user} creator=${ev.creator} ` +
      `isMayhemMode=${ev.isMayhemMode} isCashbackEnabled=${ev.isCashbackEnabled} ` +
      `blockTime=${blockTime} createdAt=${createdAt} ageSecs=${ageSecs}`,
  );
}

function flushStalePending() {
  for (const [slot, evs] of pending) {
    if (latestSlot - slot > PENDING_SLOT_GRACE) {
      pending.delete(slot);
      for (const ev of evs) {
        console.warn(`slot=${slot} block_time never arrived; emitting without timestamp`);
        emit(ev, null);
      }
    }
  }
}

function pruneBlockTimes() {
  if (blockTimes.size <= BLOCK_TIME_CACHE_LIMIT) return;
  const cutoff = latestSlot - 1500;
  for (const slot of blockTimes.keys()) {
    if (slot < cutoff) blockTimes.delete(slot);
  }
}

function handleTx(txUpdate, pump) {
  const info = txUpdate.transaction;
  if (!info?.transaction?.message) return [];
  const message = info.transaction.message;
  const meta = info.meta;

  const keys = [...message.accountKeys];
  if (meta) {
    keys.push(...meta.loadedWritableAddresses, ...meta.loadedReadonlyAddresses);
  }

  const sig = info.signature ? bs58.encode(info.signature) : "";
  const slot = Number(txUpdate.slot);
  const events = [];

  message.instructions.forEach((ix, idx) => {
    const ev = tryDecodeCreate(keys, pump, ix, sig, slot, idx, null);
    if (ev) events.push(ev);
  });

  if (meta) {
    for (const inner of meta.innerInstructions) {
      inner.instructions.forEach((inst, ii) => {
        const ev = tryDecodeCreate(keys, pump, inst, sig, slot, inner.index, ii);
        if (ev) events.push(ev);
      });
    }
  }

  return events;
}

function tryDecodeCreate(keys, pump, ix, sig, slot, outerIx, innerIx) {
  const prog = keys[ix.programIdIndex];
  if (!prog || !bytesEqual(prog, pump)) return null;

  const data = ix.data;
  if (data.length < 8) return null;
  const disc = data.subarray(0, 8);

  let variant;
  let userIdx;
  let parseV2Tail;
  if (bytesEqual(disc, CREATE_DISCRIMINATOR)) {
    variant = "create";
    userIdx = 7;
    parseV2Tail = false;
  } else if (bytesEqual(disc, CREATE_V2_DISCRIMINATOR)) {
    variant = "create_v2";
    userIdx = 5;
    parseV2Tail = true;
  } else {
    return null;
  }

  let off = 8;
  const name = readBorshString(data, off);
  if (!name) return null;
  off = name.next;
  const symbol = readBorshString(data, off);
  if (!symbol) return null;
  off = symbol.next;
  const uri = readBorshString(data, off);
  if (!uri) return null;
  off = uri.next;

  if (data.length - off < 32) return null;
  const creator = bs58.encode(data.subarray(off, off + 32));
  off += 32;

  let isMayhemMode = null;
  let isCashbackEnabled = null;
  if (parseV2Tail) {
    if (off < data.length) isMayhemMode = data[off] !== 0;
    if (off + 1 < data.length) isCashbackEnabled = data[off + 1] !== 0;
  }

  const accounts = ix.accounts;
  const resolve = (pos) => {
    if (pos < 0 || pos >= accounts.length) return null;
    const key = keys[accounts[pos]];
    return key ? bs58.encode(key) : null;
  };

  const location = innerIx === null ? `ix[${outerIx}]` : `inner[${outerIx}.${innerIx}]`;

  return {
    slot,
    sig,
    variant,
    location,
    name: name.value,
    symbol: symbol.value,
    uri: uri.value,
    mint: resolve(0),
    bondingCurve: resolve(2),
    associatedBondingCurve: resolve(3),
    user: resolve(userIdx),
    creator,
    isMayhemMode,
    isCashbackEnabled,
  };
}

function readBorshString(data, offset) {
  if (data.length - offset < 4) return null;
  const len = new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0, true);
  offset += 4;
  if (data.length - offset < len) return null;
  let value;
  try {
    value = decoder.decode(data.subarray(offset, offset + len));
  } catch {
    return null;
  }
  return { value, next: offset + len };
}

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
