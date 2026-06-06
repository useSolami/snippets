import { builder, SubscriptionBuilder, CommitmentLevel } from "solami";
import bs58 from "bs58";

const RAYDIUM_AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SWAP_BASE_IN = 9;
const SWAP_BASE_OUT = 11;

const token = process.env.GRPC_X_TOKEN;

let b = builder().withGrpc(token);
if (process.env.GRPC_ENDPOINT) {
  b = b.grpcUrl(process.env.GRPC_ENDPOINT);
}
const client = await b.build();
const grpc = client.grpc();
console.log(`connected to ${grpc.url}, subscribing to raydium amm v4 transactions`);

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.PROCESSED)
  .transactions("raydium_txs", {
    vote: false,
    failed: false,
    accountInclude: [RAYDIUM_AMM_V4],
    accountExclude: [],
    accountRequired: [],
    signature: undefined,
  })
  .build();

const raydiumBytes = bs58.decode(RAYDIUM_AMM_V4);
const stream = await grpc.subscribe(request);
console.log("subscribed, watching for raydium swaps...");

stream.on("data", (update) => {
  if (!update.transaction) return;
  for (const trade of handleTx(update.transaction, raydiumBytes)) {
    emit(trade);
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

function emit(ev) {
  console.log(
    `raydium swap slot=${ev.slot} sig=${ev.sig} variant=${ev.variant} ` +
      `location=${ev.location} quotedIn=${ev.quotedIn} quotedOut=${ev.quotedOut} ` +
      `pool=${ev.pool} userSource=${ev.userSource} ` +
      `userDestination=${ev.userDestination} userOwner=${ev.userOwner}`,
  );
}

function handleTx(txUpdate, raydium) {
  const info = txUpdate.transaction;
  if (!info?.transaction?.message) return [];
  const message = info.transaction.message;
  const meta = info.meta;

  const keys = [...message.accountKeys];
  if (meta) {
    keys.push(...meta.loadedWritableAddresses, ...meta.loadedReadonlyAddresses);
  }

  const sig = info.signature ? bs58.encode(info.signature) : "";
  const slot = txUpdate.slot;
  const trades = [];

  message.instructions.forEach((ix, idx) => {
    const ev = tryDecodeSwap(keys, raydium, ix, sig, slot, idx, null);
    if (ev) trades.push(ev);
  });

  if (meta) {
    for (const inner of meta.innerInstructions) {
      inner.instructions.forEach((inst, ii) => {
        const ev = tryDecodeSwap(keys, raydium, inst, sig, slot, inner.index, ii);
        if (ev) trades.push(ev);
      });
    }
  }

  return trades;
}

function tryDecodeSwap(keys, raydium, ix, sig, slot, outerIx, innerIx) {
  const prog = keys[ix.programIdIndex];
  if (!prog || !bytesEqual(prog, raydium)) return null;

  const data = ix.data;
  if (data.length < 17) return null;

  let variant;
  if (data[0] === SWAP_BASE_IN) variant = "swap_base_in";
  else if (data[0] === SWAP_BASE_OUT) variant = "swap_base_out";
  else return null;

  const quotedIn = readU64LE(data, 1);
  const quotedOut = readU64LE(data, 9);

  const accounts = ix.accounts;
  const n = accounts.length;
  const resolve = (pos) => {
    if (pos < 0 || pos >= n) return null;
    const key = keys[accounts[pos]];
    return key ? bs58.encode(key) : null;
  };

  const location = innerIx === null ? `ix[${outerIx}]` : `inner[${outerIx}.${innerIx}]`;

  return {
    slot,
    sig,
    variant,
    location,
    quotedIn,
    quotedOut,
    pool: resolve(1),
    userSource: resolve(n - 3),
    userDestination: resolve(n - 2),
    userOwner: resolve(n - 1),
  };
}

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function readU64LE(bytes, offset) {
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 8);
  return view.getBigUint64(0, true);
}
