import { builder, SubscriptionBuilder, CommitmentLevel } from "solami";
import bs58 from "bs58";

const SYSTEM_PROGRAM = "11111111111111111111111111111111";
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const SYSTEM_TRANSFER = [2, 0, 0, 0];
const TOKEN_TRANSFER = 3;
const TOKEN_TRANSFER_CHECKED = 12;

const token = process.env.GRPC_X_TOKEN;
const watchAccount = process.env.WATCH_ACCOUNT;

let b = builder().withGrpc(token);
if (process.env.GRPC_ENDPOINT) {
  b = b.grpcUrl(process.env.GRPC_ENDPOINT);
}
const client = await b.build();
const grpc = client.grpc();
console.log(`connected to ${grpc.url}, watching transfers for ${watchAccount}`);

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.CONFIRMED)
  .transactions("account_txs", {
    vote: false,
    failed: false,
    accountInclude: [watchAccount],
    accountExclude: [],
    accountRequired: [],
    signature: undefined,
  })
  .build();

const systemBytes = bs58.decode(SYSTEM_PROGRAM);
const tokenBytes = bs58.decode(TOKEN_PROGRAM);
const token2022Bytes = bs58.decode(TOKEN_2022_PROGRAM);

const stream = await grpc.subscribe(request);
console.log("subscribed, watching for sol + spl token transfers...");

stream.on("data", (update) => {
  if (!update.transaction) return;
  for (const t of handleTx(update.transaction)) emit(t);
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
    `transfer slot=${ev.slot} sig=${ev.sig} kind=${ev.kind} location=${ev.location} ` +
      `direction=${ev.direction} source=${ev.source} destination=${ev.destination} ` +
      `authority=${ev.authority} mint=${ev.mint} amount=${ev.amount} ` +
      `decimals=${ev.decimals} uiAmount=${ev.uiAmount}`,
  );
}

function handleTx(txUpdate) {
  const info = txUpdate.transaction;
  if (!info?.transaction?.message) return [];
  const message = info.transaction.message;
  const meta = info.meta;

  const keys = [...message.accountKeys];
  if (meta) {
    keys.push(...meta.loadedWritableAddresses, ...meta.loadedReadonlyAddresses);
  }

  const tokenAccounts = new Map();
  if (meta) {
    for (const bal of [...meta.preTokenBalances, ...meta.postTokenBalances]) {
      if (!tokenAccounts.has(bal.accountIndex)) {
        tokenAccounts.set(bal.accountIndex, {
          owner: bal.owner,
          mint: bal.mint,
          decimals: bal.uiTokenAmount?.decimals ?? 0,
        });
      }
    }
  }

  const sig = info.signature ? bs58.encode(info.signature) : "";
  const slot = Number(txUpdate.slot);
  const transfers = [];

  const decode = (ix, outer, inner) => {
    const prog = keys[ix.programIdIndex];
    if (!prog) return;
    let ev = null;
    if (bytesEqual(prog, systemBytes)) {
      ev = tryDecodeSol(keys, ix, sig, slot, outer, inner);
    } else if (bytesEqual(prog, tokenBytes) || bytesEqual(prog, token2022Bytes)) {
      ev = tryDecodeSpl(keys, tokenAccounts, ix, sig, slot, outer, inner);
    }
    if (ev) transfers.push(ev);
  };

  message.instructions.forEach((ix, idx) => decode(ix, idx, null));
  if (meta) {
    for (const inner of meta.innerInstructions) {
      inner.instructions.forEach((inst, ii) => decode(inst, inner.index, ii));
    }
  }

  return transfers;
}

function tryDecodeSol(keys, ix, sig, slot, outer, inner) {
  const data = ix.data;
  if (data.length < 12) return null;
  for (let i = 0; i < 4; i++) if (data[i] !== SYSTEM_TRANSFER[i]) return null;

  const lamports = readU64LE(data, 4);
  const from = resolve(keys, ix.accounts, 0);
  const to = resolve(keys, ix.accounts, 1);

  let direction;
  if (from === watchAccount) direction = "out";
  else if (to === watchAccount) direction = "in";
  else return null;

  return {
    slot,
    sig,
    kind: "sol",
    location: location(outer, inner),
    direction,
    source: from,
    destination: to,
    authority: null,
    mint: null,
    amount: lamports,
    decimals: 9,
    uiAmount: Number(lamports) / 1_000_000_000,
  };
}

function tryDecodeSpl(keys, tokenAccounts, ix, sig, slot, outer, inner) {
  const data = ix.data;
  if (data.length === 0) return null;

  let amount;
  let ixDecimals = null;
  let srcPos;
  let dstPos;
  let authPos;
  let mintPos = null;
  if (data[0] === TOKEN_TRANSFER && data.length >= 9) {
    amount = readU64LE(data, 1);
    [srcPos, dstPos, authPos] = [0, 1, 2];
  } else if (data[0] === TOKEN_TRANSFER_CHECKED && data.length >= 10) {
    amount = readU64LE(data, 1);
    ixDecimals = data[9];
    [srcPos, mintPos, dstPos, authPos] = [0, 1, 2, 3];
  } else {
    return null;
  }

  const accounts = ix.accounts;
  if (srcPos >= accounts.length || dstPos >= accounts.length) return null;
  const src = tokenAccounts.get(accounts[srcPos]);
  const dst = tokenAccounts.get(accounts[dstPos]);

  let direction;
  if (src?.owner === watchAccount) direction = "out";
  else if (dst?.owner === watchAccount) direction = "in";
  else return null;

  const info = src ?? dst;
  const mint = info?.mint ?? (mintPos !== null ? resolve(keys, accounts, mintPos) : null);
  const decimals = info?.decimals ?? ixDecimals;
  const uiAmount = decimals != null ? Number(amount) / 10 ** decimals : null;

  return {
    slot,
    sig,
    kind: "spl",
    location: location(outer, inner),
    direction,
    source: resolve(keys, accounts, srcPos),
    destination: resolve(keys, accounts, dstPos),
    authority: resolve(keys, accounts, authPos),
    mint,
    amount,
    decimals,
    uiAmount,
  };
}

function resolve(keys, accounts, pos) {
  if (pos < 0 || pos >= accounts.length) return null;
  const key = keys[accounts[pos]];
  return key ? bs58.encode(key) : null;
}

function location(outer, inner) {
  return inner === null ? `ix[${outer}]` : `inner[${outer}.${inner}]`;
}

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function readU64LE(bytes, offset) {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getBigUint64(0, true);
}
