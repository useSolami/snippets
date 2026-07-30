import { builder, SubscriptionBuilder, CommitmentLevel } from "solami";
import { Connection, PublicKey } from "@solana/web3.js";
import bs58 from "bs58";

const PUMP_AMM = new PublicKey("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
const PUMP_PROGRAM = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const WSOL = "So11111111111111111111111111111111111111112";
const DEFAULT_TOKEN = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

const RAYDIUM_V4_SPEC = {
  name: "raydium_v4",
  program: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
  dataSize: 752,
  mintAOff: 400,
  mintBOff: 432,
  vaultAOff: 336,
  vaultBOff: 368,
};
const RAYDIUM_CPMM_SPEC = {
  name: "raydium_cpmm",
  program: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
  dataSize: null,
  mintAOff: 168,
  mintBOff: 200,
  vaultAOff: 72,
  vaultBOff: 104,
};
const PUMP_AMM_SPEC = {
  name: "pump_amm",
  mintAOff: 43,
  mintBOff: 75,
  vaultAOff: 139,
  vaultBOff: 171,
};

const grpcToken = process.env.GRPC_X_TOKEN;
const rpcToken = process.env.RPC_TOKEN;
const token = process.env.TOKEN_MINT || DEFAULT_TOKEN;
const minSpreadBps = Number(process.env.MIN_SPREAD_BPS ?? "50");
const minWsol = BigInt(Math.round(Number(process.env.MIN_WSOL_SOL ?? "1") * 1e9));
const minProfitSol = Number(process.env.MIN_PROFIT_SOL ?? "0");

let b = builder().withRpc(rpcToken).withGrpc(grpcToken);
if (process.env.GRPC_ENDPOINT) b = b.grpcUrl(process.env.GRPC_ENDPOINT);
const client = await b.build();
const connection = new Connection(client.rpc().url, "processed");
const tokenPk = new PublicKey(token);
const wsolPk = new PublicKey(WSOL);

console.log(`cross-dex arb detector starting token=${token} quote=${WSOL} min_spread_bps=${minSpreadBps} min_wsol_lamports=${minWsol}`);

const [dt, dq] = await fetchDecimals();
console.log(`decimals loaded token=${dt} wsol=${dq}`);

console.log("discovering pools (getProgramAccounts on solami RPC is ~15s; running venues concurrently)...");
const discovered = await Promise.all([
  discoverPump(),
  discoverRaydium(RAYDIUM_V4_SPEC),
  discoverRaydium(RAYDIUM_CPMM_SPEC),
]);
const candidates = discovered.flat();

const pools = await seedReserves(candidates);
if (!pools.length) {
  console.error("no valid pools discovered for this token/WSOL pair; nothing to compare");
  process.exit(1);
}

const vaultToLoc = new Map();
pools.forEach((p, i) => {
  vaultToLoc.set(p.tokenVault, { idx: i, side: "token" });
  vaultToLoc.set(p.wsolVault, { idx: i, side: "wsol" });
  console.log(
    `discovered pool venue=${p.venue} pool=${p.id} tokenVault=${p.tokenVault} ` +
      `wsolVault=${p.wsolVault} price=${price(p)}`,
  );
});
const liquid = pools.filter((p) => p.rq >= minWsol).length;
console.log(`pool discovery complete total_pools=${pools.length} liquid_pools=${liquid}`);
const initial = spread();
if (initial) {
  console.log(
    `initial cross-venue spread across liquid pools spread_bps=${initial.spreadBps} ` +
      `cheapest=${initial.buy.venue} cheapest_price=${initial.buyPrice} ` +
      `dearest=${initial.sell.venue} dearest_price=${initial.sellPrice}`,
  );
}

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.PROCESSED)
  .accounts("vaults", {
    account: [...vaultToLoc.keys()],
    owner: [],
    filters: [],
    nonemptyTxnSignature: undefined,
  })
  .build();

const stream = await client.grpc().subscribe(request);
let lastEmit = null;
console.log("subscribed, watching vault reserves...");

stream.on("data", (update) => {
  if (!update.account) return;
  const info = update.account.account;
  if (!info || info.data.length < 72) return;
  const pubkey = bs58.encode(info.pubkey);
  const loc = vaultToLoc.get(pubkey);
  if (!loc) return;
  const amount = readU64LE(info.data, 64);
  if (loc.side === "token") pools[loc.idx].rt = amount;
  else pools[loc.idx].rq = amount;
  recomputeAndEmit(Number(update.account.slot));
});
stream.on("error", (e) => {
  console.error(`stream error: ${e.message ?? e}`);
  process.exit(1);
});

function price(p) {
  if (p.rt === 0n || p.rq === 0n) return null;
  return Number(p.rq) / 10 ** dq / (Number(p.rt) / 10 ** dt);
}

function spread() {
  const priced = pools.filter((p) => p.rq >= minWsol).map((p) => ({ p, pr: price(p) })).filter((x) => x.pr != null);
  if (priced.length < 2) return null;
  let buy = priced[0];
  let sell = priced[0];
  for (const x of priced) {
    if (x.pr < buy.pr) buy = x;
    if (x.pr > sell.pr) sell = x;
  }
  if (buy.pr <= 0) return null;
  const spreadBps = Math.floor(((sell.pr - buy.pr) / buy.pr) * 10000);
  return { buy: buy.p, buyPrice: buy.pr, sell: sell.p, sellPrice: sell.pr, spreadBps };
}

function recomputeAndEmit(slot) {
  const s = spread();
  if (!s || s.spreadBps < minSpreadBps) return;
  const sim = simulateArb(s.buy, s.sell);
  if (sim.netSol <= minProfitSol) return;
  const key = `${s.buy.id}|${s.sell.id}|${Math.floor(s.spreadBps / 10)}`;
  if (lastEmit === key) return;
  lastEmit = key;
  console.log(
    `arbitrage edge (gross, after pool fees + price impact; EXCLUDES tx fee/priority/tip) ` +
      `slot=${slot} spread_bps=${s.spreadBps} ` +
      `buy_venue=${s.buy.venue} buy_pool=${s.buy.id} buy_price=${s.buyPrice.toFixed(9)} ` +
      `sell_venue=${s.sell.venue} sell_pool=${s.sell.id} sell_price=${s.sellPrice.toFixed(9)} ` +
      `optimal_in_sol=${sim.inSol.toFixed(6)} token_qty=${sim.tokenQty.toFixed(4)} ` +
      `gross_profit_lamports=${Math.round(sim.netSol * 1e9)} gross_profit_sol=${sim.netSol.toFixed(9)}`,
  );
}

function feeBps(venue) {
  return 25.0;
}

function simulateArb(buy, sell) {
  const fb = 1 - feeBps(buy.venue) / 10000;
  const fs = 1 - feeBps(sell.venue) / 10000;
  const rtB = Number(buy.rt);
  const rqB = Number(buy.rq);
  const rtS = Number(sell.rt);
  const rqS = Number(sell.rq);
  const net = (x) => {
    const xe = x * fb;
    const tok = (rtB * xe) / (rqB + xe);
    const te = tok * fs;
    const out = (rqS * te) / (rtS + te);
    return out - x;
  };
  let lo = 0;
  let hi = Math.min(rqB, rqS);
  for (let i = 0; i < 200; i++) {
    const m1 = lo + (hi - lo) / 3;
    const m2 = hi - (hi - lo) / 3;
    if (net(m1) < net(m2)) lo = m1;
    else hi = m2;
  }
  const x = (lo + hi) / 2;
  const xe = x * fb;
  const tok = (rtB * xe) / (rqB + xe);
  return { inSol: x / 1e9, tokenQty: tok / 10 ** dt, netSol: net(x) / 1e9 };
}

async function fetchDecimals() {
  const accounts = await connection.getMultipleAccountsInfo([tokenPk, wsolPk]);
  return [accounts[0].data[44], accounts[1].data[44]];
}

async function discoverPump() {
  const authority = PublicKey.findProgramAddressSync(
    [Buffer.from("pool-authority"), tokenPk.toBuffer()],
    PUMP_PROGRAM,
  )[0];
  const pool = PublicKey.findProgramAddressSync(
    [Buffer.from("pool"), Buffer.from([0, 0]), authority.toBuffer(), tokenPk.toBuffer(), wsolPk.toBuffer()],
    PUMP_AMM,
  )[0];
  const acc = await connection.getAccountInfo(pool);
  if (!acc) {
    console.log("no pump AMM pool for this token (expected unless it graduated from pump.fun)");
    return [];
  }
  const c = decodeCandidate(PUMP_AMM_SPEC, pool.toBase58(), acc.data);
  return c ? [c] : [];
}

async function discoverRaydium(spec) {
  const program = new PublicKey(spec.program);
  const [a, c] = await Promise.all([
    gpaIds(program, spec.dataSize, spec.mintAOff, spec.mintBOff),
    gpaIds(program, spec.dataSize, spec.mintBOff, spec.mintAOff),
  ]);
  const ids = [...new Set([...a, ...c])];
  if (!ids.length) return [];
  const accounts = await connection.getMultipleAccountsInfo(ids.map((s) => new PublicKey(s)));
  const out = [];
  ids.forEach((id, i) => {
    const acc = accounts[i];
    if (acc) {
      const cand = decodeCandidate(spec, id, acc.data);
      if (cand) out.push(cand);
    }
  });
  return out;
}

async function gpaIds(program, dataSize, tokenOff, wsolOff) {
  const filters = [
    { memcmp: { offset: tokenOff, bytes: token } },
    { memcmp: { offset: wsolOff, bytes: WSOL } },
  ];
  if (dataSize) filters.unshift({ dataSize });
  try {
    const res = await connection.getProgramAccounts(program, {
      commitment: "processed",
      dataSlice: { offset: 0, length: 0 },
      filters,
    });
    return res.map((r) => r.pubkey.toBase58());
  } catch (e) {
    console.warn(`getProgramAccounts failed: ${e.message ?? e}`);
    return [];
  }
}

function decodeCandidate(spec, id, data) {
  const mintA = readPk(data, spec.mintAOff);
  const mintB = readPk(data, spec.mintBOff);
  const vaultA = readPk(data, spec.vaultAOff);
  const vaultB = readPk(data, spec.vaultBOff);
  if (!mintA || !mintB || !vaultA || !vaultB) return null;
  let tokenVault;
  let wsolVault;
  if (mintA === token && mintB === WSOL) {
    [tokenVault, wsolVault] = [vaultA, vaultB];
  } else if (mintA === WSOL && mintB === token) {
    [tokenVault, wsolVault] = [vaultB, vaultA];
  } else {
    console.warn(`calibration skip: ${spec.name} ${id} is not token/WSOL`);
    return null;
  }
  return { venue: spec.name, id, tokenVault, wsolVault };
}

async function seedReserves(cands) {
  if (!cands.length) return [];
  const keys = [];
  for (const c of cands) {
    keys.push(new PublicKey(c.tokenVault));
    keys.push(new PublicKey(c.wsolVault));
  }
  const accounts = await connection.getMultipleAccountsInfo(keys);
  const out = [];
  cands.forEach((c, i) => {
    const tv = accounts[i * 2];
    const wv = accounts[i * 2 + 1];
    if (!tv || !wv) {
      console.warn(`calibration skip: ${c.venue} ${c.id} vault account missing`);
      return;
    }
    if (
      tv.owner.toBase58() !== TOKEN_PROGRAM ||
      wv.owner.toBase58() !== TOKEN_PROGRAM ||
      tv.data.length < 72 ||
      wv.data.length < 72
    ) {
      console.warn(`calibration skip: ${c.venue} ${c.id} vault not an SPL token account`);
      return;
    }
    out.push({
      venue: c.venue,
      id: c.id,
      tokenVault: c.tokenVault,
      wsolVault: c.wsolVault,
      rt: readU64LE(tv.data, 64),
      rq: readU64LE(wv.data, 64),
    });
  });
  return out;
}

function readPk(data, off) {
  return data.length >= off + 32 ? bs58.encode(data.subarray(off, off + 32)) : null;
}

function readU64LE(bytes, offset) {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getBigUint64(0, true);
}
