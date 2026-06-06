import { builder, SubscriptionBuilder, CommitmentLevel, buildTipIx } from "solami";
import { OnlinePumpSdk, PumpSdk, getBuyTokenAmountFromSolAmount } from "@pump-fun/pump-sdk";
import {
  Connection,
  PublicKey,
  Keypair,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import BN from "bn.js";
import bs58 from "bs58";

const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const WSOL = new PublicKey("So11111111111111111111111111111111111111112");
const BUY_DISCRIMINATOR = [102, 6, 61, 18, 1, 218, 235, 234];
const TIP_SOL = 0.0001;
const pumpBytes = bs58.decode(PUMP_PROGRAM);

const grpcToken = process.env.GRPC_X_TOKEN;
const rpcToken = process.env.RPC_TOKEN;
const payer = Keypair.fromSecretKey(bs58.decode(process.env.KEYPAIR));
let currentTarget = process.env.TARGET || null;
const budgetLamports = new BN(Math.round(Number(process.env.BUY_SOL) * 1e9));
const slippagePct = Number(process.env.SLIPPAGE_BPS ?? "500") / 100;
const armed = process.env.ARM != null;
const sim = !["0", "false"].includes((process.env.SIM ?? "").toLowerCase());

let bld = builder().withRpc(rpcToken).withGrpc(grpcToken);
if (armed) bld = bld.withSwqos(process.env.SWQOS_TOKEN);
const client = await bld.build();

const connection = new Connection(client.rpc().url, "processed");
const online = new OnlinePumpSdk(connection);
const pump = new PumpSdk();
const global = await online.fetchGlobal();
const feeConfig = await online.fetchFeeConfig();

if (!currentTarget) {
  console.warn("no TARGET set; will copy the FIRST pump.fun buyer observed (set TARGET to pin one)");
}
console.log(
  `copy-trade bot starting (pump-sdk buy builder + solami beam) target=${currentTarget ?? "(first buyer seen)"} ` +
    `our_wallet=${payer.publicKey.toBase58()} budget_sol=${budgetLamports.toNumber() / 1e9} ` +
    `slippage_pct=${slippagePct} simulate=${sim} ` +
    `mode=${armed ? "ARMED (will beam)" : "DRY-RUN (build + log only)"}`,
);

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.PROCESSED)
  .transactions("target", {
    vote: false,
    failed: false,
    accountInclude: [currentTarget ?? PUMP_PROGRAM],
    accountExclude: [],
    accountRequired: [],
    signature: undefined,
  })
  .build();

const stream = await client.grpc().subscribe(request);
console.log("subscribed, watching target for pump.fun buys...");

stream.on("data", async (update) => {
  if (!update.transaction) return;
  const hit = detectTargetBuy(update.transaction);
  if (!hit) return;
  if (!currentTarget) {
    console.warn(`no TARGET set; latching onto first observed buyer ${hit.buyer} and copying it`);
    currentTarget = hit.buyer;
  }
  await handleBuy(hit);
});
stream.on("error", (e) => {
  console.error(`stream error: ${e.message ?? e}`);
  process.exit(1);
});

function detectTargetBuy(txUpdate) {
  const info = txUpdate.transaction;
  const message = info?.transaction?.message;
  const meta = info?.meta;
  if (!message || !meta) return null;

  const keys = [...message.accountKeys, ...meta.loadedWritableAddresses, ...meta.loadedReadonlyAddresses];
  const bal = meta.postTokenBalances.find((b) => b.mint && b.mint !== WSOL.toBase58());
  if (!bal || !keys.length) return null;
  const buyer = bs58.encode(keys[0]);
  const targetBytes = currentTarget ? new PublicKey(currentTarget).toBytes() : null;

  const ixs = [
    ...message.instructions,
    ...meta.innerInstructions.flatMap((g) => g.instructions),
  ];
  for (const ix of ixs) {
    const prog = keys[ix.programIdIndex];
    if (!prog || !bytesEqual(prog, pumpBytes)) continue;
    if (ix.data.length < 8 || !bytesEqual(ix.data.subarray(0, 8), BUY_DISCRIMINATOR)) continue;
    if (targetBytes && ![...ix.accounts].some((i) => bytesEqual(keys[i], targetBytes))) continue;
    return {
      mint: bal.mint,
      tokenProgram: bal.programId,
      srcSig: info.signature ? bs58.encode(info.signature) : "",
      buyer,
    };
  }
  return null;
}

async function handleBuy({ mint, tokenProgram, srcSig }) {
  const mintPk = new PublicKey(mint);
  const tokenProgPk = new PublicKey(tokenProgram);

  let state;
  try {
    state = await online.fetchBuyState(mintPk, payer.publicKey, tokenProgPk);
  } catch (e) {
    console.warn(`fetchBuyState failed for ${mint} (graduated to AMM?), skipping: ${e.message ?? e}`);
    return;
  }
  const { bondingCurveAccountInfo, bondingCurve, associatedUserAccountInfo } = state;

  const amount = getBuyTokenAmountFromSolAmount({
    global,
    feeConfig,
    mintSupply: bondingCurve.tokenTotalSupply ?? null,
    bondingCurve,
    amount: budgetLamports,
    quoteMint: WSOL,
  });

  const buyIxs = await pump.buyInstructions({
    global,
    bondingCurveAccountInfo,
    bondingCurve,
    associatedUserAccountInfo,
    mint: mintPk,
    user: payer.publicKey,
    amount,
    solAmount: budgetLamports,
    slippage: slippagePct,
    tokenProgram: tokenProgPk,
  });

  const instructions = [...buyIxs, buildTipIx(payer.publicKey, TIP_SOL)];

  console.log(
    `detected target buy -> quoted + built copy via pump-sdk src_sig=${srcSig} mint=${mint} ` +
      `token_program=${tokenProgram} tokens_out=${amount.toString()} ixs=${instructions.length}`,
  );

  const { blockhash } = await connection.getLatestBlockhash();
  const message = new TransactionMessage({
    payerKey: payer.publicKey,
    recentBlockhash: blockhash,
    instructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  tx.sign([payer]);

  let simOk = true;
  if (sim) {
    try {
      const { value } = await connection.simulateTransaction(tx, { sigVerify: false });
      if (value.err) {
        console.warn(`simulation failed: ${JSON.stringify(value.err)} (units=${value.unitsConsumed})`);
        simOk = false;
      } else {
        console.log(`simulation ok (units=${value.unitsConsumed})`);
      }
    } catch (e) {
      console.warn(`simulate call failed: ${e.message ?? e}`);
      simOk = false;
    }
  }

  if (!armed) {
    console.log(
      `DRY-RUN: built and signed copy buy (not sent). set ARM=1 + SWQOS_TOKEN to go live ` +
        `signature=${bs58.encode(tx.signatures[0])} tx_bytes=${tx.serialize().length}`,
    );
    return;
  }

  if (!simOk) {
    console.warn("skipping beam: simulation failed (set SIM=0 to bypass)");
    return;
  }

  try {
    const sig = await client.landTransaction(tx);
    console.log(`beamed copy buy ${sig}, confirming...`);
    await confirm(sig);
  } catch (e) {
    console.error(`beam failed: ${e.message ?? e}`);
  }
}

async function confirm(sig) {
  for (let i = 0; i < 30; i++) {
    const { value } = await connection.getSignatureStatuses([sig]);
    const status = value[0];
    if (status) {
      if (status.err) {
        console.error(`copy buy landed but failed on-chain: ${JSON.stringify(status.err)}`);
        return;
      }
      if (status.confirmationStatus === "confirmed" || status.confirmationStatus === "finalized") {
        console.log(`copy buy landed and confirmed in slot ${status.slot}`);
        return;
      }
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.error("copy buy not confirmed within 30s");
}

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}
