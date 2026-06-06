import { builder, SubscriptionBuilder, CommitmentLevel } from "solami";
import bs58 from "bs58";

const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const OWNER_OFFSET = "32";

const token = process.env.GRPC_X_TOKEN;
const watchOwner = process.env.WATCH_OWNER;

let b = builder().withGrpc(token);
if (process.env.GRPC_ENDPOINT) {
  b = b.grpcUrl(process.env.GRPC_ENDPOINT);
}
const client = await b.build();
const grpc = client.grpc();
console.log(`connected to ${grpc.url}, subscribing to spl token accounts owned by ${watchOwner}`);

const request = new SubscriptionBuilder()
  .commitment(CommitmentLevel.CONFIRMED)
  .accounts("token_accounts", {
    account: [],
    owner: [TOKEN_PROGRAM, TOKEN_2022_PROGRAM],
    filters: [
      { tokenAccountState: true },
      { memcmp: { offset: OWNER_OFFSET, base58: watchOwner } },
    ],
    nonemptyTxnSignature: undefined,
  })
  .build();

const stream = await grpc.subscribe(request);
console.log("subscribed, watching for token account state changes...");

stream.on("data", (update) => {
  if (!update.account) return;
  const info = update.account.account;
  if (info) emit(Number(update.account.slot), info);
});
stream.on("error", (e) => {
  console.error(`stream error: ${e.message ?? e}`);
  process.exit(1);
});
stream.on("end", () => {
  console.log("stream closed");
  process.exit(0);
});

function emit(slot, info) {
  const pubkey = bs58.encode(info.pubkey);
  const data = info.data;
  if (data.length < 72) {
    console.warn(`account=${pubkey} data too short to decode (len=${data.length})`);
    return;
  }
  const mint = bs58.encode(data.subarray(0, 32));
  const owner = bs58.encode(data.subarray(32, 64));
  const amount = readU64LE(data, 64);

  console.log(
    `token account update slot=${slot} account=${pubkey} mint=${mint} owner=${owner} ` +
      `amount=${amount} lamports=${info.lamports} writeVersion=${info.writeVersion}`,
  );
}

function readU64LE(bytes, offset) {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getBigUint64(0, true);
}
