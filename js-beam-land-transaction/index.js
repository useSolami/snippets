import { builder, buildTipIx } from "solami";
import {
  Keypair,
  SystemProgram,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import bs58 from "bs58";

const rpcToken = process.env.RPC_TOKEN;
const swqosToken = process.env.SWQOS_TOKEN;
const keypairBs58 = process.env.KEYPAIR;

const payer = Keypair.fromSecretKey(bs58.decode(keypairBs58));

let b = builder().withRpc(rpcToken).withSwqos(swqosToken);
if (process.env.BEAM_ENDPOINT) {
  b = b.landingEndpoint(process.env.BEAM_ENDPOINT);
}
const client = await b.build();

const connection = client.rpc().connection;
const { blockhash } = await connection.getLatestBlockhash();

const message = new TransactionMessage({
  payerKey: payer.publicKey,
  recentBlockhash: blockhash,
  instructions: [
    SystemProgram.transfer({
      fromPubkey: payer.publicKey,
      toPubkey: payer.publicKey,
      lamports: 1,
    }),
    buildTipIx(payer.publicKey, 0.0001),
  ],
}).compileToV0Message();

const tx = new VersionedTransaction(message);
tx.sign([payer]);

console.log(`beaming transaction from ${payer.publicKey.toBase58()}...`);
const sig = await client.landTransaction(tx);
console.log(`beamed ${sig}, waiting for confirmation...`);

for (let i = 0; i < 30; i++) {
  const { value } = await connection.getSignatureStatuses([sig]);
  const status = value[0];
  if (status) {
    if (status.err) {
      console.error(
        `landed but transaction failed on-chain: ${JSON.stringify(status.err)}`,
      );
      process.exit(1);
    }
    if (
      status.confirmationStatus === "confirmed" ||
      status.confirmationStatus === "finalized"
    ) {
      console.log(`landed and confirmed in slot ${status.slot}`);
      process.exit(0);
    }
  }
  await new Promise((r) => setTimeout(r, 1000));
}

console.error(
  "not confirmed within 30s; transaction may have been dropped or expired",
);
process.exit(1);
