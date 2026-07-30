import crypto from "crypto";
import { QUICClient } from "@matrixai/quic";
import * as x509 from "@peculiar/x509";
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  TransactionMessage,
  VersionedTransaction,
  LAMPORTS_PER_SOL,
} from "@solana/web3.js";
import bs58 from "bs58";

const RPC_BASE = "https://rpc.solami.dev/sol";
const DEFAULT_BEAM_ENDPOINT = "beam.solami.dev:11000";
const TIP_API_URL = "https://api.solami.dev/onchain/tip-addresses";
const ALPN_PROTOCOL = "solana-tpu";
const KEEP_ALIVE_INTERVAL = 25 * 1000;
const MAX_IDLE_TIMEOUT = 5 * 60 * 1000;
const FLUSH_DELAY_MS = 50;
const ED25519_PKCS8_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");
const TIP_SOL = 0.0001;

const rpcToken = process.env.RPC_TOKEN;
const swqosToken = process.env.SWQOS_TOKEN;
const keypairBs58 = process.env.KEYPAIR;
const endpoint = process.env.BEAM_ENDPOINT ?? DEFAULT_BEAM_ENDPOINT;

const payer = Keypair.fromSecretKey(bs58.decode(keypairBs58));
const connection = new Connection(`${RPC_BASE}?api_key=${rpcToken}`, "confirmed");

const tipAccounts = await fetchTipAccounts();
const tipAccount = tipAccounts[Math.floor(Math.random() * tipAccounts.length)];

console.log(`connecting to ${endpoint} as ${payer.publicKey.toBase58()}...`);
const quic = await connectBeam(swqosToken, endpoint);

console.log("connected, fetching recent blockhash...");
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
    SystemProgram.transfer({
      fromPubkey: payer.publicKey,
      toPubkey: tipAccount,
      lamports: Math.floor(TIP_SOL * LAMPORTS_PER_SOL),
    }),
  ],
}).compileToV0Message();

const tx = new VersionedTransaction(message);
tx.sign([payer]);

const balance = await connection.getBalance(payer.publicKey);
console.log(
  `payer balance before sending payer=${payer.publicKey.toBase58()} lamports=${balance} ` +
    `sol=${balance / LAMPORTS_PER_SOL} tip_account=${tipAccount.toBase58()} tip_sol=${TIP_SOL}`,
);

console.log("simulating transaction before beaming...");
const { value: sim } = await connection.simulateTransaction(tx, { sigVerify: false });
if (sim.err) {
  console.error(
    `simulation failed; aborting before beam error=${JSON.stringify(sim.err)} logs=${JSON.stringify(sim.logs)}`,
  );
  await quic.destroy();
  process.exit(1);
}
console.log(`simulation succeeded units_consumed=${sim.unitsConsumed}`);

console.log(`beaming transaction from ${payer.publicKey.toBase58()}...`);
const sig = await sendTransaction(quic, tx);
console.log(`beamed ${sig}, waiting for confirmation...`);

for (let i = 0; i < 30; i++) {
  const { value } = await connection.getSignatureStatuses([sig]);
  const status = value[0];
  if (status) {
    if (status.err) {
      console.error(`landed but transaction failed on-chain: ${JSON.stringify(status.err)}`);
      await quic.destroy();
      process.exit(1);
    }
    if (status.confirmationStatus === "confirmed" || status.confirmationStatus === "finalized") {
      console.log(`landed and confirmed in slot ${status.slot}`);
      await quic.destroy();
      process.exit(0);
    }
  }
  await new Promise((r) => setTimeout(r, 1000));
}

console.error("not confirmed within 30s; transaction may have been dropped or expired");
await quic.destroy();
process.exit(1);

async function fetchTipAccounts() {
  const res = await fetch(TIP_API_URL);
  if (!res.ok) throw new Error(`failed to fetch tip accounts: ${res.status}`);
  return (await res.json()).map((s) => new PublicKey(s));
}

async function selfSignedCert(seed) {
  x509.cryptoProvider.set(crypto.webcrypto);
  const privateKey = crypto.createPrivateKey({
    key: Buffer.concat([ED25519_PKCS8_PREFIX, Buffer.from(seed)]),
    format: "der",
    type: "pkcs8",
  });
  const publicKey = crypto.createPublicKey(privateKey);
  const alg = { name: "Ed25519" };
  const keys = {
    privateKey: await crypto.webcrypto.subtle.importKey(
      "pkcs8",
      privateKey.export({ type: "pkcs8", format: "der" }),
      alg,
      true,
      ["sign"],
    ),
    publicKey: await crypto.webcrypto.subtle.importKey(
      "spki",
      publicKey.export({ type: "spki", format: "der" }),
      alg,
      true,
      ["verify"],
    ),
  };
  const cert = await x509.X509CertificateGenerator.createSelfSigned({
    serialNumber: "01",
    name: "CN=solana-node",
    notBefore: new Date(),
    notAfter: new Date(Date.now() + 365 * 86400000),
    keys,
    signingAlgorithm: alg,
  });
  return {
    keyPem: privateKey.export({ type: "pkcs8", format: "pem" }),
    certPem: cert.toString("pem"),
  };
}

async function connectBeam(swqosKey, beamEndpoint) {
  const { keyPem, certPem } = await selfSignedCert(bs58.decode(swqosKey).slice(0, 32));
  const sep = beamEndpoint.lastIndexOf(":");
  if (sep < 0) throw new Error(`invalid endpoint: ${beamEndpoint}`);

  return QUICClient.createQUICClient({
    host: beamEndpoint.slice(0, sep),
    port: parseInt(beamEndpoint.slice(sep + 1), 10),
    crypto: {
      ops: {
        async randomBytes(data) {
          new Uint8Array(data).set(crypto.randomBytes(data.byteLength));
        },
      },
    },
    config: {
      key: keyPem,
      cert: certPem,
      verifyPeer: false,
      applicationProtos: [ALPN_PROTOCOL],
      maxIdleTimeout: MAX_IDLE_TIMEOUT,
      keepAliveIntervalTime: KEEP_ALIVE_INTERVAL,
    },
  });
}

async function sendTransaction(client, transaction) {
  const stream = client.connection.newStream("uni");
  const writer = stream.writable.getWriter();
  await writer.write(transaction.serialize());
  await writer.close();
  await new Promise((r) => setTimeout(r, FLUSH_DELAY_MS));
  return bs58.encode(transaction.signatures[0]);
}
