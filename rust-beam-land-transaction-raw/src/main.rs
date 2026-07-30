use std::env;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, TransportConfig};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_tls_utils::{new_dummy_x509_certificate, tls_client_config_builder};
use tracing::{info, warn};

const RPC_BASE: &str = "https://rpc.solami.dev/sol";
const DEFAULT_BEAM_ENDPOINT: &str = "beam.solami.dev:11000";
const TIP_API_URL: &str = "https://api.solami.dev/onchain/tip-addresses";
const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const SOLAMI_SERVER: &str = "solami-beam";
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(25);
const MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const TIP_SOL: f64 = 0.0001;

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    beam_transaction().await;
}

async fn beam_transaction() {
    let rpc_token = env::var("RPC_TOKEN").expect("RPC_TOKEN is not set");
    let swqos_token = env::var("SWQOS_TOKEN").expect("SWQOS_TOKEN is not set");
    let keypair_bs58 = env::var("KEYPAIR").expect("KEYPAIR is not set");
    let endpoint = env::var("BEAM_ENDPOINT").unwrap_or_else(|_| DEFAULT_BEAM_ENDPOINT.to_owned());

    let payer = Keypair::from_base58_string(&keypair_bs58);
    let rpc = RpcClient::new(format!("{RPC_BASE}?api_key={rpc_token}"));

    let tip_accounts = fetch_tip_accounts().await;
    let tip_account = pick_tip(&tip_accounts);

    info!(payer = %payer.pubkey(), beam = %endpoint, "connecting...");
    let connection = connect_beam(&swqos_token, &endpoint).await;

    info!("connected, fetching recent blockhash...");
    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .expect("failed to fetch blockhash");

    let ixs = vec![
        solana_system_interface::instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1),
        solana_system_interface::instruction::transfer(
            &payer.pubkey(),
            &tip_account,
            (TIP_SOL * 1_000_000_000.0) as u64,
        ),
    ];

    let tx = Transaction::new_signed_with_payer(&ixs, Some(&payer.pubkey()), &[&payer], blockhash);
    let tx = VersionedTransaction::from(tx);

    let balance = rpc
        .get_balance(&payer.pubkey())
        .await
        .expect("failed to fetch balance");
    info!(
        payer = %payer.pubkey(),
        lamports = balance,
        sol = balance as f64 / 1_000_000_000.0,
        tip_account = %tip_account,
        tip_sol = TIP_SOL,
        "payer balance before sending"
    );

    info!("simulating transaction before beaming...");
    let sim = rpc
        .simulate_transaction(&tx)
        .await
        .expect("failed to simulate transaction")
        .value;
    if let Some(err) = sim.err {
        warn!(error = %err, logs = ?sim.logs, "simulation failed; aborting before beam");
        return;
    }
    info!(
        units_consumed = ?sim.units_consumed,
        logs = ?sim.logs,
        "simulation succeeded"
    );

    info!(payer = %payer.pubkey(), "beaming transaction...");
    let sig = send_transaction(&connection, &tx).await;
    info!(%sig, "beamed, waiting for confirmation...");

    for _ in 0..30 {
        match rpc
            .get_signature_status_with_commitment(&sig, CommitmentConfig::confirmed())
            .await
        {
            Ok(Some(Ok(()))) => {
                info!(%sig, "landed and confirmed");
                return;
            }
            Ok(Some(Err(e))) => {
                warn!(%sig, error = %e, "landed but transaction failed on-chain");
                return;
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "status check failed, retrying"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    warn!(%sig, "not confirmed within 30s; transaction may have been dropped or expired");
}

async fn fetch_tip_accounts() -> Vec<Pubkey> {
    let raw: Vec<String> = reqwest::get(TIP_API_URL)
        .await
        .expect("failed to fetch tip accounts")
        .json()
        .await
        .expect("failed to decode tip accounts");
    raw.iter()
        .map(|s| s.parse().expect("invalid tip account"))
        .collect()
}

fn pick_tip(accounts: &[Pubkey]) -> Pubkey {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .subsec_nanos() as usize;
    accounts[nanos % accounts.len()]
}

async fn connect_beam(swqos_key: &str, endpoint: &str) -> Connection {
    let keypair = Keypair::from_base58_string(swqos_key);
    let (cert, key) = new_dummy_x509_certificate(&keypair);

    let mut crypto = tls_client_config_builder()
        .with_client_auth_cert(vec![cert], key)
        .expect("failed to build client auth cert");
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let mut client_config = ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto).expect("failed to build quic crypto config"),
    ));
    let mut transport = TransportConfig::default();
    transport.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
    transport.max_idle_timeout(Some(
        IdleTimeout::try_from(MAX_IDLE_TIMEOUT).expect("invalid idle timeout"),
    ));
    client_config.transport_config(Arc::new(transport));

    let mut quic = Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("failed to bind udp socket");
    quic.set_default_client_config(client_config);

    let addr = endpoint
        .to_socket_addrs()
        .expect("failed to resolve beam endpoint")
        .next()
        .expect("beam endpoint resolved to no address");

    quic.connect(addr, SOLAMI_SERVER)
        .expect("failed to start quic handshake")
        .await
        .expect("quic handshake failed")
}

async fn send_transaction(connection: &Connection, tx: &VersionedTransaction) -> Signature {
    let payload = bincode::serialize(tx).expect("failed to serialize transaction");
    let mut stream = connection
        .open_uni()
        .await
        .expect("failed to open quic stream");
    stream
        .write_all(&payload)
        .await
        .expect("failed to write transaction");
    stream.finish().expect("failed to finish quic stream");
    stream.stopped().await.ok();
    tx.signatures[0]
}
