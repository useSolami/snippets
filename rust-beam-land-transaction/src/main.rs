use std::env;
use std::time::Duration;

use solami::{build_tip_ix, system_instruction, Keypair, Transaction, VersionedTransaction};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signer::Signer;
use tracing::{info, warn};

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

    let payer = Keypair::from_base58_string(&keypair_bs58);

    let mut builder = solami::builder()
        .with_rpc(&rpc_token)
        .with_swqos(&swqos_token);
    if let Ok(endpoint) = env::var("BEAM_ENDPOINT") {
        builder = builder.beam_endpoint(&endpoint);
    }

    info!(payer = %payer.pubkey(), "connecting...");
    let client = builder.build().await.expect("failed to connect");

    info!("connected, fetching recent blockhash...");
    let blockhash = client
        .get_latest_blockhash()
        .await
        .expect("failed to fetch blockhash");

    let ixs = vec![
        system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1),
        build_tip_ix(&payer.pubkey(), 0.0001),
    ];

    let tx = Transaction::new_signed_with_payer(
        &ixs,
        Some(&payer.pubkey()),
        &[&payer],
        blockhash,
    );
    let tx = VersionedTransaction::from(tx);

    info!(payer = %payer.pubkey(), "beaming transaction...");
    let sig = client.beam(&tx).await.expect("beam failed");
    info!(%sig, "beamed, waiting for confirmation...");

    for _ in 0..30 {
        match client
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
