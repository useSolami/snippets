use std::env;
use std::sync::Arc;

use futures::StreamExt;
use pump_rust_client::{constants, AsyncPumpClient, PumpSdk};
use solami::{
    build_tip_ix, builder, CommitmentLevel, GrpcUpdateKind, Keypair, Off, On, RpcKit, Signature,
    Solami, SubscriptionBuilder, SwqosClient, Transaction, TxFilter, VersionedTransaction,
};
use solana_client_2::nonblocking::rpc_client::RpcClient as RpcClient2;
use solana_sdk_2::commitment_config::CommitmentConfig as Commitment2;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use solana_sdk_2::instruction::Instruction as Ix2;
use solana_sdk_2::pubkey::Pubkey as Pk2;
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

const PUMP_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const WSOL: &str = "So11111111111111111111111111111111111111112";
const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const TIP_SOL: f64 = 0.0001;

type RpcClient = Solami<On<RpcKit>, Off, Off>;
type BeamClient = Solami<Off, Off, On<SwqosClient>>;

struct Config {
    budget_lamports: u64,
    slippage_bps: u64,
    armed: bool,
    sim: bool,
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    run().await;
}

async fn run() {
    let grpc_token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN is not set");
    let rpc_token = env::var("RPC_TOKEN").expect("RPC_TOKEN is not set");
    let payer = Keypair::from_base58_string(&env::var("KEYPAIR").expect("KEYPAIR is not set"));

    let initial_target: Option<Pubkey> =
        env::var("TARGET").ok().map(|s| s.parse().expect("bad TARGET pubkey"));
    let cfg = Config {
        budget_lamports: (env::var("BUY_SOL").expect("BUY_SOL is not set").parse::<f64>().expect("bad BUY_SOL")
            * 1_000_000_000.0) as u64,
        slippage_bps: env::var("SLIPPAGE_BPS").ok().and_then(|s| s.parse().ok()).unwrap_or(500),
        armed: env::var("ARM").is_ok(),
        sim: env::var("SIM").map(|v| v != "0" && !v.eq_ignore_ascii_case("false")).unwrap_or(true),
    };

    let rpc: RpcClient = builder().with_rpc(&rpc_token).build().await.expect("failed to connect rpc");
    let mut grpc = builder().with_grpc(&grpc_token).build().await.expect("failed to connect grpc");
    let beam: Option<BeamClient> = if cfg.armed {
        let key = env::var("SWQOS_TOKEN").expect("ARM set but SWQOS_TOKEN is not set");
        Some(builder().with_swqos(&key).build().await.expect("failed to connect swqos"))
    } else {
        None
    };

    info!(rpc_url = %rpc.rpc().url(), "connected; fetching pump global + fee config...");
    let pump_rpc = Arc::new(RpcClient2::new_with_commitment(
        rpc.rpc().url().to_string(),
        Commitment2::processed(),
    ));
    let pump = AsyncPumpClient::new(pump_rpc);
    let sdk = PumpSdk::new();
    let global = pump.fetch_global().await.expect("fetch_global");
    let fee_config = pump.fetch_fee_config().await.expect("fetch_fee_config");

    match &initial_target {
        Some(t) => info!(target = %t, "copying a pinned target"),
        None => warn!("no TARGET set; will copy the FIRST pump.fun buyer observed (set TARGET to pin one)"),
    }
    info!(
        our_wallet = %payer.pubkey(),
        budget_sol = cfg.budget_lamports as f64 / 1e9,
        slippage_bps = cfg.slippage_bps,
        simulate = cfg.sim,
        mode = if cfg.armed { "ARMED (will beam)" } else { "DRY-RUN (build + log only)" },
        "copy-trade bot starting (pump-rust-client buy builder + solami beam)"
    );

    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .transactions(
            "target",
            TxFilter {
                vote: Some(false),
                failed: Some(false),
                account_include: match &initial_target {
                    Some(t) => vec![t.to_string()],
                    None => vec![PUMP_PROGRAM.to_owned()],
                },
                account_exclude: vec![],
                account_required: vec![],
                signature: None,
            },
        )
        .build();

    let (_sink, mut stream) = grpc.grpc().subscribe(request).await.expect("subscribe failed");
    let pump_bytes = bs58::decode(PUMP_PROGRAM).into_vec().unwrap();
    let mut target = initial_target;

    info!("subscribed, watching for pump.fun buys...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                if let Some(GrpcUpdateKind::Transaction(tx)) = update.update_oneof {
                    if let Some((mint, src_sig, buyer)) = detect_target_buy(&tx, &pump_bytes, target) {
                        if target.is_none() {
                            warn!(%buyer, "no TARGET set; latching onto first observed buyer and copying it");
                            target = Some(buyer);
                        }
                        handle_buy(
                            &mint, &src_sig, &pump, &sdk, &global, &fee_config, &rpc, &payer, &cfg,
                            beam.as_ref(),
                        )
                        .await;
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "stream error");
                break;
            }
        }
    }
}

fn detect_target_buy(
    tx: &SubscribeUpdateTransaction,
    pump_bytes: &[u8],
    target: Option<Pubkey>,
) -> Option<(String, String, Pubkey)> {
    let info = tx.transaction.as_ref()?;
    let message = info.transaction.as_ref()?.message.as_ref()?;
    let meta = info.meta.as_ref()?;

    let mut keys: Vec<&[u8]> = message.account_keys.iter().map(|k| k.as_slice()).collect();
    for k in meta.loaded_writable_addresses.iter().chain(meta.loaded_readonly_addresses.iter()) {
        keys.push(k.as_slice());
    }

    let buyer = Pubkey::try_from(*keys.first()?).ok()?;
    let mint = meta
        .post_token_balances
        .iter()
        .find(|b| b.mint != WSOL && !b.mint.is_empty())?
        .mint
        .clone();

    let all = message
        .instructions
        .iter()
        .map(|ix| (ix.program_id_index, &ix.accounts, &ix.data))
        .chain(meta.inner_instructions.iter().flat_map(|g| {
            g.instructions.iter().map(|ix| (ix.program_id_index, &ix.accounts, &ix.data))
        }));

    for (program_idx, accounts, data) in all {
        if keys.get(program_idx as usize).copied() != Some(pump_bytes) {
            continue;
        }
        if data.len() < 8 || data[..8] != BUY_DISCRIMINATOR {
            continue;
        }
        let matches = match target {
            Some(t) => accounts
                .iter()
                .any(|i| keys.get(*i as usize) == Some(&t.to_bytes().as_slice())),
            None => true,
        };
        if matches {
            let src_sig = info
                .signature
                .get(..64)
                .map(|b| bs58::encode(b).into_string())
                .unwrap_or_default();
            return Some((mint, src_sig, buyer));
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn handle_buy(
    mint: &str,
    src_sig: &str,
    pump: &AsyncPumpClient,
    sdk: &PumpSdk,
    global: &pump_rust_client::state::Global,
    fee_config: &pump_rust_client::state::FeeConfig,
    rpc: &RpcClient,
    payer: &Keypair,
    cfg: &Config,
    beam: Option<&BeamClient>,
) {
    let mint2: Pk2 = match mint.parse() {
        Ok(m) => m,
        Err(_) => return,
    };

    let bonding_curve = match pump.fetch_bonding_curve(&mint2).await {
        Ok(bc) => bc,
        Err(e) => {
            warn!(mint, error = %e, "fetch_bonding_curve failed (graduated to AMM?), skipping");
            return;
        }
    };

    let quote = match sdk.buy_quote_bonding_curve_sol_in(
        global,
        Some(fee_config),
        &bonding_curve,
        bonding_curve.token_total_supply,
        cfg.budget_lamports,
        cfg.slippage_bps as u16,
    ) {
        Ok(q) => q,
        Err(e) => {
            warn!(mint, error = ?e, "quote failed, skipping");
            return;
        }
    };

    let our_user2 = Pk2::new_from_array(payer.pubkey().to_bytes());
    let our_max_sol = cfg.budget_lamports * (10_000 + cfg.slippage_bps) / 10_000;

    let built = sdk.buy_v2_instructions(
        global,
        &bonding_curve,
        mint2,
        constants::SPL_TOKEN_PROGRAM_ID,
        our_user2,
        quote.amount,
        our_max_sol,
    );
    let Some(pump_ixs) = built else {
        warn!(mint, "buy_v2_instructions returned None, skipping");
        return;
    };

    let mut instructions: Vec<Instruction> = pump_ixs.into_iter().map(bridge_ix).collect();
    instructions.push(build_tip_ix(&payer.pubkey(), TIP_SOL));

    info!(
        src_sig = %src_sig,
        mint = %mint,
        tokens_out = quote.amount,
        our_max_sol = our_max_sol,
        ixs = instructions.len(),
        "detected target buy -> quoted + built copy via pump-rust-client"
    );

    let blockhash = match rpc.get_latest_blockhash().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to fetch blockhash");
            return;
        }
    };

    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    let tx = VersionedTransaction::from(tx);

    let sim_ok = if cfg.sim {
        match rpc.simulate_transaction(&tx).await {
            Ok(resp) => match &resp.value.err {
                Some(e) => {
                    warn!(error = ?e, units = ?resp.value.units_consumed, "simulation failed");
                    false
                }
                None => {
                    info!(units = ?resp.value.units_consumed, "simulation ok");
                    true
                }
            },
            Err(e) => {
                warn!(error = %e, "simulate call failed");
                false
            }
        }
    } else {
        true
    };

    match beam {
        Some(client) => {
            if !sim_ok {
                warn!("skipping beam: simulation failed (set SIM=0 to bypass)");
                return;
            }
            match client.beam(&tx).await {
                Ok(sig) => {
                    info!(%sig, "beamed copy buy, confirming...");
                    confirm(rpc, &sig).await;
                }
                Err(e) => warn!(error = %e, "beam failed"),
            }
        }
        None => {
            let bytes = bincode::serialize(&tx).map(|v| v.len()).unwrap_or(0);
            info!(
                signature = %tx.signatures.first().map(|s| s.to_string()).unwrap_or_default(),
                tx_bytes = bytes,
                "DRY-RUN: built and signed copy buy (not sent). set ARM=1 + SWQOS_TOKEN to go live"
            );
        }
    }
}

fn bridge_ix(ix: Ix2) -> Instruction {
    Instruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|m| AccountMeta {
                pubkey: Pubkey::new_from_array(m.pubkey.to_bytes()),
                is_signer: m.is_signer,
                is_writable: m.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

async fn confirm(rpc: &RpcClient, sig: &Signature) {
    for _ in 0..30 {
        match rpc
            .get_signature_status_with_commitment(sig, CommitmentConfig::confirmed())
            .await
        {
            Ok(Some(Ok(()))) => {
                info!(%sig, "copy buy landed and confirmed");
                return;
            }
            Ok(Some(Err(e))) => {
                warn!(%sig, error = %e, "copy buy landed but failed on-chain");
                return;
            }
            _ => {}
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    warn!(%sig, "copy buy not confirmed within 30s");
}
