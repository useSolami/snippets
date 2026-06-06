use std::env;

use futures::StreamExt;
use solami::{CommitmentLevel, GrpcUpdateKind, SubscriptionBuilder, TxFilter};
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SWAP_BASE_IN: u8 = 9;
const SWAP_BASE_OUT: u8 = 11;

#[derive(Debug)]
struct TradeEvent {
    slot: u64,
    sig: String,
    variant: &'static str,
    location: String,
    quoted_in: u64,
    quoted_out: u64,
    pool: Option<String>,
    user_source: Option<String>,
    user_destination: Option<String>,
    user_owner: Option<String>,
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
    subscribe_raydium_trades().await;
}

async fn subscribe_raydium_trades() {
    let token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN is not set");

    let mut builder = solami::builder().with_grpc(&token);
    if let Ok(endpoint) = env::var("GRPC_ENDPOINT") {
        builder = builder.grpc_url(&endpoint);
    }

    let mut client = builder.build().await.expect("failed to connect to geyser");

    info!(
        url = client.grpc().url(),
        program = RAYDIUM_AMM_V4,
        "connected, subscribing to raydium amm v4 transactions"
    );

    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .transactions(
            "raydium_txs",
            TxFilter {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![RAYDIUM_AMM_V4.to_owned()],
                account_exclude: vec![],
                account_required: vec![],
                signature: None,
            },
        )
        .build();

    let (_sink, mut stream) = client
        .grpc()
        .subscribe(request)
        .await
        .expect("subscribe failed");

    let raydium_program = bs58::decode(RAYDIUM_AMM_V4)
        .into_vec()
        .expect("bad raydium program id");

    info!("subscribed, watching for raydium swaps...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => match update.update_oneof {
                Some(GrpcUpdateKind::Transaction(tx_update)) => {
                    let trades = handle_tx(&tx_update, &raydium_program).unwrap_or_else(|e| {
                        warn!(error = %e, "failed to handle tx");
                        Vec::new()
                    });
                    for trade in trades {
                        emit(&trade);
                    }
                }
                Some(GrpcUpdateKind::Ping(_)) => {}
                _ => {}
            },
            Err(e) => {
                error!(error = %e, "stream error");
                break;
            }
        }
    }
}

fn emit(ev: &TradeEvent) {
    info!(
        slot = ev.slot,
        sig = %ev.sig,
        variant = %ev.variant,
        location = %ev.location,
        quoted_in = ev.quoted_in,
        quoted_out = ev.quoted_out,
        pool = ?ev.pool,
        user_source = ?ev.user_source,
        user_destination = ?ev.user_destination,
        user_owner = ?ev.user_owner,
        "raydium swap"
    );
}

fn handle_tx(
    tx_update: &SubscribeUpdateTransaction,
    raydium_program: &[u8],
) -> Result<Vec<TradeEvent>, String> {
    let info = tx_update.transaction.as_ref().ok_or("missing tx info")?;
    let tx = info.transaction.as_ref().ok_or("missing transaction")?;
    let message = tx.message.as_ref().ok_or("missing message")?;
    let meta = info.meta.as_ref();

    let mut keys: Vec<&[u8]> = message.account_keys.iter().map(|k| k.as_slice()).collect();
    if let Some(m) = meta {
        for k in &m.loaded_writable_addresses {
            keys.push(k.as_slice());
        }
        for k in &m.loaded_readonly_addresses {
            keys.push(k.as_slice());
        }
    }

    let sig = info
        .signature
        .get(..64)
        .map(|b| bs58::encode(b).into_string())
        .unwrap_or_default();
    let slot = tx_update.slot;
    let mut trades = Vec::new();

    for (idx, ix) in message.instructions.iter().enumerate() {
        if let Some(ev) = try_decode_swap(
            &keys,
            raydium_program,
            ix.program_id_index as usize,
            &ix.accounts,
            &ix.data,
            &sig,
            slot,
            idx,
            None,
        ) {
            trades.push(ev);
        }
    }

    if let Some(m) = meta {
        for inner in &m.inner_instructions {
            for (ii, inst) in inner.instructions.iter().enumerate() {
                if let Some(ev) = try_decode_swap(
                    &keys,
                    raydium_program,
                    inst.program_id_index as usize,
                    &inst.accounts,
                    &inst.data,
                    &sig,
                    slot,
                    inner.index as usize,
                    Some(ii),
                ) {
                    trades.push(ev);
                }
            }
        }
    }

    Ok(trades)
}

#[allow(clippy::too_many_arguments)]
fn try_decode_swap(
    keys: &[&[u8]],
    raydium_program: &[u8],
    program_idx: usize,
    accounts: &[u8],
    data: &[u8],
    sig: &str,
    slot: u64,
    outer_ix: usize,
    inner_ix: Option<usize>,
) -> Option<TradeEvent> {
    let prog = keys.get(program_idx)?;
    if *prog != raydium_program {
        return None;
    }
    if data.len() < 17 {
        return None;
    }

    let variant = match data[0] {
        SWAP_BASE_IN => "swap_base_in",
        SWAP_BASE_OUT => "swap_base_out",
        _ => return None,
    };

    let quoted_in = u64::from_le_bytes(data[1..9].try_into().ok()?);
    let quoted_out = u64::from_le_bytes(data[9..17].try_into().ok()?);

    let resolve = |pos: usize| -> Option<String> {
        accounts
            .get(pos)
            .and_then(|i| keys.get(*i as usize))
            .map(|b| bs58::encode(b).into_string())
    };

    let n = accounts.len();
    let pool = resolve(1);
    let user_source = n.checked_sub(3).and_then(resolve);
    let user_destination = n.checked_sub(2).and_then(resolve);
    let user_owner = n.checked_sub(1).and_then(resolve);

    let location = match inner_ix {
        Some(ii) => format!("inner[{outer_ix}.{ii}]"),
        None => format!("ix[{outer_ix}]"),
    };

    Some(TradeEvent {
        slot,
        sig: sig.to_owned(),
        variant,
        location,
        quoted_in,
        quoted_out,
        pool,
        user_source,
        user_destination,
        user_owner,
    })
}
