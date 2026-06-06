use std::collections::HashMap;
use std::env;

use futures::StreamExt;
use solami::{CommitmentLevel, GrpcUpdateKind, SubscriptionBuilder, TxFilter};
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const SYSTEM_TRANSFER: [u8; 4] = [2, 0, 0, 0];
const TOKEN_TRANSFER: u8 = 3;
const TOKEN_TRANSFER_CHECKED: u8 = 12;

struct TokenAcct {
    owner: String,
    mint: String,
    decimals: u32,
}

#[derive(Debug)]
struct TransferEvent {
    slot: u64,
    sig: String,
    kind: &'static str,
    location: String,
    direction: &'static str,
    source: Option<String>,
    destination: Option<String>,
    authority: Option<String>,
    mint: Option<String>,
    amount: u64,
    decimals: Option<u32>,
    ui_amount: Option<f64>,
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
    watch_account_transfers().await;
}

async fn watch_account_transfers() {
    let token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN is not set");
    let watch_account = env::var("WATCH_ACCOUNT").expect("WATCH_ACCOUNT is not set");

    let mut builder = solami::builder().with_grpc(&token);
    if let Ok(endpoint) = env::var("GRPC_ENDPOINT") {
        builder = builder.grpc_url(&endpoint);
    }

    let mut client = builder.build().await.expect("failed to connect to geyser");

    info!(
        url = client.grpc().url(),
        account = %watch_account,
        "connected, subscribing to transactions involving account"
    );

    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Confirmed)
        .transactions(
            "account_txs",
            TxFilter {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![watch_account.clone()],
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

    let system_program = bs58::decode(SYSTEM_PROGRAM).into_vec().unwrap();
    let token_program = bs58::decode(TOKEN_PROGRAM).into_vec().unwrap();
    let token_2022_program = bs58::decode(TOKEN_2022_PROGRAM).into_vec().unwrap();

    info!("subscribed, watching for sol + spl token transfers...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => match update.update_oneof {
                Some(GrpcUpdateKind::Transaction(tx_update)) => {
                    let transfers = handle_tx(
                        &tx_update,
                        &system_program,
                        &token_program,
                        &token_2022_program,
                        &watch_account,
                    )
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "failed to handle tx");
                        Vec::new()
                    });
                    for transfer in transfers {
                        emit(&transfer);
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

fn emit(ev: &TransferEvent) {
    info!(
        slot = ev.slot,
        sig = %ev.sig,
        kind = %ev.kind,
        location = %ev.location,
        direction = %ev.direction,
        source = ?ev.source,
        destination = ?ev.destination,
        authority = ?ev.authority,
        mint = ?ev.mint,
        amount = ev.amount,
        decimals = ?ev.decimals,
        ui_amount = ?ev.ui_amount,
        "transfer"
    );
}

fn handle_tx(
    tx_update: &SubscribeUpdateTransaction,
    system_program: &[u8],
    token_program: &[u8],
    token_2022_program: &[u8],
    watch_account: &str,
) -> Result<Vec<TransferEvent>, String> {
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

    let mut token_accounts: HashMap<u32, TokenAcct> = HashMap::new();
    if let Some(m) = meta {
        for bal in m.pre_token_balances.iter().chain(m.post_token_balances.iter()) {
            token_accounts.entry(bal.account_index).or_insert_with(|| TokenAcct {
                owner: bal.owner.clone(),
                mint: bal.mint.clone(),
                decimals: bal.ui_token_amount.as_ref().map(|a| a.decimals).unwrap_or(0),
            });
        }
    }

    let sig = info
        .signature
        .get(..64)
        .map(|b| bs58::encode(b).into_string())
        .unwrap_or_default();
    let slot = tx_update.slot;
    let mut transfers = Vec::new();

    let mut decode = |program_idx: usize,
                      accounts: &[u8],
                      data: &[u8],
                      outer: usize,
                      inner: Option<usize>| {
        let prog = match keys.get(program_idx) {
            Some(p) => *p,
            None => return,
        };
        let ev = if prog == system_program {
            try_decode_sol(&keys, watch_account, accounts, data, &sig, slot, outer, inner)
        } else if prog == token_program || prog == token_2022_program {
            try_decode_spl(
                &keys,
                &token_accounts,
                watch_account,
                accounts,
                data,
                &sig,
                slot,
                outer,
                inner,
            )
        } else {
            None
        };
        if let Some(ev) = ev {
            transfers.push(ev);
        }
    };

    for (idx, ix) in message.instructions.iter().enumerate() {
        decode(ix.program_id_index as usize, &ix.accounts, &ix.data, idx, None);
    }

    if let Some(m) = meta {
        for inner in &m.inner_instructions {
            for (ii, inst) in inner.instructions.iter().enumerate() {
                decode(
                    inst.program_id_index as usize,
                    &inst.accounts,
                    &inst.data,
                    inner.index as usize,
                    Some(ii),
                );
            }
        }
    }

    Ok(transfers)
}

fn resolve(keys: &[&[u8]], accounts: &[u8], pos: usize) -> Option<String> {
    accounts
        .get(pos)
        .and_then(|i| keys.get(*i as usize))
        .map(|b| bs58::encode(b).into_string())
}

fn location(outer: usize, inner: Option<usize>) -> String {
    match inner {
        Some(ii) => format!("inner[{outer}.{ii}]"),
        None => format!("ix[{outer}]"),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_decode_sol(
    keys: &[&[u8]],
    watch_account: &str,
    accounts: &[u8],
    data: &[u8],
    sig: &str,
    slot: u64,
    outer: usize,
    inner: Option<usize>,
) -> Option<TransferEvent> {
    if data.len() < 12 || data[..4] != SYSTEM_TRANSFER {
        return None;
    }
    let lamports = u64::from_le_bytes(data[4..12].try_into().ok()?);
    let from = resolve(keys, accounts, 0);
    let to = resolve(keys, accounts, 1);

    let direction = if from.as_deref() == Some(watch_account) {
        "out"
    } else if to.as_deref() == Some(watch_account) {
        "in"
    } else {
        return None;
    };

    Some(TransferEvent {
        slot,
        sig: sig.to_owned(),
        kind: "sol",
        location: location(outer, inner),
        direction,
        source: from,
        destination: to,
        authority: None,
        mint: None,
        amount: lamports,
        decimals: Some(9),
        ui_amount: Some(lamports as f64 / 1_000_000_000.0),
    })
}

#[allow(clippy::too_many_arguments)]
fn try_decode_spl(
    keys: &[&[u8]],
    token_accounts: &HashMap<u32, TokenAcct>,
    watch_account: &str,
    accounts: &[u8],
    data: &[u8],
    sig: &str,
    slot: u64,
    outer: usize,
    inner: Option<usize>,
) -> Option<TransferEvent> {
    if data.is_empty() {
        return None;
    }

    let (amount, ix_decimals, src_pos, dst_pos, auth_pos, mint_pos) = match data[0] {
        TOKEN_TRANSFER if data.len() >= 9 => {
            (u64::from_le_bytes(data[1..9].try_into().ok()?), None, 0, 1, 2, None)
        }
        TOKEN_TRANSFER_CHECKED if data.len() >= 10 => (
            u64::from_le_bytes(data[1..9].try_into().ok()?),
            Some(data[9] as u32),
            0,
            2,
            3,
            Some(1),
        ),
        _ => return None,
    };

    let src_idx = *accounts.get(src_pos)? as u32;
    let dst_idx = *accounts.get(dst_pos)? as u32;
    let src = token_accounts.get(&src_idx);
    let dst = token_accounts.get(&dst_idx);

    let direction = if src.map(|t| t.owner.as_str()) == Some(watch_account) {
        "out"
    } else if dst.map(|t| t.owner.as_str()) == Some(watch_account) {
        "in"
    } else {
        return None;
    };

    let mint = src
        .or(dst)
        .map(|t| t.mint.clone())
        .or_else(|| mint_pos.and_then(|p| resolve(keys, accounts, p)));
    let decimals = src.or(dst).map(|t| t.decimals).or(ix_decimals);
    let ui_amount = decimals.map(|d| amount as f64 / 10f64.powi(d as i32));

    Some(TransferEvent {
        slot,
        sig: sig.to_owned(),
        kind: "spl",
        location: location(outer, inner),
        direction,
        source: resolve(keys, accounts, src_pos),
        destination: resolve(keys, accounts, dst_pos),
        authority: resolve(keys, accounts, auth_pos),
        mint,
        amount,
        decimals,
        ui_amount,
    })
}
