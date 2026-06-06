use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use solami::{CommitmentLevel, GrpcUpdateKind, SubscriptionBuilder, TxFilter};
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
const CREATE_V2_DISCRIMINATOR: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];

const PENDING_SLOT_GRACE: u64 = 32;
const BLOCK_TIME_CACHE_LIMIT: usize = 4000;

#[derive(Debug)]
struct CreateEvent {
    slot: u64,
    sig: String,
    variant: &'static str,
    location: String,
    name: String,
    symbol: String,
    uri: String,
    mint: Option<String>,
    bonding_curve: Option<String>,
    associated_bonding_curve: Option<String>,
    user: Option<String>,
    creator: String,
    is_mayhem_mode: Option<bool>,
    is_cashback_enabled: Option<bool>,
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    subscribe_pumpfun_creates().await;
}

async fn subscribe_pumpfun_creates() {
    let token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN is not set");

    let mut builder = solami::builder().with_grpc(&token);
    if let Ok(endpoint) = env::var("GRPC_ENDPOINT") {
        builder = builder.grpc_url(&endpoint);
    }

    let mut client = builder.build().await.expect("failed to connect to geyser");

    info!(
        url = client.grpc().url(),
        program = PUMPFUN_PROGRAM_ID,
        "connected, subscribing to pump.fun transactions + block meta"
    );

    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .transactions(
            "pump_txs",
            TxFilter {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![PUMPFUN_PROGRAM_ID.to_owned()],
                account_exclude: vec![],
                account_required: vec![],
                signature: None,
            },
        )
        .blocks_meta("meta")
        .build();

    let (_sink, mut stream) = client
        .grpc()
        .subscribe(request)
        .await
        .expect("subscribe failed");

    let pump_program = bs58::decode(PUMPFUN_PROGRAM_ID)
        .into_vec()
        .expect("bad pump.fun program id");

    let mut block_times: HashMap<u64, i64> = HashMap::new();
    let mut pending: HashMap<u64, Vec<CreateEvent>> = HashMap::new();
    let mut latest_slot: u64 = 0;

    info!("subscribed, watching for pump.fun create instructions...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => match update.update_oneof {
                Some(GrpcUpdateKind::Transaction(tx_update)) => {
                    let events = handle_tx(&tx_update, &pump_program).unwrap_or_else(|e| {
                        warn!(error = %e, "failed to handle tx");
                        Vec::new()
                    });
                    for ev in events {
                        latest_slot = latest_slot.max(ev.slot);
                        match block_times.get(&ev.slot).copied() {
                            Some(bt) => emit(&ev, Some(bt)),
                            None => pending.entry(ev.slot).or_default().push(ev),
                        }
                    }
                    flush_stale_pending(&mut pending, latest_slot);
                }
                Some(GrpcUpdateKind::BlockMeta(bm)) => {
                    latest_slot = latest_slot.max(bm.slot);
                    if let Some(bt) = bm.block_time {
                        block_times.insert(bm.slot, bt.timestamp);
                        if let Some(evs) = pending.remove(&bm.slot) {
                            for ev in evs {
                                emit(&ev, Some(bt.timestamp));
                            }
                        }
                        prune_block_times(&mut block_times, latest_slot);
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

fn emit(ev: &CreateEvent, block_time: Option<i64>) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let age_secs = block_time.map(|bt| now - bt);
    let created_at_iso = block_time.map(unix_to_iso8601);

    info!(
        slot = ev.slot,
        sig = %ev.sig,
        variant = %ev.variant,
        location = %ev.location,
        name = %ev.name,
        symbol = %ev.symbol,
        uri = %ev.uri,
        mint = ?ev.mint,
        bonding_curve = ?ev.bonding_curve,
        associated_bonding_curve = ?ev.associated_bonding_curve,
        user = ?ev.user,
        creator = %ev.creator,
        is_mayhem_mode = ?ev.is_mayhem_mode,
        is_cashback_enabled = ?ev.is_cashback_enabled,
        block_time = ?block_time,
        created_at = ?created_at_iso,
        age_secs = ?age_secs,
        "pump.fun create"
    );
}

fn flush_stale_pending(pending: &mut HashMap<u64, Vec<CreateEvent>>, latest_slot: u64) {
    let stale_slots: Vec<u64> = pending
        .keys()
        .copied()
        .filter(|s| latest_slot.saturating_sub(*s) > PENDING_SLOT_GRACE)
        .collect();
    for slot in stale_slots {
        if let Some(evs) = pending.remove(&slot) {
            for ev in evs {
                warn!(
                    slot = ev.slot,
                    "block_time never arrived; emitting without timestamp"
                );
                emit(&ev, None);
            }
        }
    }
}

fn prune_block_times(cache: &mut HashMap<u64, i64>, latest_slot: u64) {
    if cache.len() <= BLOCK_TIME_CACHE_LIMIT {
        return;
    }
    let cutoff = latest_slot.saturating_sub(1500);
    cache.retain(|slot, _| *slot >= cutoff);
}

fn unix_to_iso8601(ts: i64) -> String {
    let days_since_epoch = ts.div_euclid(86_400);
    let secs_of_day = ts.rem_euclid(86_400);
    let (hour, rem) = (secs_of_day / 3600, secs_of_day % 3600);
    let (minute, second) = (rem / 60, rem % 60);
    let (year, month, day) = civil_from_days(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn handle_tx(
    tx_update: &SubscribeUpdateTransaction,
    pump_program: &[u8],
) -> Result<Vec<CreateEvent>, String> {
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
    let mut events = Vec::new();

    for (idx, ix) in message.instructions.iter().enumerate() {
        if let Some(ev) = try_decode_create(
            &keys,
            pump_program,
            ix.program_id_index as usize,
            &ix.accounts,
            &ix.data,
            &sig,
            slot,
            idx,
            None,
        ) {
            events.push(ev);
        }
    }

    if let Some(m) = meta {
        for inner in &m.inner_instructions {
            for (ii, inst) in inner.instructions.iter().enumerate() {
                if let Some(ev) = try_decode_create(
                    &keys,
                    pump_program,
                    inst.program_id_index as usize,
                    &inst.accounts,
                    &inst.data,
                    &sig,
                    slot,
                    inner.index as usize,
                    Some(ii),
                ) {
                    events.push(ev);
                }
            }
        }
    }

    Ok(events)
}

#[allow(clippy::too_many_arguments)]
fn try_decode_create(
    keys: &[&[u8]],
    pump_program: &[u8],
    program_idx: usize,
    accounts: &[u8],
    data: &[u8],
    sig: &str,
    slot: u64,
    outer_ix: usize,
    inner_ix: Option<usize>,
) -> Option<CreateEvent> {
    let prog = keys.get(program_idx)?;
    if *prog != pump_program {
        return None;
    }
    if data.len() < 8 {
        return None;
    }
    let disc: [u8; 8] = data[..8].try_into().ok()?;

    let (variant, user_idx, parse_v2_tail) = match disc {
        CREATE_DISCRIMINATOR => ("create", 7, false),
        CREATE_V2_DISCRIMINATOR => ("create_v2", 5, true),
        _ => return None,
    };

    let mut cursor = &data[8..];
    let name = read_borsh_string(&mut cursor)?;
    let symbol = read_borsh_string(&mut cursor)?;
    let uri = read_borsh_string(&mut cursor)?;
    if cursor.len() < 32 {
        return None;
    }
    let mut creator_bytes = [0u8; 32];
    creator_bytes.copy_from_slice(&cursor[..32]);
    let creator = bs58::encode(creator_bytes).into_string();
    cursor = &cursor[32..];

    let (is_mayhem_mode, is_cashback_enabled) = if parse_v2_tail {
        (
            cursor.first().copied().map(|b| b != 0),
            cursor.get(1).copied().map(|b| b != 0),
        )
    } else {
        (None, None)
    };

    let resolve = |pos: usize| -> Option<String> {
        accounts
            .get(pos)
            .and_then(|i| keys.get(*i as usize))
            .map(|b| bs58::encode(b).into_string())
    };
    let mint = resolve(0);
    let bonding_curve = resolve(2);
    let associated_bonding_curve = resolve(3);
    let user = resolve(user_idx);

    let location = match inner_ix {
        Some(ii) => format!("inner[{outer_ix}.{ii}]"),
        None => format!("ix[{outer_ix}]"),
    };

    Some(CreateEvent {
        slot,
        sig: sig.to_owned(),
        variant,
        location,
        name,
        symbol,
        uri,
        mint,
        bonding_curve,
        associated_bonding_curve,
        user,
        creator,
        is_mayhem_mode,
        is_cashback_enabled,
    })
}

fn read_borsh_string(cursor: &mut &[u8]) -> Option<String> {
    if cursor.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(cursor[..4].try_into().ok()?) as usize;
    *cursor = &cursor[4..];
    if cursor.len() < len {
        return None;
    }
    let s = String::from_utf8(cursor[..len].to_vec()).ok()?;
    *cursor = &cursor[len..];
    Some(s)
}
