use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Semaphore;
use solami::{
    builder, CommitmentLevel, GrpcUpdateKind, Off, On, RpcKit, Solami, SubscribeRequestFilterAccounts,
    SubscriptionBuilder,
};
use solana_account_decoder::UiAccountEncoding;
use solana_account_decoder::UiDataSliceConfig;
use solana_commitment_config::CommitmentConfig;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::pubkey::Pubkey;
use tracing::{error, info, warn};

const PUMP_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMP_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const WSOL: &str = "So11111111111111111111111111111111111111112";
const DEFAULT_TOKEN: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

type RpcClient = Solami<On<RpcKit>, Off, Off>;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Cp,
    Orca,
    Dlmm,
}

struct VenueSpec {
    name: &'static str,
    program: &'static str,
    kind: Kind,
    data_size: Option<u64>,
    mint_a_off: usize,
    mint_b_off: usize,
    vault_a_off: usize,
    vault_b_off: usize,
}

const RAYDIUM_V4_SPEC: VenueSpec = VenueSpec {
    name: "raydium_v4",
    program: RAYDIUM_V4,
    kind: Kind::Cp,
    data_size: Some(752),
    mint_a_off: 400,
    mint_b_off: 432,
    vault_a_off: 336,
    vault_b_off: 368,
};
const RAYDIUM_CPMM_SPEC: VenueSpec = VenueSpec {
    name: "raydium_cpmm",
    program: RAYDIUM_CPMM,
    kind: Kind::Cp,
    data_size: None,
    mint_a_off: 168,
    mint_b_off: 200,
    vault_a_off: 72,
    vault_b_off: 104,
};
const PUMP_AMM_SPEC: VenueSpec = VenueSpec {
    name: "pump_amm",
    program: PUMP_AMM,
    kind: Kind::Cp,
    data_size: None,
    mint_a_off: 43,
    mint_b_off: 75,
    vault_a_off: 139,
    vault_b_off: 171,
};
const ORCA_SPEC: VenueSpec = VenueSpec {
    name: "orca_whirlpool",
    program: ORCA,
    kind: Kind::Orca,
    data_size: Some(653),
    mint_a_off: 101,
    mint_b_off: 181,
    vault_a_off: 133,
    vault_b_off: 213,
};
const DLMM_SPEC: VenueSpec = VenueSpec {
    name: "meteora_dlmm",
    program: DLMM,
    kind: Kind::Dlmm,
    data_size: None,
    mint_a_off: 72,
    mint_b_off: 104,
    vault_a_off: 136,
    vault_b_off: 168,
};

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Token,
    Wsol,
}

enum Action {
    CpVault(usize, Side),
    ClState(usize),
}

struct Pool {
    venue: &'static str,
    id: Pubkey,
    kind: Kind,
    token_vault: Pubkey,
    wsol_vault: Pubkey,
    token_is_base: bool,
    rt: u64,
    rq: u64,
    liquidity_wsol: u64,
    price: Option<f64>,
    fresh: bool,
}

impl Pool {
    fn cur_price(&self, dt: u32, dq: u32) -> Option<f64> {
        match self.kind {
            Kind::Cp => {
                if self.rt == 0 || self.rq == 0 {
                    return None;
                }
                let q = self.rq as f64 / 10f64.powi(dq as i32);
                let t = self.rt as f64 / 10f64.powi(dt as i32);
                Some(q / t)
            }
            _ => self.price,
        }
    }
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
    let token_mint: Pubkey = env::var("TOKEN_MINT")
        .unwrap_or_else(|_| DEFAULT_TOKEN.to_owned())
        .parse()
        .expect("bad TOKEN_MINT");
    let wsol: Pubkey = WSOL.parse().unwrap();
    let min_spread_bps: u64 = env::var("MIN_SPREAD_BPS").ok().and_then(|s| s.parse().ok()).unwrap_or(50);
    let min_wsol: u64 = (env::var("MIN_WSOL_SOL").ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.0)
        * 1_000_000_000.0) as u64;
    let min_profit_sol: f64 = env::var("MIN_PROFIT_SOL").ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);

    let rpc: RpcClient = builder().with_rpc(&rpc_token).build().await.expect("failed to connect rpc");
    let mut grpc = builder().with_grpc(&grpc_token).build().await.expect("failed to connect grpc");
    if let Ok(endpoint) = env::var("GRPC_ENDPOINT") {
        grpc = builder().with_grpc(&grpc_token).grpc_url(&endpoint).build().await.expect("failed to connect grpc");
    }

    info!(token = %token_mint, quote = %wsol, min_spread_bps, min_wsol_lamports = min_wsol, "cross-dex arb detector starting (6 venues)");

    let (dt, dq) = fetch_decimals(&rpc, &token_mint, &wsol).await;
    info!(token_decimals = dt, wsol_decimals = dq, "decimals loaded");

    info!("discovering pools across venues (getProgramAccounts ~15s each, capped at 4 in-flight)...");
    let sem = Arc::new(Semaphore::new(3));
    let (pump_c, v4_c, cpmm_c, orca_c, dlmm_c) = tokio::join!(
        discover_pump(&rpc, &token_mint, &wsol, dt, dq),
        discover_gpa(&rpc, &sem, &RAYDIUM_V4_SPEC, &token_mint, &wsol, dt, dq),
        discover_gpa(&rpc, &sem, &RAYDIUM_CPMM_SPEC, &token_mint, &wsol, dt, dq),
        discover_gpa(&rpc, &sem, &ORCA_SPEC, &token_mint, &wsol, dt, dq),
        discover_gpa(&rpc, &sem, &DLMM_SPEC, &token_mint, &wsol, dt, dq),
    );
    let mut candidates = Vec::new();
    candidates.extend(pump_c);
    candidates.extend(v4_c);
    candidates.extend(cpmm_c);
    candidates.extend(orca_c);
    candidates.extend(dlmm_c);

    let mut pools = seed_reserves(&rpc, candidates).await;
    if pools.is_empty() {
        error!("no valid pools discovered for this token/WSOL pair; nothing to compare");
        return;
    }

    let mut account_to_action: HashMap<String, Action> = HashMap::new();
    for (i, p) in pools.iter().enumerate() {
        match p.kind {
            Kind::Cp => {
                account_to_action.insert(p.token_vault.to_string(), Action::CpVault(i, Side::Token));
                account_to_action.insert(p.wsol_vault.to_string(), Action::CpVault(i, Side::Wsol));
            }
            _ => {
                account_to_action.insert(p.id.to_string(), Action::ClState(i));
            }
        }
        info!(
            venue = p.venue,
            kind = if p.kind == Kind::Cp { "cp" } else { "cl" },
            pool = %p.id,
            price = ?p.cur_price(dt, dq),
            liquidity_wsol = p.liquidity_wsol,
            "discovered pool"
        );
    }

    let liquid = pools.iter().filter(|p| p.liquidity_wsol >= min_wsol).count();
    info!(total_pools = pools.len(), liquid_pools = liquid, "pool discovery complete");
    if let Some((b, bp, s, sp, sb)) = spread(&pools, dt, dq, min_wsol) {
        info!(
            spread_bps = sb,
            cheapest = b.venue, cheapest_price = format!("{bp:.9}"),
            dearest = s.venue, dearest_price = format!("{sp:.9}"),
            "initial cross-venue spread across liquid pools"
        );
    } else {
        warn!("fewer than 2 priced+liquid pools; raise activity or lower MIN_WSOL_SOL");
    }

    let accounts: Vec<String> = account_to_action.keys().cloned().collect();
    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .accounts(
            "pools",
            SubscribeRequestFilterAccounts {
                account: accounts,
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: None,
            },
        )
        .build();

    let (_sink, mut stream) = grpc.grpc().subscribe(request).await.expect("subscribe failed");
    let mut last_emit: Option<(Pubkey, Pubkey, u64)> = None;

    info!("subscribed, watching pool prices (CP vaults + CL state accounts)...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                if let Some(GrpcUpdateKind::Account(acc)) = update.update_oneof {
                    let slot = acc.slot;
                    if let Some(info) = acc.account {
                        let pubkey = bs58::encode(&info.pubkey).into_string();
                        match account_to_action.get(&pubkey) {
                            Some(&Action::CpVault(idx, side)) => {
                                if info.data.len() < 72 {
                                    continue;
                                }
                                let amount = u64::from_le_bytes(info.data[64..72].try_into().unwrap());
                                match side {
                                    Side::Token => pools[idx].rt = amount,
                                    Side::Wsol => {
                                        pools[idx].rq = amount;
                                        pools[idx].liquidity_wsol = amount;
                                    }
                                }
                            }
                            Some(&Action::ClState(idx)) => {
                                let (kind, tib) = (pools[idx].kind, pools[idx].token_is_base);
                                pools[idx].price = decode_cl_price(kind, &info.data, tib, dt, dq);
                                pools[idx].fresh = true;
                            }
                            None => continue,
                        }
                        recompute_and_emit(&pools, dt, dq, min_wsol, min_spread_bps, min_profit_sol, slot, &mut last_emit);
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

fn spread<'a>(
    pools: &'a [Pool],
    dt: u32,
    dq: u32,
    min_wsol: u64,
) -> Option<(&'a Pool, f64, &'a Pool, f64, u64)> {
    let priced: Vec<(&Pool, f64)> = pools
        .iter()
        .filter(|p| p.liquidity_wsol >= min_wsol)
        .filter(|p| p.kind == Kind::Cp || p.fresh)
        .filter_map(|p| p.cur_price(dt, dq).map(|pr| (p, pr)))
        .collect();
    if priced.len() < 2 {
        return None;
    }
    let (buy, buy_price) = priced.iter().cloned().min_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
    let (sell, sell_price) = priced.iter().cloned().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
    if buy_price <= 0.0 {
        return None;
    }
    let spread_bps = ((sell_price - buy_price) / buy_price * 10_000.0) as u64;
    Some((buy, buy_price, sell, sell_price, spread_bps))
}

#[allow(clippy::too_many_arguments)]
fn recompute_and_emit(
    pools: &[Pool],
    dt: u32,
    dq: u32,
    min_wsol: u64,
    min_spread_bps: u64,
    min_profit_sol: f64,
    slot: u64,
    last_emit: &mut Option<(Pubkey, Pubkey, u64)>,
) {
    let Some((buy, buy_price, sell, sell_price, spread_bps)) = spread(pools, dt, dq, min_wsol) else {
        return;
    };
    if spread_bps < min_spread_bps {
        return;
    }

    let cp_legs = buy.kind == Kind::Cp && sell.kind == Kind::Cp;
    let (in_sol, token_qty, net_sol) = if cp_legs {
        simulate_arb(buy, sell, dt)
    } else {
        (0.0, 0.0, 0.0)
    };
    if cp_legs && net_sol <= min_profit_sol {
        return;
    }

    let key = (buy.id, sell.id, spread_bps / 10);
    if last_emit.as_ref() == Some(&key) {
        return;
    }
    *last_emit = Some(key);

    if cp_legs {
        info!(
            slot,
            spread_bps,
            buy_venue = buy.venue,
            buy_pool = %buy.id,
            buy_price = format!("{buy_price:.9}"),
            sell_venue = sell.venue,
            sell_pool = %sell.id,
            sell_price = format!("{sell_price:.9}"),
            optimal_in_sol = format!("{in_sol:.6}"),
            token_qty = format!("{token_qty:.4}"),
            gross_profit_lamports = (net_sol * 1e9) as i64,
            gross_profit_sol = format!("{net_sol:.9}"),
            "arbitrage edge (gross, after pool fees + price impact; EXCLUDES tx fee/priority/tip)"
        );
    } else {
        info!(
            slot,
            spread_bps,
            buy_venue = buy.venue,
            buy_pool = %buy.id,
            buy_price = format!("{buy_price:.9}"),
            sell_venue = sell.venue,
            sell_pool = %sell.id,
            sell_price = format!("{sell_price:.9}"),
            sizing = "spot-only (a leg is concentrated-liquidity; exact sizing pending)",
            "cross-venue price gap"
        );
    }
}

fn fee_bps(_venue: &str) -> f64 {
    25.0
}

fn simulate_arb(buy: &Pool, sell: &Pool, dt: u32) -> (f64, f64, f64) {
    let fb = 1.0 - fee_bps(buy.venue) / 10_000.0;
    let fs = 1.0 - fee_bps(sell.venue) / 10_000.0;
    let (rt_b, rq_b) = (buy.rt as f64, buy.rq as f64);
    let (rt_s, rq_s) = (sell.rt as f64, sell.rq as f64);
    let net = |x: f64| -> f64 {
        let xe = x * fb;
        let tok = rt_b * xe / (rq_b + xe);
        let te = tok * fs;
        let out = rq_s * te / (rt_s + te);
        out - x
    };
    let (mut lo, mut hi) = (0.0f64, rq_b.min(rq_s));
    for _ in 0..200 {
        let m1 = lo + (hi - lo) / 3.0;
        let m2 = hi - (hi - lo) / 3.0;
        if net(m1) < net(m2) {
            lo = m1;
        } else {
            hi = m2;
        }
    }
    let x = (lo + hi) / 2.0;
    let xe = x * fb;
    let tok = rt_b * xe / (rq_b + xe);
    (x / 1e9, tok / 10f64.powi(dt as i32), net(x) / 1e9)
}

fn decode_cl_price(kind: Kind, data: &[u8], token_is_base: bool, dt: u32, dq: u32) -> Option<f64> {
    let dec_adj = if token_is_base {
        10f64.powi(dt as i32 - dq as i32)
    } else {
        10f64.powi(dq as i32 - dt as i32)
    };
    let raw = match kind {
        Kind::Orca => {
            let sqrt = u128::from_le_bytes(data.get(65..81)?.try_into().ok()?);
            let s = sqrt as f64 / 2f64.powi(64);
            s * s
        }
        Kind::Dlmm => {
            let active_id = i32::from_le_bytes(data.get(60..64)?.try_into().ok()?);
            let bin_step = u16::from_le_bytes(data.get(64..66)?.try_into().ok()?);
            (1.0 + bin_step as f64 / 10_000.0).powi(active_id)
        }
        Kind::Cp => return None,
    };
    let p = raw * dec_adj;
    if !p.is_finite() || p <= 0.0 {
        return None;
    }
    Some(if token_is_base { p } else { 1.0 / p })
}

async fn get_accounts_chunked(rpc: &RpcClient, keys: &[Pubkey]) -> Vec<Option<solana_sdk::account::Account>> {
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(100) {
        match rpc.get_multiple_accounts(chunk).await {
            Ok(accounts) => out.extend(accounts),
            Err(e) => {
                warn!(error = %e, "get_multiple_accounts chunk failed");
                out.extend(std::iter::repeat_with(|| None).take(chunk.len()));
            }
        }
    }
    out
}

async fn fetch_decimals(rpc: &RpcClient, token: &Pubkey, wsol: &Pubkey) -> (u32, u32) {
    let accounts = rpc.get_multiple_accounts(&[*token, *wsol]).await.expect("get mint accounts");
    let dt = accounts[0].as_ref().map(|a| a.data[44] as u32).expect("token mint missing");
    let dq = accounts[1].as_ref().map(|a| a.data[44] as u32).expect("wsol mint missing");
    (dt, dq)
}

async fn discover_pump(rpc: &RpcClient, token: &Pubkey, wsol: &Pubkey, dt: u32, dq: u32) -> Vec<Candidate> {
    let pump: Pubkey = PUMP_PROGRAM.parse().unwrap();
    let pump_amm: Pubkey = PUMP_AMM.parse().unwrap();
    let authority = Pubkey::find_program_address(&[b"pool-authority", token.as_ref()], &pump).0;
    let pool = Pubkey::find_program_address(
        &[b"pool", &0u16.to_le_bytes(), authority.as_ref(), token.as_ref(), wsol.as_ref()],
        &pump_amm,
    )
    .0;
    match rpc.get_account(&pool).await {
        Ok(account) => decode_candidate(&PUMP_AMM_SPEC, pool, &account.data, token, wsol, dt, dq)
            .into_iter()
            .collect(),
        Err(_) => {
            info!("no pump AMM pool for this token (expected unless it graduated from pump.fun)");
            vec![]
        }
    }
}

async fn discover_gpa(rpc: &RpcClient, sem: &Arc<Semaphore>, spec: &VenueSpec, token: &Pubkey, wsol: &Pubkey, dt: u32, dq: u32) -> Vec<Candidate> {
    let program: Pubkey = spec.program.parse().unwrap();
    let (a, b) = tokio::join!(
        gpa_ids(rpc, sem, &program, spec.data_size, token, spec.mint_a_off, wsol, spec.mint_b_off),
        gpa_ids(rpc, sem, &program, spec.data_size, token, spec.mint_b_off, wsol, spec.mint_a_off),
    );
    let id_vec: Vec<Pubkey> =
        a.into_iter().chain(b).collect::<std::collections::HashSet<_>>().into_iter().collect();
    let mut out = Vec::new();
    if id_vec.is_empty() {
        return out;
    }
    let accounts = get_accounts_chunked(rpc, &id_vec).await;
    for (id, account) in id_vec.iter().zip(accounts) {
        if let Some(account) = account {
            out.extend(decode_candidate(spec, *id, &account.data, token, wsol, dt, dq));
        }
    }
    out
}

#[allow(deprecated)]
async fn gpa_ids(
    rpc: &RpcClient,
    sem: &Arc<Semaphore>,
    program: &Pubkey,
    data_size: Option<u64>,
    token: &Pubkey,
    token_off: usize,
    wsol: &Pubkey,
    wsol_off: usize,
) -> Vec<Pubkey> {
    let mut filters = vec![
        RpcFilterType::Memcmp(Memcmp::new(token_off, MemcmpEncodedBytes::Base58(token.to_string()))),
        RpcFilterType::Memcmp(Memcmp::new(wsol_off, MemcmpEncodedBytes::Base58(wsol.to_string()))),
    ];
    if let Some(size) = data_size {
        filters.insert(0, RpcFilterType::DataSize(size));
    }
    let config = RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: Some(UiDataSliceConfig { offset: 0, length: 0 }),
            commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        },
        ..Default::default()
    };
    for attempt in 0..3 {
        let _permit = sem.acquire().await.unwrap();
        match rpc.get_program_accounts_with_config(program, config.clone()).await {
            Ok(found) => return found.into_iter().map(|(id, _)| id).collect(),
            Err(e) => warn!(error = %e, attempt, "getProgramAccounts failed, retrying"),
        }
    }
    vec![]
}

struct Candidate {
    venue: &'static str,
    id: Pubkey,
    kind: Kind,
    token_vault: Pubkey,
    wsol_vault: Pubkey,
    token_is_base: bool,
    init_price: Option<f64>,
}

fn decode_candidate(
    spec: &VenueSpec,
    id: Pubkey,
    data: &[u8],
    token: &Pubkey,
    wsol: &Pubkey,
    dt: u32,
    dq: u32,
) -> Option<Candidate> {
    let mint_a = read_pubkey(data, spec.mint_a_off)?;
    let mint_b = read_pubkey(data, spec.mint_b_off)?;
    let vault_a = read_pubkey(data, spec.vault_a_off)?;
    let vault_b = read_pubkey(data, spec.vault_b_off)?;

    let (token_vault, wsol_vault, token_is_base) = if mint_a == *token && mint_b == *wsol {
        (vault_a, vault_b, true)
    } else if mint_a == *wsol && mint_b == *token {
        (vault_b, vault_a, false)
    } else {
        warn!(venue = spec.name, pool = %id, "calibration skip: pool is not token/WSOL");
        return None;
    };

    let init_price = match spec.kind {
        Kind::Cp => None,
        _ => decode_cl_price(spec.kind, data, token_is_base, dt, dq),
    };

    Some(Candidate {
        venue: spec.name,
        id,
        kind: spec.kind,
        token_vault,
        wsol_vault,
        token_is_base,
        init_price,
    })
}

fn read_pubkey(data: &[u8], off: usize) -> Option<Pubkey> {
    data.get(off..off + 32).and_then(|s| Pubkey::try_from(s).ok())
}

async fn seed_reserves(rpc: &RpcClient, candidates: Vec<Candidate>) -> Vec<Pool> {
    if candidates.is_empty() {
        return vec![];
    }
    let token_program: Pubkey = TOKEN_PROGRAM.parse().unwrap();
    let mut keys = Vec::new();
    for c in &candidates {
        keys.push(c.token_vault);
        keys.push(c.wsol_vault);
    }
    let accounts = get_accounts_chunked(rpc, &keys).await;

    let mut pools = Vec::new();
    for (i, c) in candidates.into_iter().enumerate() {
        let tv = accounts[i * 2].as_ref();
        let wv = accounts[i * 2 + 1].as_ref();
        let (Some(tv), Some(wv)) = (tv, wv) else {
            warn!(venue = c.venue, pool = %c.id, "calibration skip: vault account missing");
            continue;
        };
        if tv.owner != token_program || wv.owner != token_program || tv.data.len() < 72 || wv.data.len() < 72 {
            warn!(venue = c.venue, pool = %c.id, "calibration skip: vault not an SPL token account");
            continue;
        }
        let rt = u64::from_le_bytes(tv.data[64..72].try_into().unwrap());
        let rq = u64::from_le_bytes(wv.data[64..72].try_into().unwrap());
        pools.push(Pool {
            venue: c.venue,
            id: c.id,
            kind: c.kind,
            token_vault: c.token_vault,
            wsol_vault: c.wsol_vault,
            token_is_base: c.token_is_base,
            rt,
            rq,
            liquidity_wsol: rq,
            price: c.init_price,
            fresh: c.kind == Kind::Cp,
        });
    }
    pools
}
