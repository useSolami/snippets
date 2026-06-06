use std::env;

use futures::StreamExt;
use solami::{
    CommitmentLevel, GrpcUpdateKind, SubscribeRequestFilterAccounts, SubscriptionBuilder,
};
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data,
};

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const OWNER_OFFSET: u64 = 32;

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    subscribe_token_accounts().await;
}

async fn subscribe_token_accounts() {
    let token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN is not set");
    let watch_owner = env::var("WATCH_OWNER").expect("WATCH_OWNER is not set");

    let mut builder = solami::builder().with_grpc(&token);
    if let Ok(endpoint) = env::var("GRPC_ENDPOINT") {
        builder = builder.grpc_url(&endpoint);
    }

    let mut client = builder.build().await.expect("failed to connect to geyser");

    info!(
        url = client.grpc().url(),
        owner = %watch_owner,
        "connected, subscribing to spl token accounts owned by wallet"
    );

    let request = SubscriptionBuilder::new()
        .commitment(CommitmentLevel::Confirmed)
        .accounts(
            "token_accounts",
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![TOKEN_PROGRAM.to_owned(), TOKEN_2022_PROGRAM.to_owned()],
                filters: vec![
                    SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::TokenAccountState(true)),
                    },
                    SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                            offset: OWNER_OFFSET,
                            data: Some(Data::Base58(watch_owner.clone())),
                        })),
                    },
                ],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: None,
            },
        )
        .build();

    let (_sink, mut stream) = client
        .grpc()
        .subscribe(request)
        .await
        .expect("subscribe failed");

    info!("subscribed, watching for token account state changes...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => match update.update_oneof {
                Some(GrpcUpdateKind::Account(acc)) => {
                    let slot = acc.slot;
                    if let Some(info) = acc.account {
                        emit(slot, &info);
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

fn emit(slot: u64, info: &yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo) {
    let pubkey = bs58::encode(&info.pubkey).into_string();

    if info.data.len() < 72 {
        warn!(account = %pubkey, len = info.data.len(), "account data too short to decode");
        return;
    }
    let mint = bs58::encode(&info.data[0..32]).into_string();
    let owner = bs58::encode(&info.data[32..64]).into_string();
    let amount = u64::from_le_bytes(info.data[64..72].try_into().unwrap());

    info!(
        slot = slot,
        account = %pubkey,
        mint = %mint,
        owner = %owner,
        amount = amount,
        lamports = info.lamports,
        write_version = info.write_version,
        "token account update"
    );
}
