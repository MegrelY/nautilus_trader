//! Process-local public DEX discovery. One refresh in flight per endpoint.
use super::{client::HyperliquidRawHttpClient, error::Result, query::InfoRequest};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Default)]
pub(super) struct Discovery {
    pub roster: Option<(Instant, Value)>,
}

pub(super) fn shared(endpoint: &str) -> Arc<tokio::sync::Mutex<Discovery>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<Discovery>>>>> =
        OnceLock::new();
    let mut cache = CACHE
        .get_or_init(Mutex::default)
        .lock()
        .expect("discovery lock poisoned");
    // Production uses one endpoint per environment; bounded for test overrides.
    if cache.len() >= 8 && !cache.contains_key(endpoint) {
        cache.clear();
    }
    cache.entry(endpoint.to_owned()).or_default().clone()
}

pub(super) async fn roster(client: &HyperliquidRawHttpClient, endpoint: &str) -> Result<Value> {
    roster_with_refresh(client, endpoint, false).await
}

pub(super) async fn roster_with_refresh(
    client: &HyperliquidRawHttpClient,
    endpoint: &str,
    force: bool,
) -> Result<Value> {
    let shared = shared(endpoint);
    let mut cached = shared.lock().await;
    if let Some((at, value)) = &cached.roster
        && at.elapsed() < Duration::from_secs(if force { 60 } else { 300 })
    {
        return Ok(value.clone());
    }
    let value = client
        .send_info_request_raw(&InfoRequest::perp_dexs())
        .await?;
    cached.roster = Some((Instant::now(), value.clone()));
    Ok(value)
}
