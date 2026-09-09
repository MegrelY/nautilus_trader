//! Read-only reuse of the owning execution connection, scoped by venue and user.
use crate::{common::enums::HyperliquidEnvironment, websocket::client::HyperliquidWebSocketClient};
use serde_json::Value;
use std::{
    sync::{Mutex, OnceLock},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct PrivateAccountObservation {
    pub generation: u64,
    pub clearinghouse_states: Vec<(String, Value)>,
    pub spot_state: Value,
    pub open_orders: Vec<(String, Vec<Value>)>,
}

type Entry = (HyperliquidEnvironment, String, HyperliquidWebSocketClient);
fn readers() -> &'static Mutex<Vec<Entry>> {
    static READERS: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();
    READERS.get_or_init(Mutex::default)
}
pub(crate) fn register(
    environment: HyperliquidEnvironment,
    user: &str,
    client: HyperliquidWebSocketClient,
) {
    let mut entries = readers()
        .lock()
        .expect("private observation registry poisoned");
    entries.retain(|(env, address, _)| *env != environment || !address.eq_ignore_ascii_case(user));
    if entries.len() >= 8 {
        entries.remove(0);
    }
    entries.push((environment, user.to_owned(), client));
}

/// No retained value survives a reconnect generation invalidation. Quiet order
/// streams retain their complete snapshot only while fresh account frames keep
/// the owning connection current. Missing coverage returns None, never absence.
pub fn read(environment: HyperliquidEnvironment, user: &str) -> Option<PrivateAccountObservation> {
    let entries = readers().lock().ok()?;
    let (_, _, client) = entries
        .iter()
        .find(|(env, address, _)| *env == environment && address.eq_ignore_ascii_case(user))?;
    client.read_private_observation(user, Duration::from_secs(30))
}
