//! Bounded REST fill observations shared by the cell's account readers.
//! Only validated pages survive cancellation; account/position truth stays in WS snapshots.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, OnceLock},
};

use serde_json::Value;

use super::{
    client::HyperliquidRawHttpClient,
    error::{Error, Result},
    fill_history::{FILL_HISTORY_REQUEST_LIMIT, FILL_PAGE_LIMIT, FillHistory, fill_identity},
};

#[derive(Clone)]
pub(super) struct RecentSnapshot {
    pub values: Vec<Value>,
    end: u64,
    // A full recent page may omit more records at its oldest millisecond.
    // None means the venue returned its entire (short) retained history.
    after: Option<u64>,
}

impl RecentSnapshot {
    fn covers(&self, start: u64, end: u64) -> bool {
        end <= self.end && self.after.is_none_or(|floor| start > floor)
    }
}

#[derive(Default)]
struct RecentState {
    snapshot: Option<RecentSnapshot>,
    pending: Option<FillHistory>,
}

struct HistoryState {
    history: FillHistory,
    complete: bool,
}

#[derive(Default)]
struct AccountFills {
    recent: tokio::sync::Mutex<RecentState>,
    history: tokio::sync::Mutex<Option<HistoryState>>,
}

fn shared(endpoint: &str, user: &str) -> Result<Arc<AccountFills>> {
    static CACHE: OnceLock<Mutex<HashMap<(String, String), Arc<AccountFills>>>> = OnceLock::new();
    let mut entries = CACHE
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| Error::decode("Fill observation cache lock failed"))?;
    let key = (endpoint.to_owned(), user.to_ascii_lowercase());
    if let Some(entry) = entries.get(&key) {
        return Ok(entry.clone());
    }
    if entries.len() >= 8 {
        let unused = entries
            .iter()
            .find(|(_, entry)| Arc::strong_count(entry) == 1)
            .map(|(key, _)| key.clone());
        if let Some(key) = unused {
            entries.remove(&key);
        } else {
            return Err(Error::decode("Fill observation account limit reached"));
        }
    }
    let entry = Arc::new(AccountFills::default());
    entries.insert(key, entry.clone());
    Ok(entry)
}

fn timestamp(value: &Value) -> Result<u64> {
    value
        .get("time")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::decode("Fill has no exact timestamp"))
}

fn merge(first: &[Value], second: &[Value]) -> Result<Vec<Value>> {
    let mut records = BTreeMap::new();
    for value in first.iter().chain(second) {
        timestamp(value)?;
        let key = fill_identity(value).map_err(Error::decode)?;
        if let Some(previous) = records.insert(key, value.clone()) {
            if previous != *value {
                return Err(Error::decode(
                    "Conflicting payloads for the same venue fill identity",
                ));
            }
        }
    }
    let mut values: Vec<Value> = records.into_values().collect();
    values.sort_by_key(|value| {
        (
            value["time"].as_u64(),
            value["oid"].as_u64(),
            value["tid"].as_u64(),
        )
    });
    Ok(values)
}

fn bounded_recent(
    mut values: Vec<Value>,
    end: u64,
    mut after: Option<u64>,
) -> Result<RecentSnapshot> {
    if values.len() > FILL_PAGE_LIMIT {
        values.drain(..values.len() - FILL_PAGE_LIMIT);
        after = values.first().map(timestamp).transpose()?;
    }
    Ok(RecentSnapshot { values, end, after })
}

fn prune_before(values: &mut Vec<Value>, start: u64) {
    // Keep every record at the last complete timestamp before the requested
    // window. That witness must survive until the fresh inclusive tail joins.
    if let Some(witness) = values
        .iter()
        .filter_map(|value| value["time"].as_u64())
        .filter(|time| *time < start)
        .max()
    {
        values.retain(|value| value["time"].as_u64().is_some_and(|time| time >= witness));
    }
}

pub(super) async fn recent(
    client: &HyperliquidRawHttpClient,
    endpoint: &str,
    user: &str,
    end: u64,
) -> Result<RecentSnapshot> {
    let account = shared(endpoint, user)?;
    let mut state = account.recent.lock().await;
    if state.snapshot.is_none() {
        let response = client.info_user_fills_raw(user).await?;
        let page = response
            .as_array()
            .ok_or_else(|| Error::decode("Expected a recent fill array"))?;
        if page.len() > FILL_PAGE_LIMIT {
            return Err(Error::decode("Recent fills exceed venue page limit"));
        }
        let values = merge(&[], page)?;
        let after = if page.len() == FILL_PAGE_LIMIT {
            values.first().map(timestamp).transpose()?
        } else {
            None
        };
        state.snapshot = Some(RecentSnapshot { values, end, after });
    }
    for _ in 0..FILL_HISTORY_REQUEST_LIMIT {
        let previous = state
            .snapshot
            .as_ref()
            .expect("recent snapshot initialized")
            .clone();
        if previous.end >= end {
            return Ok(previous);
        }
        if state.pending.is_none() {
            let cursor = previous
                .values
                .last()
                .map(timestamp)
                .transpose()?
                .unwrap_or(previous.end);
            // A recent response may include fills newer than its request start.
            let boundary = previous
                .values
                .iter()
                .filter(|value| value["time"].as_u64() == Some(cursor))
                .cloned()
                .collect();
            state.pending = Some(FillHistory::tail(cursor, end.max(cursor), boundary));
        }
        let pending = state.pending.as_ref().expect("tail initialized");
        let response = client
            .info_user_fills_by_time_raw(user, pending.cursor(), pending.end())
            .await?;
        // Validate transactionally: malformed/contradictory pages never become checkpoints.
        let mut accepted = pending.clone();
        let complete = match accepted.accept(response) {
            Ok(complete) => complete,
            Err(error) => {
                state.pending = None;
                state.snapshot = None;
                return Err(Error::decode(error));
            }
        };
        if complete {
            let accepted_end = accepted.end();
            let delta = accepted.into_records();
            let values = match merge(&previous.values, &delta) {
                Ok(values) => values,
                Err(error) => {
                    state.pending = None;
                    state.snapshot = None;
                    return Err(error);
                }
            };
            state.snapshot = Some(bounded_recent(values, accepted_end, previous.after)?);
            state.pending = None;
        } else {
            state.pending = Some(accepted);
        }
    }
    Err(Error::decode(
        "Recent fill traversal needs another bounded recovery attempt",
    ))
}

/// Proves the requested window from a recent snapshot, or resumes a full walk.
/// Each request still pays its venue weight. A canceled caller leaves accepted
/// pages in the locked state, without promoting the unfinished history.
pub(super) async fn history(
    client: &HyperliquidRawHttpClient,
    endpoint: &str,
    user: &str,
    start: u64,
    end: u64,
) -> Result<Vec<Value>> {
    let recent = recent(client, endpoint, user, end).await?;
    if recent.covers(start, end) {
        return Ok(recent
            .values
            .into_iter()
            .filter(|value| {
                value["time"]
                    .as_u64()
                    .is_some_and(|time| start <= time && time <= end)
            })
            .collect());
    }
    let account = shared(endpoint, user)?;
    let mut stored = account.history.lock().await;
    if stored
        .as_ref()
        .is_none_or(|state| start < state.history.requested_start())
    {
        *stored = Some(HistoryState {
            history: FillHistory::new(start, end),
            complete: false,
        });
    }
    for _ in 0..FILL_HISTORY_REQUEST_LIMIT {
        let state = stored.as_mut().expect("history initialized");
        state.history.prune_before(start);
        if state.complete {
            if state.history.end() < end && !recent.covers(state.history.end(), end) {
                // The recent tail cannot bridge this interval. Walk it with the
                // same inclusive boundary instead of asserting missing coverage.
                state.history.extend_end(end);
                state.complete = false;
            } else {
                if state.history.needs_retention_probe() {
                    state
                        .history
                        .verify_retention_probe(&Value::Array(recent.values.clone()))
                        .map_err(Error::decode)?;
                }
                let fixed = state.history.clone().into_records();
                let tail = recent
                    .values
                    .iter()
                    .filter(|value| {
                        value["time"]
                            .as_u64()
                            .is_some_and(|time| state.history.end() <= time && time <= end)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let mut values = merge(&fixed, &tail)?;
                prune_before(&mut values, start);
                if values.len() > 10_000
                    || (values.len() == 10_000
                        && values
                            .first()
                            .is_some_and(|value| start <= value["time"].as_u64().unwrap_or(0)))
                {
                    return Err(Error::decode(
                        "Requested fill history reaches the venue retention boundary",
                    ));
                }
                // This prefix is complete only after the explicit tail join.
                state.history = FillHistory::proven(start, end, values.clone());
                return Ok(values
                    .into_iter()
                    .filter(|value| {
                        value["time"]
                            .as_u64()
                            .is_some_and(|time| start <= time && time <= end)
                    })
                    .collect());
            }
        }
        let state = stored.as_ref().expect("history initialized");
        let response = client
            .info_user_fills_by_time_raw(user, state.history.cursor(), state.history.end())
            .await?;
        let mut accepted = state.history.clone();
        let complete = match accepted.accept(response) {
            Ok(complete) => complete,
            Err(error) => {
                *stored = None;
                return Err(Error::decode(error));
            }
        };
        *stored = Some(HistoryState {
            history: accepted,
            complete,
        });
    }
    Err(Error::decode(
        "Fill history needs another bounded recovery attempt",
    ))
}
