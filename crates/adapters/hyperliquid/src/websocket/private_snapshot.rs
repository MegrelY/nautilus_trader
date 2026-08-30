// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use nautilus_core::MUTEX_POISONED;
use serde_json::Value;
use tokio::sync::Notify;

use super::messages::{HyperliquidWsMessage, SubscriptionRequest, WsAllDexsClearinghouseStateData};

pub(super) const PRIVATE_STATE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateStateSnapshotScope {
    All,
    OpenOrders,
    Positions,
}

#[derive(Debug, Clone)]
struct SnapshotRequest {
    user: String,
    dexes: Vec<String>,
}

#[derive(Debug, Default)]
struct SnapshotState {
    generation: u64,
    request: Option<SnapshotRequest>,
    open_orders: AHashMap<String, Vec<Value>>,
    clearinghouse_states: Option<AHashMap<String, Value>>,
    spot_state: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PrivateStateSnapshot {
    pub(crate) generation: u64,
    pub(crate) open_orders: Vec<(String, Vec<Value>)>,
    pub(crate) clearinghouse_states: Vec<(String, Value)>,
    pub(crate) spot_state: Option<Value>,
    pub(crate) missing_open_order_dexes: Vec<String>,
    pub(crate) missing_clearinghouse_dexes: Vec<String>,
    pub(crate) clearinghouse_snapshot_received: bool,
}

impl PrivateStateSnapshot {
    #[must_use]
    pub(crate) fn is_complete(&self, scope: PrivateStateSnapshotScope) -> bool {
        let open_orders_complete = self.missing_open_order_dexes.is_empty();
        let positions_complete = self.clearinghouse_snapshot_received
            && self.spot_state.is_some()
            && self.missing_clearinghouse_dexes.is_empty();

        match scope {
            PrivateStateSnapshotScope::All => open_orders_complete && positions_complete,
            PrivateStateSnapshotScope::OpenOrders => open_orders_complete,
            PrivateStateSnapshotScope::Positions => positions_complete,
        }
    }
}

#[derive(Debug, Default)]
struct PrivateStateSnapshotCacheInner {
    state: Mutex<SnapshotState>,
    notify: Notify,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PrivateStateSnapshotCache {
    inner: Arc<PrivateStateSnapshotCacheInner>,
}

impl PrivateStateSnapshotCache {
    pub(crate) fn configure(&self, user: &str, dexes: &[String]) -> Vec<SubscriptionRequest> {
        let mut unique_dexes = Vec::with_capacity(dexes.len());
        let mut seen = AHashSet::with_capacity(dexes.len());
        for dex in dexes {
            if seen.insert(dex.clone()) {
                unique_dexes.push(dex.clone());
            }
        }

        let mut state = self.inner.state.lock().expect(MUTEX_POISONED);
        if let Some(request) = state.request.clone()
            && request.user.eq_ignore_ascii_case(user)
        {
            if request.dexes == unique_dexes {
                return Vec::new();
            }

            let mut subscriptions = Vec::new();
            for dex in &unique_dexes {
                if !request.dexes.contains(dex) {
                    subscriptions.push(SubscriptionRequest::OpenOrders {
                        user: user.to_string(),
                        dex: dex.clone(),
                    });
                }
            }
            state.generation = state.generation.saturating_add(1);
            state.request = Some(SnapshotRequest {
                user: user.to_string(),
                dexes: unique_dexes,
            });
            return subscriptions;
        }

        state.generation = state.generation.saturating_add(1);
        state.request = Some(SnapshotRequest {
            user: user.to_string(),
            dexes: unique_dexes.clone(),
        });
        Self::clear_values(&mut state);

        let mut subscriptions = Vec::with_capacity(unique_dexes.len() + 2);
        subscriptions.push(SubscriptionRequest::AllDexsClearinghouseState {
            user: user.to_string(),
        });
        subscriptions.push(SubscriptionRequest::SpotState {
            user: user.to_string(),
            is_portfolio_margin: None,
        });
        subscriptions.extend(
            unique_dexes
                .into_iter()
                .map(|dex| SubscriptionRequest::OpenOrders {
                    user: user.to_string(),
                    dex,
                }),
        );
        subscriptions
    }

    pub(crate) fn reset(&self) {
        let mut state = self.inner.state.lock().expect(MUTEX_POISONED);
        state.generation = state.generation.saturating_add(1);
        state.request = None;
        Self::clear_values(&mut state);
        drop(state);
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn invalidate_generation(&self) {
        let mut state = self.inner.state.lock().expect(MUTEX_POISONED);
        state.generation = state.generation.saturating_add(1);
        Self::clear_values(&mut state);
        drop(state);
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn observe(&self, message: &HyperliquidWsMessage) {
        let mut state = self.inner.state.lock().expect(MUTEX_POISONED);
        let Some(request) = &state.request else {
            return;
        };

        match message {
            HyperliquidWsMessage::OpenOrders { data }
                if request.user.eq_ignore_ascii_case(&data.user)
                    && request.dexes.contains(&data.dex) =>
            {
                state
                    .open_orders
                    .insert(data.dex.clone(), data.orders.clone());
            }
            HyperliquidWsMessage::AllDexsClearinghouseState { data }
                if request.user.eq_ignore_ascii_case(&data.user) =>
            {
                state.clearinghouse_states = Some(match &data.clearinghouse_states {
                    WsAllDexsClearinghouseStateData::Entries(entries) => {
                        entries.iter().cloned().collect()
                    }
                    WsAllDexsClearinghouseStateData::Map(states) => states.clone(),
                });
            }
            HyperliquidWsMessage::SpotState { data }
                if request.user.eq_ignore_ascii_case(&data.user) =>
            {
                state.spot_state = Some(data.spot_state.clone());
            }
            _ => return,
        }
        drop(state);
        self.inner.notify.notify_waiters();
    }

    pub(crate) async fn wait(
        &self,
        timeout: Duration,
        scope: PrivateStateSnapshotScope,
    ) -> PrivateStateSnapshot {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.notify.notified();
            let snapshot = self.snapshot();
            if snapshot.is_complete(scope) {
                return snapshot;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.snapshot();
            }
        }
    }

    pub(crate) fn snapshot(&self) -> PrivateStateSnapshot {
        let state = self.inner.state.lock().expect(MUTEX_POISONED);
        let expected_dexes = state
            .request
            .as_ref()
            .map_or_else(Vec::new, |request| request.dexes.clone());

        let mut open_orders = Vec::with_capacity(state.open_orders.len());
        let mut missing_open_order_dexes = Vec::new();
        for dex in &expected_dexes {
            if let Some(orders) = state.open_orders.get(dex) {
                open_orders.push((dex.clone(), orders.clone()));
            } else {
                missing_open_order_dexes.push(dex.clone());
            }
        }

        let clearinghouse_snapshot_received = state.clearinghouse_states.is_some();
        let mut clearinghouse_states = state
            .clearinghouse_states
            .as_ref()
            .map(|states| {
                states
                    .iter()
                    .map(|(dex, value)| (dex.clone(), value.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        clearinghouse_states.sort_by(|left, right| left.0.cmp(&right.0));

        let clearinghouse_dexes = clearinghouse_states
            .iter()
            .map(|(dex, _)| dex.as_str())
            .collect::<AHashSet<_>>();
        let missing_clearinghouse_dexes = expected_dexes
            .iter()
            .filter(|dex| !clearinghouse_dexes.contains(dex.as_str()))
            .cloned()
            .collect();

        PrivateStateSnapshot {
            generation: state.generation,
            open_orders,
            clearinghouse_states,
            spot_state: state.spot_state.clone(),
            missing_open_order_dexes,
            missing_clearinghouse_dexes,
            clearinghouse_snapshot_received,
        }
    }

    fn clear_values(state: &mut SnapshotState) {
        state.open_orders.clear();
        state.clearinghouse_states = None;
        state.spot_state = None;
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::websocket::messages::{WsOpenOrdersData, WsSpotStateData};

    #[rstest]
    #[tokio::test]
    async fn snapshot_is_complete_only_after_every_source_and_dex() {
        let cache = PrivateStateSnapshotCache::default();
        let dexes = vec![String::new(), "xyz".to_string()];
        assert_eq!(cache.configure("0xabc", &dexes).len(), 4);

        cache.observe(&HyperliquidWsMessage::AllDexsClearinghouseState {
            data: super::super::messages::WsAllDexsClearinghouseState {
                user: "0xAbC".to_string(),
                clearinghouse_states: WsAllDexsClearinghouseStateData::Entries(vec![
                    (String::new(), json!({"assetPositions": []})),
                    ("xyz".to_string(), json!({"assetPositions": []})),
                ]),
            },
        });
        cache.observe(&HyperliquidWsMessage::SpotState {
            data: WsSpotStateData {
                user: "0xabc".to_string(),
                spot_state: json!({"balances": []}),
            },
        });
        cache.observe(&HyperliquidWsMessage::OpenOrders {
            data: WsOpenOrdersData {
                dex: String::new(),
                user: "0xabc".to_string(),
                orders: vec![],
            },
        });

        let partial = cache.snapshot();
        assert!(!partial.is_complete(PrivateStateSnapshotScope::All));
        assert_eq!(partial.missing_open_order_dexes, ["xyz"]);

        cache.observe(&HyperliquidWsMessage::OpenOrders {
            data: WsOpenOrdersData {
                dex: "xyz".to_string(),
                user: "0xabc".to_string(),
                orders: vec![],
            },
        });
        assert!(
            cache
                .wait(Duration::from_millis(10), PrivateStateSnapshotScope::All)
                .await
                .is_complete(PrivateStateSnapshotScope::All)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn scoped_wait_does_not_depend_on_unrelated_sources() {
        let cache = PrivateStateSnapshotCache::default();
        cache.configure("0xabc", &[String::new()]);
        cache.observe(&HyperliquidWsMessage::OpenOrders {
            data: WsOpenOrdersData {
                dex: String::new(),
                user: "0xabc".to_string(),
                orders: vec![],
            },
        });

        let snapshot = cache
            .wait(
                Duration::from_millis(10),
                PrivateStateSnapshotScope::OpenOrders,
            )
            .await;
        assert!(snapshot.is_complete(PrivateStateSnapshotScope::OpenOrders));
        assert!(!snapshot.is_complete(PrivateStateSnapshotScope::Positions));
    }

    #[rstest]
    fn reconnect_invalidates_values_but_preserves_subscription_scope() {
        let cache = PrivateStateSnapshotCache::default();
        let dexes = vec![String::new()];
        cache.configure("0xabc", &dexes);
        cache.observe(&HyperliquidWsMessage::OpenOrders {
            data: WsOpenOrdersData {
                dex: String::new(),
                user: "0xabc".to_string(),
                orders: vec![],
            },
        });
        let generation = cache.snapshot().generation;

        cache.invalidate_generation();
        let snapshot = cache.snapshot();
        assert!(snapshot.generation > generation);
        assert_eq!(snapshot.missing_open_order_dexes, [""]);
        assert!(cache.configure("0xabc", &dexes).is_empty());
    }

    #[rstest]
    fn expanded_dex_scope_is_incomplete_until_the_new_snapshot_arrives() {
        let cache = PrivateStateSnapshotCache::default();
        cache.configure("0xabc", &[String::new()]);

        let added = cache.configure("0xabc", &[String::new(), "xyz".to_string()]);
        assert_eq!(
            added,
            [SubscriptionRequest::OpenOrders {
                user: "0xabc".to_string(),
                dex: "xyz".to_string(),
            }]
        );
        assert_eq!(cache.snapshot().missing_open_order_dexes, ["", "xyz"]);
    }
}
