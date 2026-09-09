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
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use crate::{
    common::enums::HyperliquidInfoRequestType,
    http::{
        models::HyperliquidExecAction,
        query::{ExchangeAction, ExchangeActionParams, InfoRequest},
    },
};

#[derive(Debug)]
pub struct WeightedLimiter {
    capacity: f64,       // maximum burst across both lanes
    refill_per_sec: f64, // shared configured weight per second
    state: tokio::sync::Mutex<State>,
    reserve: f64,
    queue: tokio::sync::Semaphore,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    protective_tokens: f64,
    last_refill: Instant,
    cooldown: Instant,
}

impl WeightedLimiter {
    pub fn per_minute(capacity: u32) -> Self {
        Self::with_burst(capacity, capacity, 0)
    }

    pub fn with_burst(per_minute: u32, burst: u32, reserve: u32) -> Self {
        assert!(per_minute > 0 && burst > reserve);
        let cap = burst as f64;
        Self {
            capacity: cap,
            refill_per_sec: per_minute as f64 / 60.0,
            reserve: reserve as f64,
            queue: tokio::sync::Semaphore::new(64),
            state: tokio::sync::Mutex::new(State {
                tokens: cap - reserve as f64,
                protective_tokens: reserve as f64,
                last_refill: Instant::now(),
                cooldown: Instant::now(),
            }),
        }
    }

    /// Acquire `weight` tokens, sleeping until available.
    pub async fn acquire(&self, weight: u32) {
        self.acquire_inner(weight, false).await;
    }

    /// Bound admission wait and pending readers. Signed actions have reserved
    /// tokens and do not queue behind catalog/history reads.
    pub async fn acquire_bounded(&self, weight: u32, action: bool) -> bool {
        let _permit = if action {
            None
        } else {
            match self.queue.try_acquire() {
                Ok(p) => Some(p),
                Err(_) => return false,
            }
        };
        tokio::time::timeout(Duration::from_secs(10), self.acquire_inner(weight, action))
            .await
            .is_ok()
    }

    pub async fn cool_down(&self, delay: Duration) {
        let mut state = self.state.lock().await;
        state.cooldown = state.cooldown.max(Instant::now() + delay);
    }

    async fn acquire_inner(&self, weight: u32, action: bool) {
        // Large requests pay in chunks; never wait for more than bucket capacity.
        let available = self.capacity - if action { 0.0 } else { self.reserve };
        let mut remaining = weight as f64;
        while remaining > 0.0 {
            let need = remaining.min(available);
            self.acquire_chunk(need, action).await;
            remaining -= need;
        }
    }

    async fn acquire_chunk(&self, need: f64, action: bool) {
        loop {
            let mut st = self.state.lock().await;
            self.refill_locked(&mut st);

            let cooldown = st.cooldown.saturating_duration_since(Instant::now());
            let available = if action {
                st.tokens.max(0.0) + st.protective_tokens
            } else {
                st.tokens
            };
            if available >= need && cooldown.is_zero() {
                let ordinary = if action {
                    need.min(st.tokens.max(0.0))
                } else {
                    need
                };
                st.tokens -= ordinary;
                st.protective_tokens -= need - ordinary;
                return;
            }
            let deficit = (need - available).max(0.0);
            let rate = if action {
                self.refill_per_sec
            } else {
                self.refill_per_sec * (1.0 - self.reserve / self.capacity)
            };
            // Recheck when either lane refills; ordinary response debt must
            // never delay an action which fits the independent reserve.
            let secs = (deficit / rate).min(1.0);
            drop(st);
            tokio::time::sleep(Duration::from_secs_f64(secs.max(0.01)).max(cooldown)).await;
        }
    }

    /// Post-response debit for per-item adders, retaining debt until refill.
    pub async fn debit_extra(&self, extra: u32) {
        if extra == 0 {
            return;
        }
        let mut st = self.state.lock().await;
        self.refill_locked(&mut st);
        st.tokens -= extra as f64; // Keep response surcharge debt; never forgive it.
    }

    pub async fn snapshot(&self) -> RateLimitSnapshot {
        let mut st = self.state.lock().await;
        self.refill_locked(&mut st);
        RateLimitSnapshot {
            capacity: self.capacity as u32,
            tokens: (st.tokens + st.protective_tokens).max(0.0) as u32,
        }
    }

    fn refill_locked(&self, st: &mut State) {
        let now = Instant::now();
        let dt = now.duration_since(st.last_refill).as_secs_f64();
        if dt > 0.0 {
            // Split, rather than duplicate, the configured refill budget.
            // Reserved capacity cannot be spent by read-response surcharges.
            let protective_rate = self.refill_per_sec * self.reserve / self.capacity;
            st.tokens = (st.tokens + dt * (self.refill_per_sec - protective_rate))
                .min(self.capacity - self.reserve);
            st.protective_tokens = (st.protective_tokens + dt * protective_rate).min(self.reserve);
            st.last_refill = now;
        }
    }
}

/// One IP-budget allocation per cell process and venue environment. Configure
/// before constructing clients. This does not coordinate different processes.
pub fn configure_process_budget(
    environment: crate::common::enums::HyperliquidEnvironment,
    per_minute: u32,
    burst: u32,
    reserve: u32,
) -> Result<(), &'static str> {
    budget_slot(environment)
        .set(std::sync::Arc::new(WeightedLimiter::with_burst(
            per_minute, burst, reserve,
        )))
        .map_err(|_| "Hyperliquid process budget was already initialized")
}

fn budget_slot(
    environment: crate::common::enums::HyperliquidEnvironment,
) -> &'static std::sync::OnceLock<std::sync::Arc<WeightedLimiter>> {
    static MAINNET: std::sync::OnceLock<std::sync::Arc<WeightedLimiter>> =
        std::sync::OnceLock::new();
    static TESTNET: std::sync::OnceLock<std::sync::Arc<WeightedLimiter>> =
        std::sync::OnceLock::new();
    if environment == crate::common::enums::HyperliquidEnvironment::Mainnet {
        &MAINNET
    } else {
        &TESTNET
    }
}

pub fn process_budget(
    environment: crate::common::enums::HyperliquidEnvironment,
) -> std::sync::Arc<WeightedLimiter> {
    budget_slot(environment)
        .get_or_init(|| std::sync::Arc::new(WeightedLimiter::per_minute(1200)))
        .clone()
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitSnapshot {
    pub capacity: u32,
    pub tokens: u32,
}

pub fn backoff_full_jitter(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let mut hasher = DefaultHasher::new();
    attempt.hash(&mut hasher);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    nanos.hash(&mut hasher);
    let hash = hasher.finish();

    let max = (base.as_millis() as u64)
        .saturating_mul(1u64 << attempt.min(16))
        .min(cap.as_millis() as u64)
        .max(base.as_millis() as u64);

    // Floor at 1ms to prevent zero-duration backoff
    Duration::from_millis((hash % max).max(1))
}

/// Classify Info requests into weight classes based on request type.
pub fn info_base_weight(req: &InfoRequest) -> u32 {
    match req.request_type {
        HyperliquidInfoRequestType::L2Book
        | HyperliquidInfoRequestType::AllMids
        | HyperliquidInfoRequestType::ClearinghouseState
        | HyperliquidInfoRequestType::OrderStatus
        | HyperliquidInfoRequestType::SpotClearinghouseState
        | HyperliquidInfoRequestType::ExchangeStatus => 2,
        HyperliquidInfoRequestType::UserRole => 60,
        _ => 20,
    }
}

/// Extra weight for heavy Info endpoints: +1 per 20 (most), +1 per 60 for candleSnapshot.
/// We count the largest array in the response (robust to schema variants).
pub fn info_extra_weight(req: &InfoRequest, json: &Value) -> u32 {
    let items = match json {
        Value::Array(a) => a.len(),
        Value::Object(m) => m
            .values()
            .filter_map(|v| v.as_array().map(|a| a.len()))
            .max()
            .unwrap_or(0),
        _ => 0,
    };

    let unit = match req.request_type {
        HyperliquidInfoRequestType::CandleSnapshot => 60usize,
        HyperliquidInfoRequestType::RecentTrades
        | HyperliquidInfoRequestType::HistoricalOrders
        | HyperliquidInfoRequestType::UserFills
        | HyperliquidInfoRequestType::UserFillsByTime
        | HyperliquidInfoRequestType::FundingHistory
        | HyperliquidInfoRequestType::UserFunding
        | HyperliquidInfoRequestType::NonUserFundingUpdates
        | HyperliquidInfoRequestType::TwapHistory
        | HyperliquidInfoRequestType::UserTwapSliceFills
        | HyperliquidInfoRequestType::UserTwapSliceFillsByTime
        | HyperliquidInfoRequestType::DelegatorHistory
        | HyperliquidInfoRequestType::DelegatorRewards
        | HyperliquidInfoRequestType::ValidatorStats => 20usize,
        _ => return 0,
    };
    (items / unit) as u32
}

/// Exchange: 1 + floor(batch_len / 40)
pub fn exchange_weight(action: &ExchangeAction) -> u32 {
    // Extract batch size from typed params
    let batch_size = match &action.params {
        ExchangeActionParams::Order(params) => params.orders.len(),
        ExchangeActionParams::Cancel(params) => params.cancels.len(),
        ExchangeActionParams::Modify(_) => {
            // Modify is for a single order
            1
        }
        ExchangeActionParams::UpdateLeverage(_) | ExchangeActionParams::UpdateIsolatedMargin(_) => {
            0
        }
    };
    1 + (batch_size as u32 / 40)
}

/// Exchange weight for the canonical typed execution action model.
pub fn exec_action_weight(action: &HyperliquidExecAction) -> u32 {
    let batch_size = match action {
        HyperliquidExecAction::Order { orders, .. } => orders.len(),
        HyperliquidExecAction::Cancel { cancels, .. } => cancels.len(),
        HyperliquidExecAction::CancelByCloid { cancels, .. } => cancels.len(),
        HyperliquidExecAction::Modify { .. } => 1,
        HyperliquidExecAction::BatchModify { modifies } => modifies.len(),
        HyperliquidExecAction::UpdateLeverage { .. }
        | HyperliquidExecAction::UpdateIsolatedMargin { .. }
        | HyperliquidExecAction::ScheduleCancel { .. }
        | HyperliquidExecAction::UsdClassTransfer { .. }
        | HyperliquidExecAction::UserOutcome { .. }
        | HyperliquidExecAction::TwapPlace { .. }
        | HyperliquidExecAction::TwapCancel { .. }
        | HyperliquidExecAction::Noop => 0,
    };
    1 + (batch_size as u32 / 40)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::{
        super::models::{
            Cloid, HyperliquidExecAction, HyperliquidExecCancelByCloidRequest,
            HyperliquidExecCancelOrderRequest, HyperliquidExecGrouping, HyperliquidExecLimitParams,
            HyperliquidExecModifyOrderRequest, HyperliquidExecOrderKind,
            HyperliquidExecPlaceOrderRequest, HyperliquidExecTif,
        },
        *,
    };
    use crate::http::query::{
        CancelParams, ExchangeAction, ExchangeActionParams, ExchangeActionType, OrderParams,
        UpdateLeverageParams,
    };

    fn exec_order() -> HyperliquidExecPlaceOrderRequest {
        HyperliquidExecPlaceOrderRequest {
            asset: 0,
            is_buy: true,
            price: Decimal::new(50000, 0),
            size: Decimal::new(1, 0),
            reduce_only: false,
            kind: HyperliquidExecOrderKind::Limit {
                limit: HyperliquidExecLimitParams {
                    tif: HyperliquidExecTif::Gtc,
                },
            },
            cloid: Some(Cloid::from_hex("0x00000000000000000000000000000000").unwrap()),
        }
    }

    fn exec_modify() -> HyperliquidExecModifyOrderRequest {
        HyperliquidExecModifyOrderRequest {
            oid: 12345.into(),
            order: exec_order(),
        }
    }

    fn exec_cancel_by_cloid() -> HyperliquidExecCancelByCloidRequest {
        HyperliquidExecCancelByCloidRequest {
            asset: 0,
            cloid: Cloid::from_hex("0x00000000000000000000000000000000").unwrap(),
        }
    }

    #[rstest]
    #[case(1, 1)]
    #[case(39, 1)]
    #[case(40, 2)]
    #[case(79, 2)]
    #[case(80, 3)]
    fn test_exchange_weight_order_steps_every_40(
        #[case] array_len: usize,
        #[case] expected_weight: u32,
    ) {
        let orders: Vec<HyperliquidExecPlaceOrderRequest> =
            (0..array_len).map(|_| exec_order()).collect();

        let action = ExchangeAction {
            action_type: ExchangeActionType::Order,
            params: ExchangeActionParams::Order(OrderParams {
                orders,
                grouping: HyperliquidExecGrouping::Na,
                builder: None,
            }),
        };
        assert_eq!(exchange_weight(&action), expected_weight);
    }

    #[rstest]
    #[case(1, 1)]
    #[case(39, 1)]
    #[case(40, 2)]
    #[case(79, 2)]
    #[case(80, 3)]
    fn test_exec_action_weight_order_steps_every_40(
        #[case] array_len: usize,
        #[case] expected_weight: u32,
    ) {
        let action = HyperliquidExecAction::Order {
            orders: (0..array_len).map(|_| exec_order()).collect(),
            grouping: HyperliquidExecGrouping::Na,
            builder: None,
        };

        assert_eq!(exec_action_weight(&action), expected_weight);
    }

    #[rstest]
    #[case(1, 1)]
    #[case(39, 1)]
    #[case(40, 2)]
    #[case(79, 2)]
    #[case(80, 3)]
    fn test_exec_action_weight_cancel_by_oid_steps_every_40(
        #[case] array_len: usize,
        #[case] expected_weight: u32,
    ) {
        let action = HyperliquidExecAction::Cancel {
            cancels: (0..array_len)
                .map(|i| HyperliquidExecCancelOrderRequest {
                    asset: 0,
                    oid: i as u64,
                })
                .collect(),
            fast: None,
        };

        assert_eq!(exec_action_weight(&action), expected_weight);
    }

    #[rstest]
    #[case(1, 1)]
    #[case(39, 1)]
    #[case(40, 2)]
    #[case(79, 2)]
    #[case(80, 3)]
    fn test_exec_action_weight_cancel_by_cloid_steps_every_40(
        #[case] array_len: usize,
        #[case] expected_weight: u32,
    ) {
        let action = HyperliquidExecAction::CancelByCloid {
            cancels: (0..array_len).map(|_| exec_cancel_by_cloid()).collect(),
            fast: None,
        };

        assert_eq!(exec_action_weight(&action), expected_weight);
    }

    #[rstest]
    #[case(1, 1)]
    #[case(39, 1)]
    #[case(40, 2)]
    #[case(79, 2)]
    #[case(80, 3)]
    fn test_exec_action_weight_batch_modify_steps_every_40(
        #[case] array_len: usize,
        #[case] expected_weight: u32,
    ) {
        let action = HyperliquidExecAction::BatchModify {
            modifies: (0..array_len).map(|_| exec_modify()).collect(),
        };

        assert_eq!(exec_action_weight(&action), expected_weight);
    }

    #[rstest]
    fn test_exec_action_weight_modify() {
        let action = HyperliquidExecAction::Modify {
            modify: exec_modify(),
        };

        assert_eq!(exec_action_weight(&action), 1);
    }

    #[rstest]
    fn test_exec_action_weight_non_batch_action() {
        let action = HyperliquidExecAction::UpdateLeverage {
            asset: 1,
            is_cross: true,
            leverage: 10,
        };

        assert_eq!(exec_action_weight(&action), 1);
    }

    #[rstest]
    fn test_exchange_weight_cancel() {
        let cancels: Vec<HyperliquidExecCancelByCloidRequest> =
            (0..40).map(|_| exec_cancel_by_cloid()).collect();

        let action = ExchangeAction {
            action_type: ExchangeActionType::Cancel,
            params: ExchangeActionParams::Cancel(CancelParams {
                cancels,
                fast: None,
            }),
        };
        assert_eq!(exchange_weight(&action), 2);
    }

    #[rstest]
    fn test_exchange_weight_non_batch_action() {
        let update_leverage = ExchangeAction {
            action_type: ExchangeActionType::UpdateLeverage,
            params: ExchangeActionParams::UpdateLeverage(UpdateLeverageParams {
                asset: 1,
                is_cross: true,
                leverage: 10,
            }),
        };
        assert_eq!(exchange_weight(&update_leverage), 1);
    }

    #[tokio::test]
    async fn test_limiter_roughly_caps_to_capacity() {
        let limiter = WeightedLimiter::per_minute(1200);

        // Consume ~1200 in quick succession
        for _ in 0..60 {
            limiter.acquire(20).await; // 60 * 20 = 1200
        }

        // The next acquire should take time for tokens to refill
        let t0 = std::time::Instant::now();
        limiter.acquire(20).await;
        let elapsed = t0.elapsed();

        // Should take at least some time to refill (allow some jitter/timing variance)
        assert!(
            elapsed.as_millis() >= 500,
            "Expected significant delay, was {}ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_limiter_debit_extra_works() {
        let limiter = WeightedLimiter::per_minute(100);

        // Start with full bucket
        let snapshot = limiter.snapshot().await;
        assert_eq!(snapshot.capacity, 100);
        assert_eq!(snapshot.tokens, 100);

        // Acquire some tokens
        limiter.acquire(30).await;
        let snapshot = limiter.snapshot().await;
        assert_eq!(snapshot.tokens, 70);

        // Debit extra
        limiter.debit_extra(20).await;
        let snapshot = limiter.snapshot().await;
        assert_eq!(snapshot.tokens, 50);

        // Debit more than available (should clamp to 0)
        limiter.debit_extra(100).await;
        let snapshot = limiter.snapshot().await;
        assert_eq!(snapshot.tokens, 0);
    }

    #[rstest]
    #[case(0, 100)]
    #[case(1, 200)]
    #[case(2, 400)]
    fn test_backoff_full_jitter_increases(#[case] attempt: u32, #[case] max_expected_ms: u64) {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(5);

        let delay = backoff_full_jitter(attempt, base, cap);

        assert!(delay.as_millis() >= 1);
        assert!(delay.as_millis() <= max_expected_ms as u128);
    }

    #[rstest]
    fn test_backoff_full_jitter_respects_cap() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(5);

        let delay_high = backoff_full_jitter(10, base, cap);
        assert!(delay_high.as_millis() <= cap.as_millis());
    }
}
