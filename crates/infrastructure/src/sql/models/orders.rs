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

use std::str::FromStr;

use indexmap::IndexMap;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{
        ContingencyType, LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce, TriggerType,
    },
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied, OrderEmulated,
        OrderEventAny, OrderExpired, OrderFillVoided, OrderFilled, OrderInitialized,
        OrderModifyRejected, OrderPendingCancel, OrderPendingUpdate, OrderRejected, OrderReleased,
        OrderSnapshot, OrderSubmitted, OrderTriggered, OrderUpdated,
    },
    identifiers::{
        AccountId, ClientOrderId, ExecAlgorithmId, InstrumentId, OrderListId, PositionId,
        StrategyId, TradeId, TraderId, VenueOrderId,
    },
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use sqlx::{FromRow, Row, postgres::PgRow};
use ustr::Ustr;

use crate::sql::models::enums::TrailingOffsetTypeModel;

#[derive(Debug)]
pub struct OrderEventAnyModel(pub OrderEventAny);

#[derive(Debug)]
pub struct OrderAcceptedModel(pub OrderAccepted);

#[derive(Debug)]
pub struct OrderCancelRejectedModel(pub OrderCancelRejected);

#[derive(Debug)]
pub struct OrderCanceledModel(pub OrderCanceled);

#[derive(Debug)]
pub struct OrderDeniedModel(pub OrderDenied);

#[derive(Debug)]
pub struct OrderEmulatedModel(pub OrderEmulated);

#[derive(Debug)]
pub struct OrderExpiredModel(pub OrderExpired);

#[derive(Debug)]
pub struct OrderFillVoidedModel(pub OrderFillVoided);

#[derive(Debug)]
pub struct OrderFilledModel(pub OrderFilled);

#[derive(Debug)]
pub struct OrderInitializedModel(pub OrderInitialized);

#[derive(Debug)]
pub struct OrderModifyRejectedModel(pub OrderModifyRejected);

#[derive(Debug)]
pub struct OrderPendingCancelModel(pub OrderPendingCancel);

#[derive(Debug)]
pub struct OrderPendingUpdateModel(pub OrderPendingUpdate);

#[derive(Debug)]
pub struct OrderRejectedModel(pub OrderRejected);

#[derive(Debug)]
pub struct OrderReleasedModel(pub OrderReleased);

#[derive(Debug)]
pub struct OrderSubmittedModel(pub OrderSubmitted);

#[derive(Debug)]
pub struct OrderTriggeredModel(pub OrderTriggered);

#[derive(Debug)]
pub struct OrderUpdatedModel(pub OrderUpdated);

#[derive(Debug)]
pub struct OrderSnapshotModel(pub OrderSnapshot);

struct CommonOrderEventFields {
    trader_id: TraderId,
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    event_id: UUID4,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

impl CommonOrderEventFields {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            trader_id: row.try_get::<&str, _>("trader_id").map(TraderId::from)?,
            strategy_id: row
                .try_get::<&str, _>("strategy_id")
                .map(StrategyId::from)?,
            instrument_id: row
                .try_get::<&str, _>("instrument_id")
                .map(InstrumentId::from)?,
            client_order_id: row
                .try_get::<&str, _>("client_order_id")
                .map(ClientOrderId::from)?,
            event_id: row.try_get::<&str, _>("id").map(UUID4::from)?,
            ts_event: row.try_get::<&str, _>("ts_event").map(UnixNanos::from)?,
            ts_init: row.try_get::<&str, _>("ts_init").map(UnixNanos::from)?,
        })
    }
}

fn optional_execution_ids(
    row: &PgRow,
) -> Result<(Option<VenueOrderId>, Option<AccountId>), sqlx::Error> {
    let venue_order_id = row
        .try_get::<Option<&str>, _>("venue_order_id")?
        .map(VenueOrderId::from);
    let account_id = row
        .try_get::<Option<&str>, _>("account_id")?
        .map(AccountId::from);
    Ok((venue_order_id, account_id))
}

fn reconciliation(row: &PgRow) -> Result<bool, sqlx::Error> {
    Ok(row
        .try_get::<Option<bool>, _>("reconciliation")?
        .unwrap_or(false))
}

/// The `kind` column values of the order event rows: one for each [`OrderEventAny`] variant.
///
/// The row decoder matches on this enum with no wildcard arm. A kind added here, for a new
/// event variant, fails to build until the decoder has an arm for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderEventKind {
    Accepted,
    CancelRejected,
    Canceled,
    Denied,
    Emulated,
    Expired,
    FillVoided,
    Filled,
    Initialized,
    ModifyRejected,
    PendingCancel,
    PendingUpdate,
    Rejected,
    Released,
    Submitted,
    Triggered,
    Updated,
}

impl OrderEventKind {
    /// Returns the kind that a row names, which is the `type_name` of its event.
    fn parse(kind: &str) -> Option<Self> {
        Some(match kind {
            "OrderAccepted" => Self::Accepted,
            "OrderCancelRejected" => Self::CancelRejected,
            "OrderCanceled" => Self::Canceled,
            "OrderDenied" => Self::Denied,
            "OrderEmulated" => Self::Emulated,
            "OrderExpired" => Self::Expired,
            "OrderFillVoided" => Self::FillVoided,
            "OrderFilled" => Self::Filled,
            "OrderInitialized" => Self::Initialized,
            "OrderModifyRejected" => Self::ModifyRejected,
            "OrderPendingCancel" => Self::PendingCancel,
            "OrderPendingUpdate" => Self::PendingUpdate,
            "OrderRejected" => Self::Rejected,
            "OrderReleased" => Self::Released,
            "OrderSubmitted" => Self::Submitted,
            "OrderTriggered" => Self::Triggered,
            "OrderUpdated" => Self::Updated,
            _ => return None,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for OrderEventAnyModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let kind = row.get::<String, _>("kind");
        let Some(kind) = OrderEventKind::parse(&kind) else {
            return Err(sqlx::Error::Decode(
                format!("Unknown order event kind: {kind} in Postgres transformation").into(),
            ));
        };

        let event = match kind {
            OrderEventKind::Accepted => {
                OrderEventAny::Accepted(OrderAcceptedModel::from_row(row)?.0)
            }
            OrderEventKind::CancelRejected => {
                OrderEventAny::CancelRejected(OrderCancelRejectedModel::from_row(row)?.0)
            }
            OrderEventKind::Canceled => {
                OrderEventAny::Canceled(OrderCanceledModel::from_row(row)?.0)
            }
            OrderEventKind::Denied => OrderEventAny::Denied(OrderDeniedModel::from_row(row)?.0),
            OrderEventKind::Emulated => {
                OrderEventAny::Emulated(OrderEmulatedModel::from_row(row)?.0)
            }
            OrderEventKind::Expired => OrderEventAny::Expired(OrderExpiredModel::from_row(row)?.0),
            OrderEventKind::FillVoided => {
                OrderEventAny::FillVoided(OrderFillVoidedModel::from_row(row)?.0)
            }
            OrderEventKind::Filled => OrderEventAny::Filled(OrderFilledModel::from_row(row)?.0),
            OrderEventKind::Initialized => {
                OrderEventAny::Initialized(OrderInitializedModel::from_row(row)?.0)
            }
            OrderEventKind::ModifyRejected => {
                OrderEventAny::ModifyRejected(OrderModifyRejectedModel::from_row(row)?.0)
            }
            OrderEventKind::PendingCancel => {
                OrderEventAny::PendingCancel(OrderPendingCancelModel::from_row(row)?.0)
            }
            OrderEventKind::PendingUpdate => {
                OrderEventAny::PendingUpdate(OrderPendingUpdateModel::from_row(row)?.0)
            }
            OrderEventKind::Rejected => {
                OrderEventAny::Rejected(OrderRejectedModel::from_row(row)?.0)
            }
            OrderEventKind::Released => {
                OrderEventAny::Released(OrderReleasedModel::from_row(row)?.0)
            }
            OrderEventKind::Submitted => {
                OrderEventAny::Submitted(OrderSubmittedModel::from_row(row)?.0)
            }
            OrderEventKind::Triggered => {
                OrderEventAny::Triggered(OrderTriggeredModel::from_row(row)?.0)
            }
            OrderEventKind::Updated => OrderEventAny::Updated(OrderUpdatedModel::from_row(row)?.0),
        };
        Ok(Self(event))
    }
}

fn parse_text_column<T>(row: &PgRow, column: &str) -> Result<T, sqlx::Error>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.try_get::<&str, _>(column)?
        .parse::<T>()
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

impl<'r> FromRow<'r, PgRow> for OrderInitializedModel {
    #[expect(
        clippy::too_many_lines,
        reason = "SQL row mapping mirrors the full order initialized event constructor"
    )]
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let order_type = row
            .try_get::<&str, _>("order_type")
            .map(|x| OrderType::from_str(x).unwrap())?;
        let order_side = row
            .try_get::<&str, _>("order_side")
            .map(|x| OrderSide::from_str(x).unwrap())?;
        let quantity = row.try_get::<&str, _>("quantity").map(Quantity::from)?;
        let time_in_force = row
            .try_get::<&str, _>("time_in_force")
            .map(|x| TimeInForce::from_str(x).unwrap())?;
        let post_only = row.try_get::<bool, _>("post_only")?;
        let reduce_only = row.try_get::<bool, _>("reduce_only")?;
        let quote_quantity = row.try_get::<bool, _>("quote_quantity")?;
        let reconciliation = row.try_get::<bool, _>("reconciliation")?;
        let ts_event = row.try_get::<String, _>("ts_event").map(UnixNanos::from)?;
        let ts_init = row.try_get::<String, _>("ts_init").map(UnixNanos::from)?;
        let price = row
            .try_get::<Option<&str>, _>("price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let activation_price = row
            .try_get::<Option<&str>, _>("activation_price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let trigger_price = row
            .try_get::<Option<&str>, _>("trigger_price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let trigger_type = row
            .try_get::<Option<&str>, _>("trigger_type")
            .ok()
            .and_then(|x| x.map(|x| TriggerType::from_str(x).unwrap()));
        let limit_offset = row
            .try_get::<Option<&str>, _>("limit_offset")
            .ok()
            .and_then(|x| x.and_then(|s| Decimal::from_str(s).ok()));
        let trailing_offset = row
            .try_get::<Option<&str>, _>("trailing_offset")
            .ok()
            .and_then(|x| x.and_then(|s| Decimal::from_str(s).ok()));
        let trailing_offset_type = row
            .try_get::<Option<TrailingOffsetTypeModel>, _>("trailing_offset_type")
            .ok()
            .and_then(|x| x.map(|x| x.0));
        let expire_time = row
            .try_get::<Option<&str>, _>("expire_time")
            .ok()
            .and_then(|x| x.map(UnixNanos::from));
        let display_qty = row
            .try_get::<Option<&str>, _>("display_qty")
            .ok()
            .and_then(|x| x.map(Quantity::from));
        let emulation_trigger = row
            .try_get::<Option<&str>, _>("emulation_trigger")
            .ok()
            .and_then(|x| x.map(|x| TriggerType::from_str(x).unwrap()));
        let trigger_instrument_id = row
            .try_get::<Option<&str>, _>("trigger_instrument_id")
            .ok()
            .and_then(|x| x.map(InstrumentId::from));
        let contingency_type = row
            .try_get::<Option<&str>, _>("contingency_type")
            .ok()
            .and_then(|x| x.map(|x| ContingencyType::from_str(x).unwrap()));
        let order_list_id = row
            .try_get::<Option<&str>, _>("order_list_id")
            .ok()
            .and_then(|x| x.map(OrderListId::from));
        let linked_order_ids = row
            .try_get::<Vec<String>, _>("linked_order_ids")
            .ok()
            .map(|x| x.iter().map(|x| ClientOrderId::from(x.as_str())).collect());
        let parent_order_id = row
            .try_get::<Option<&str>, _>("parent_order_id")
            .ok()
            .and_then(|x| x.map(ClientOrderId::from));
        let exec_algorithm_id = row
            .try_get::<Option<&str>, _>("exec_algorithm_id")
            .ok()
            .and_then(|x| x.map(ExecAlgorithmId::from));
        let exec_algorithm_params: Option<IndexMap<Ustr, Ustr>> = row
            .try_get::<Option<serde_json::Value>, _>("exec_algorithm_params")
            .ok()
            .and_then(|x| x.map(|x| serde_json::from_value::<IndexMap<String, String>>(x).unwrap()))
            .map(|x| {
                x.into_iter()
                    .map(|(k, v)| (Ustr::from(k.as_str()), Ustr::from(v.as_str())))
                    .collect()
            });
        let exec_spawn_id = row
            .try_get::<Option<&str>, _>("exec_spawn_id")
            .ok()
            .and_then(|x| x.map(ClientOrderId::from));
        let tags: Option<Vec<Ustr>> = row
            .try_get::<Option<serde_json::Value>, _>("tags")
            .ok()
            .and_then(|x| x.map(|x| serde_json::from_value::<Vec<String>>(x).unwrap()))
            .map(|x| x.into_iter().map(|x| Ustr::from(x.as_str())).collect());
        let order_event = OrderInitialized::new_checked(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            order_side,
            order_type,
            quantity,
            time_in_force,
            post_only,
            reduce_only,
            quote_quantity,
            reconciliation,
            event_id,
            ts_event,
            ts_init,
            price,
            activation_price,
            trigger_price,
            trigger_type,
            limit_offset,
            trailing_offset,
            trailing_offset_type,
            expire_time,
            display_qty,
            emulation_trigger,
            trigger_instrument_id,
            contingency_type,
            order_list_id,
            linked_order_ids,
            parent_order_id,
            exec_algorithm_id,
            exec_algorithm_params,
            exec_spawn_id,
            tags,
        )
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderAcceptedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let venue_order_id = row
            .try_get::<&str, _>("venue_order_id")
            .map(VenueOrderId::from)?;
        let account_id = row.try_get::<&str, _>("account_id").map(AccountId::from)?;
        let ts_event = row.try_get::<&str, _>("ts_event").map(UnixNanos::from)?;
        let ts_init = row.try_get::<&str, _>("ts_init").map(UnixNanos::from)?;
        let order_event = OrderAccepted::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            account_id,
            event_id,
            ts_event,
            ts_init,
            reconciliation(row)?,
        );
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderCancelRejectedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let reason = row.try_get::<&str, _>("reason").map(Ustr::from)?;
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let ts_event = row.try_get::<&str, _>("ts_event").map(UnixNanos::from)?;
        let ts_init = row.try_get::<&str, _>("ts_init").map(UnixNanos::from)?;
        let reconciliation = row.try_get::<bool, _>("reconciliation")?;
        let venue_order_id = row
            .try_get::<Option<&str>, _>("venue_order_id")?
            .map(Into::into);
        let account_id = row
            .try_get::<Option<&str>, _>("account_id")?
            .map(Into::into);
        let order_event = OrderCancelRejected::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            reason,
            event_id,
            ts_event,
            ts_init,
            reconciliation,
            venue_order_id,
            account_id,
        );
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderCanceledModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        Ok(Self(OrderCanceled::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
            account_id,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderDeniedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let reason = row.try_get::<&str, _>("reason").map(Ustr::from)?;
        Ok(Self(OrderDenied::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            reason,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderEmulatedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        Ok(Self(OrderEmulated::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderExpiredModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        Ok(Self(OrderExpired::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
            account_id,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderFilledModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let venue_order_id = row
            .try_get::<&str, _>("venue_order_id")
            .map(VenueOrderId::from)?;
        let account_id = row.try_get::<&str, _>("account_id").map(AccountId::from)?;
        let trade_id = row.try_get::<&str, _>("trade_id").map(TradeId::from)?;
        let order_side = row
            .try_get::<&str, _>("order_side")
            .map(|x| OrderSide::from_str(x).unwrap())?;
        let order_type = row
            .try_get::<&str, _>("order_type")
            .map(|x| OrderType::from_str(x).unwrap())?;
        let last_px = row.try_get::<&str, _>("last_px").map(Price::from)?;
        let last_qty = row.try_get::<&str, _>("last_qty").map(Quantity::from)?;
        let currency = row.try_get::<&str, _>("currency").map(Currency::from)?;
        let liquidity_side = row
            .try_get::<&str, _>("liquidity_side")
            .map(|x| LiquiditySide::from_str(x).unwrap())?;
        let ts_event = row.try_get::<&str, _>("ts_event").map(UnixNanos::from)?;
        let ts_init = row.try_get::<&str, _>("ts_init").map(UnixNanos::from)?;
        let position_id = row
            .try_get::<Option<&str>, _>("position_id")
            .map(|x| x.map(PositionId::from))?;
        let commission = row
            .try_get::<Option<&str>, _>("commission")
            .map(|x| x.map(|x| Money::from_str(x).unwrap()))?;
        let order_event = OrderFilled::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            account_id,
            trade_id,
            order_side,
            order_type,
            last_qty,
            last_px,
            currency,
            liquidity_side,
            event_id,
            ts_event,
            ts_init,
            reconciliation(row)?,
            position_id,
            commission,
            None,
        );
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderFillVoidedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let venue_order_id = row
            .try_get::<&str, _>("venue_order_id")
            .map(VenueOrderId::from)?;
        let account_id = row.try_get::<&str, _>("account_id").map(AccountId::from)?;
        let trade_id = row.try_get::<&str, _>("trade_id").map(TradeId::from)?;
        let voided_qty = row.try_get::<&str, _>("quantity").map(Quantity::from)?;
        let order_side = parse_text_column::<OrderSide>(row, "order_side")?;
        let order_type = parse_text_column::<OrderType>(row, "order_type")?;
        let last_px = row.try_get::<&str, _>("last_px").map(Price::from)?;
        let currency = row.try_get::<&str, _>("currency").map(Currency::from)?;
        let liquidity_side = parse_text_column::<LiquiditySide>(row, "liquidity_side")?;
        let position_id = row
            .try_get::<Option<&str>, _>("position_id")?
            .map(PositionId::from);
        let commission_voided = row
            .try_get::<Option<&str>, _>("commission")?
            .map(|x| Money::from_str(x).map_err(|e| sqlx::Error::Decode(e.into())))
            .transpose()?;
        let reason = row.try_get::<Option<&str>, _>("reason")?.map(Ustr::from);
        // The writer keeps the correction identity in `exec_spawn_id` and the reopen flag in
        // `reduce_only`. A row written before it kept them has neither. The known case of such
        // a row is a void that reopened its order, and a replay needs that flag to restore the
        // order, so a row without the flag is read as reopened.
        let correction_id = row
            .try_get::<Option<&str>, _>("exec_spawn_id")?
            .map_or_else(Ustr::default, Ustr::from);
        let is_reopened = row
            .try_get::<Option<bool>, _>("reduce_only")?
            .unwrap_or(true);
        Ok(Self(OrderFillVoided::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            venue_order_id,
            account_id,
            correction_id,
            trade_id,
            voided_qty,
            commission_voided,
            order_side,
            order_type,
            last_px,
            currency,
            liquidity_side,
            position_id,
            reason,
            None,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            is_reopened,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderModifyRejectedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let reason = row.try_get::<&str, _>("reason").map(Ustr::from)?;
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let ts_event = row.try_get::<&str, _>("ts_event").map(UnixNanos::from)?;
        let ts_init = row.try_get::<&str, _>("ts_init").map(UnixNanos::from)?;
        let reconciliation = row.try_get::<bool, _>("reconciliation")?;
        let venue_order_id = row
            .try_get::<Option<&str>, _>("venue_order_id")?
            .map(Into::into);
        let account_id = row
            .try_get::<Option<&str>, _>("account_id")?
            .map(Into::into);
        let order_event = OrderModifyRejected::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            reason,
            event_id,
            ts_event,
            ts_init,
            reconciliation,
            venue_order_id,
            account_id,
        );
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderPendingCancelModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        Ok(Self(OrderPendingCancel::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            account_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderPendingUpdateModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        Ok(Self(OrderPendingUpdate::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            account_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderRejectedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let account_id = row.try_get::<&str, _>("account_id").map(AccountId::from)?;
        let reason = row.try_get::<&str, _>("reason").map(Ustr::from)?;
        let due_post_only = row
            .try_get::<Option<bool>, _>("post_only")?
            .unwrap_or(false);
        Ok(Self(OrderRejected::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            account_id,
            reason,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            due_post_only,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderReleasedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let released_price = row
            .try_get::<Option<&str>, _>("price")?
            .map(Price::from)
            .ok_or_else(|| {
                sqlx::Error::Decode(
                    "OrderReleased row is missing its required release price".into(),
                )
            })?;
        Ok(Self(OrderReleased::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            released_price,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderSubmittedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let account_id = row.try_get::<&str, _>("account_id").map(AccountId::from)?;
        let event_id = row.try_get::<&str, _>("id").map(UUID4::from)?;
        let ts_event = row
            .try_get::<String, _>("ts_event")
            .map(|res| UnixNanos::from(res.as_str()))?;
        let ts_init = row
            .try_get::<String, _>("ts_init")
            .map(|res| UnixNanos::from(res.as_str()))?;
        let order_event = OrderSubmitted::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            account_id,
            event_id,
            ts_event,
            ts_init,
        );
        Ok(Self(order_event))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderTriggeredModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        Ok(Self(OrderTriggered::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
            account_id,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderUpdatedModel {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let fields = CommonOrderEventFields::from_row(row)?;
        let (venue_order_id, account_id) = optional_execution_ids(row)?;
        let quantity = row.try_get::<&str, _>("quantity").map(Quantity::from)?;
        let price = row.try_get::<Option<&str>, _>("price")?.map(Price::from);
        let trigger_price = row
            .try_get::<Option<&str>, _>("trigger_price")?
            .map(Price::from);
        let protection_price = row
            .try_get::<Option<&str>, _>("activation_price")?
            .map(Price::from);
        let is_quote_quantity = row
            .try_get::<Option<bool>, _>("quote_quantity")?
            .unwrap_or(false);
        Ok(Self(OrderUpdated::new(
            fields.trader_id,
            fields.strategy_id,
            fields.instrument_id,
            fields.client_order_id,
            quantity,
            fields.event_id,
            fields.ts_event,
            fields.ts_init,
            reconciliation(row)?,
            venue_order_id,
            account_id,
            price,
            trigger_price,
            protection_price,
            is_quote_quantity,
        )))
    }
}

impl<'r> FromRow<'r, PgRow> for OrderSnapshotModel {
    #[expect(
        clippy::too_many_lines,
        reason = "SQL row mapping mirrors the full order snapshot constructor"
    )]
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let trader_id = row.try_get::<&str, _>("trader_id").map(TraderId::from)?;
        let strategy_id = row
            .try_get::<&str, _>("strategy_id")
            .map(StrategyId::from)?;
        let instrument_id = row
            .try_get::<&str, _>("instrument_id")
            .map(InstrumentId::from)?;
        let client_order_id = row
            .try_get::<&str, _>("client_order_id")
            .map(ClientOrderId::from)?;
        let venue_order_id = row
            .try_get::<Option<&str>, _>("venue_order_id")
            .ok()
            .and_then(|x| x.map(VenueOrderId::from));
        let position_id = row
            .try_get::<Option<&str>, _>("position_id")
            .ok()
            .and_then(|x| x.map(PositionId::from));
        let account_id = row
            .try_get::<Option<&str>, _>("account_id")
            .ok()
            .and_then(|x| x.map(AccountId::from));
        let last_trade_id = row
            .try_get::<Option<&str>, _>("last_trade_id")
            .ok()
            .and_then(|x| x.map(TradeId::from));
        let order_type = row
            .try_get::<&str, _>("order_type")
            .map(|x| OrderType::from_str(x).expect("Invalid `OrderType`"))?;
        let order_side = row
            .try_get::<&str, _>("order_side")
            .map(|x| OrderSide::from_str(x).expect("Invalid `OrderSide`"))?;
        let quantity = row.try_get::<&str, _>("quantity").map(Quantity::from)?;
        let price = row
            .try_get::<Option<&str>, _>("price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let activation_price = row
            .try_get::<Option<&str>, _>("activation_price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let trigger_price = row
            .try_get::<Option<&str>, _>("trigger_price")
            .ok()
            .and_then(|x| x.map(Price::from));
        let trigger_type = row
            .try_get::<Option<&str>, _>("trigger_type")
            .ok()
            .and_then(|x| x.map(|x| TriggerType::from_str(x).expect("Invalid `TriggerType`")));
        let limit_offset = row
            .try_get::<Option<&str>, _>("limit_offset")
            .ok()
            .and_then(|x| x.and_then(|s| Decimal::from_str(s).ok()));
        let trailing_offset = row
            .try_get::<Option<&str>, _>("trailing_offset")
            .ok()
            .and_then(|x| x.and_then(|s| Decimal::from_str(s).ok()));
        let trailing_offset_type = row
            .try_get::<Option<TrailingOffsetTypeModel>, _>("trailing_offset_type")
            .ok()
            .and_then(|x| x.map(|x| x.0));
        let time_in_force = row
            .try_get::<&str, _>("time_in_force")
            .map(|x| TimeInForce::from_str(x).expect("Invalid `TimeInForce`"))?;
        let expire_time = row
            .try_get::<Option<&str>, _>("expire_time")
            .ok()
            .and_then(|x| x.map(UnixNanos::from));
        let filled_qty = row.try_get::<&str, _>("filled_qty").map(Quantity::from)?;
        let liquidity_side = row
            .try_get::<Option<&str>, _>("liquidity_side")
            .ok()
            .and_then(|x| x.map(|x| LiquiditySide::from_str(x).expect("Invalid `LiquiditySide`")));
        let avg_px = row.try_get::<Option<Decimal>, _>("avg_px").ok().flatten();
        let slippage = row.try_get::<Option<Decimal>, _>("slippage").ok().flatten();
        let commissions = row
            .try_get::<Option<Vec<String>>, _>("commissions")?
            .map_or_else(Vec::new, |c| {
                c.into_iter().map(|s| Money::from(&s)).collect()
            });
        let status = row
            .try_get::<&str, _>("status")
            .map(|x| OrderStatus::from_str(x).expect("Invalid `OrderStatus`"))?;
        let is_post_only = row.try_get::<bool, _>("is_post_only")?;
        let is_reduce_only = row.try_get::<bool, _>("is_reduce_only")?;
        let is_quote_quantity = row.try_get::<bool, _>("is_quote_quantity")?;
        let display_qty = row
            .try_get::<Option<&str>, _>("display_qty")
            .ok()
            .and_then(|x| x.map(Quantity::from));
        let emulation_trigger = row
            .try_get::<Option<&str>, _>("emulation_trigger")
            .ok()
            .and_then(|x| x.map(|x| TriggerType::from_str(x).expect("Invalid `TriggerType`")));
        let trigger_instrument_id = row
            .try_get::<Option<&str>, _>("trigger_instrument_id")
            .ok()
            .and_then(|x| x.map(InstrumentId::from));
        let contingency_type = row
            .try_get::<Option<&str>, _>("contingency_type")
            .ok()
            .and_then(|x| {
                x.map(|x| ContingencyType::from_str(x).expect("Invalid `ContingencyType`"))
            });
        let order_list_id = row
            .try_get::<Option<&str>, _>("order_list_id")
            .ok()
            .and_then(|x| x.map(OrderListId::from));
        let linked_order_ids = row
            .try_get::<Option<Vec<String>>, _>("linked_order_ids")
            .ok()
            .and_then(|ids| ids.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()));
        let parent_order_id = row
            .try_get::<Option<&str>, _>("parent_order_id")
            .ok()
            .and_then(|x| x.map(ClientOrderId::from));
        let exec_algorithm_id = row
            .try_get::<Option<&str>, _>("exec_algorithm_id")
            .ok()
            .and_then(|x| x.map(ExecAlgorithmId::from));
        let exec_algorithm_params: Option<IndexMap<Ustr, Ustr>> = row
            .try_get::<Option<serde_json::Value>, _>("exec_algorithm_params")
            .ok()
            .and_then(|x| {
                x.map(|x| {
                    serde_json::from_value::<IndexMap<String, String>>(x)
                        .expect("Invalid exec algorithm params")
                })
            })
            .map(|x| {
                x.into_iter()
                    .map(|(k, v)| (Ustr::from(k.as_str()), Ustr::from(v.as_str())))
                    .collect()
            });
        let exec_spawn_id = row
            .try_get::<Option<&str>, _>("exec_spawn_id")
            .ok()
            .and_then(|x| x.map(ClientOrderId::from));
        let tags = row
            .try_get::<Option<serde_json::Value>, _>("tags")
            .ok()
            .flatten()
            .and_then(|tags_value| {
                serde_json::from_value::<Vec<String>>(tags_value)
                    .ok()
                    .map(|vec| {
                        vec.into_iter()
                            .map(|tag| Ustr::from(tag.as_str()))
                            .collect::<Vec<Ustr>>()
                    })
            });
        let init_id = row.try_get::<&str, _>("init_id").map(UUID4::from)?;
        let ts_init = row.try_get::<String, _>("ts_init").map(UnixNanos::from)?;
        let ts_last = row.try_get::<String, _>("ts_last").map(UnixNanos::from)?;

        let snapshot = OrderSnapshot {
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            position_id,
            account_id,
            last_trade_id,
            order_type,
            order_side,
            quantity,
            price,
            activation_price,
            trigger_price,
            trigger_type,
            limit_offset,
            trailing_offset,
            trailing_offset_type,
            time_in_force,
            expire_time,
            filled_qty,
            liquidity_side,
            avg_px,
            slippage,
            commissions,
            status,
            is_post_only,
            is_reduce_only,
            is_quote_quantity,
            display_qty,
            emulation_trigger,
            trigger_instrument_id,
            contingency_type,
            order_list_id,
            linked_order_ids,
            parent_order_id,
            exec_algorithm_id,
            exec_algorithm_params,
            exec_spawn_id,
            tags,
            init_id,
            ts_init,
            ts_last,
            causation_id: None,
        };

        Ok(Self(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        events::order::spec::{
            OrderAcceptedSpec, OrderCancelRejectedSpec, OrderCanceledSpec, OrderDeniedSpec,
            OrderEmulatedSpec, OrderExpiredSpec, OrderFillVoidedSpec, OrderFilledSpec,
            OrderInitializedSpec, OrderModifyRejectedSpec, OrderPendingCancelSpec,
            OrderPendingUpdateSpec, OrderRejectedSpec, OrderReleasedSpec, OrderSubmittedSpec,
            OrderTriggeredSpec, OrderUpdatedSpec,
        },
        orders::OrderAny,
    };
    use rstest::rstest;

    use super::*;

    impl OrderEventKind {
        const ALL: [Self; 17] = [
            Self::Accepted,
            Self::CancelRejected,
            Self::Canceled,
            Self::Denied,
            Self::Emulated,
            Self::Expired,
            Self::FillVoided,
            Self::Filled,
            Self::Initialized,
            Self::ModifyRejected,
            Self::PendingCancel,
            Self::PendingUpdate,
            Self::Rejected,
            Self::Released,
            Self::Submitted,
            Self::Triggered,
            Self::Updated,
        ];
    }

    /// Returns the kind that the row of an event names. The match is exhaustive: a new
    /// `OrderEventAny` variant fails to build here until it has a kind and a decoder.
    fn kind_of(event: &OrderEventAny) -> OrderEventKind {
        match event {
            OrderEventAny::Accepted(_) => OrderEventKind::Accepted,
            OrderEventAny::CancelRejected(_) => OrderEventKind::CancelRejected,
            OrderEventAny::Canceled(_) => OrderEventKind::Canceled,
            OrderEventAny::Denied(_) => OrderEventKind::Denied,
            OrderEventAny::Emulated(_) => OrderEventKind::Emulated,
            OrderEventAny::Expired(_) => OrderEventKind::Expired,
            OrderEventAny::FillVoided(_) => OrderEventKind::FillVoided,
            OrderEventAny::Filled(_) => OrderEventKind::Filled,
            OrderEventAny::Initialized(_) => OrderEventKind::Initialized,
            OrderEventAny::ModifyRejected(_) => OrderEventKind::ModifyRejected,
            OrderEventAny::PendingCancel(_) => OrderEventKind::PendingCancel,
            OrderEventAny::PendingUpdate(_) => OrderEventKind::PendingUpdate,
            OrderEventAny::Rejected(_) => OrderEventKind::Rejected,
            OrderEventAny::Released(_) => OrderEventKind::Released,
            OrderEventAny::Submitted(_) => OrderEventKind::Submitted,
            OrderEventAny::Triggered(_) => OrderEventKind::Triggered,
            OrderEventAny::Updated(_) => OrderEventKind::Updated,
        }
    }

    /// Returns one event of a kind. The match is exhaustive: a new kind fails to build here.
    fn sample_of(kind: OrderEventKind) -> OrderEventAny {
        match kind {
            OrderEventKind::Accepted => {
                OrderEventAny::Accepted(OrderAcceptedSpec::builder().build())
            }
            OrderEventKind::CancelRejected => {
                OrderEventAny::CancelRejected(OrderCancelRejectedSpec::builder().build())
            }
            OrderEventKind::Canceled => {
                OrderEventAny::Canceled(OrderCanceledSpec::builder().build())
            }
            OrderEventKind::Denied => OrderEventAny::Denied(OrderDeniedSpec::builder().build()),
            OrderEventKind::Emulated => {
                OrderEventAny::Emulated(OrderEmulatedSpec::builder().build())
            }
            OrderEventKind::Expired => OrderEventAny::Expired(OrderExpiredSpec::builder().build()),
            OrderEventKind::FillVoided => {
                OrderEventAny::FillVoided(OrderFillVoidedSpec::builder().build())
            }
            OrderEventKind::Filled => OrderEventAny::Filled(OrderFilledSpec::builder().build()),
            OrderEventKind::Initialized => {
                OrderEventAny::Initialized(OrderInitializedSpec::builder().build())
            }
            OrderEventKind::ModifyRejected => {
                OrderEventAny::ModifyRejected(OrderModifyRejectedSpec::builder().build())
            }
            OrderEventKind::PendingCancel => {
                OrderEventAny::PendingCancel(OrderPendingCancelSpec::builder().build())
            }
            OrderEventKind::PendingUpdate => {
                OrderEventAny::PendingUpdate(OrderPendingUpdateSpec::builder().build())
            }
            OrderEventKind::Rejected => {
                OrderEventAny::Rejected(OrderRejectedSpec::builder().build())
            }
            OrderEventKind::Released => {
                OrderEventAny::Released(OrderReleasedSpec::builder().build())
            }
            OrderEventKind::Submitted => {
                OrderEventAny::Submitted(OrderSubmittedSpec::builder().build())
            }
            OrderEventKind::Triggered => {
                OrderEventAny::Triggered(OrderTriggeredSpec::builder().build())
            }
            OrderEventKind::Updated => OrderEventAny::Updated(OrderUpdatedSpec::builder().build()),
        }
    }

    #[rstest]
    fn test_every_order_event_variant_has_a_decoder_kind() {
        for kind in OrderEventKind::ALL {
            let event = sample_of(kind);
            let name = event.clone().into_boxed().type_name();

            assert_eq!(kind_of(&event), kind);
            assert_eq!(OrderEventKind::parse(name), Some(kind), "row kind {name}");
        }
        assert_eq!(OrderEventKind::parse("OrderUnknown"), None);
    }

    fn fill(trade_id: &str, last_qty: u64, reconciliation: bool) -> OrderEventAny {
        OrderEventAny::Filled(
            OrderFilledSpec::builder()
                .trade_id(TradeId::from(trade_id))
                .last_qty(Quantity::from(last_qty))
                .reconciliation(reconciliation)
                .build(),
        )
    }

    /// The incident of 2026-10-19: a lagging snapshot voids the newest fill and reopens the
    /// order, and the reconciliation then accepts the order and infers the same fill again.
    fn reopened_void_events(is_reopened: bool) -> Vec<OrderEventAny> {
        vec![
            OrderEventAny::Initialized(
                OrderInitializedSpec::builder()
                    .order_type(OrderType::Limit)
                    .quantity(Quantity::from(7))
                    .price(Price::from("1.00000"))
                    .build(),
            ),
            OrderEventAny::Submitted(OrderSubmittedSpec::builder().build()),
            OrderEventAny::Accepted(OrderAcceptedSpec::builder().build()),
            fill("T-LIVE-1", 4, false),
            fill("T-LIVE-2", 3, false),
            OrderEventAny::FillVoided(
                OrderFillVoidedSpec::builder()
                    .trade_id(TradeId::from("T-LIVE-2"))
                    .voided_qty(Quantity::from(3))
                    .reconciliation(true)
                    .is_reopened(is_reopened)
                    .build(),
            ),
            OrderEventAny::Accepted(OrderAcceptedSpec::builder().reconciliation(true).build()),
            fill("T-INFERRED", 3, true),
        ]
    }

    #[rstest]
    #[case::reopened(true, true)]
    #[case::not_reopened(false, false)]
    fn test_reopen_flag_selects_the_replayed_order_status(
        #[case] is_reopened: bool,
        #[case] replays: bool,
    ) {
        let replay = OrderAny::from_events(reopened_void_events(is_reopened));

        assert_eq!(replay.is_ok(), replays, "{replay:?}");
    }
}
