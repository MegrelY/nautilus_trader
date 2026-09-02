// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
// -------------------------------------------------------------------------------------------------

//! Process-local, payload-level Hyperliquid transport counters.
//!
//! Byte counters measure serialized HTTP bodies and WebSocket frame payloads. They intentionally
//! exclude HTTP headers, TLS framing, and TCP/IP overhead. Counters never retain payload content.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative process-local Hyperliquid transport counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HyperliquidNetworkMetricsSnapshot {
    pub http_requests: u64,
    pub http_request_bytes: u64,
    pub http_responses: u64,
    pub http_response_bytes: u64,
    pub websocket_inbound_messages: u64,
    pub websocket_inbound_bytes: u64,
    pub websocket_outbound_messages: u64,
    pub websocket_outbound_bytes: u64,
    pub websocket_reconnects: u64,
    pub websocket_backpressure_events: u64,
    pub websocket_subscription_sends: u64,
    pub websocket_subscription_replays: u64,
    pub websocket_duplicate_subscriptions_skipped: u64,
    pub websocket_subscription_confirmations: u64,
    pub websocket_unsubscription_confirmations: u64,
}

static HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
static HTTP_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static HTTP_RESPONSES: AtomicU64 = AtomicU64::new(0);
static HTTP_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_INBOUND_MESSAGES: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_INBOUND_BYTES: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_OUTBOUND_MESSAGES: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_OUTBOUND_BYTES: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_RECONNECTS: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_BACKPRESSURE_EVENTS: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_SUBSCRIPTION_SENDS: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_SUBSCRIPTION_REPLAYS: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_DUPLICATE_SUBSCRIPTIONS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_SUBSCRIPTION_CONFIRMATIONS: AtomicU64 = AtomicU64::new(0);
static WEBSOCKET_UNSUBSCRIPTION_CONFIRMATIONS: AtomicU64 = AtomicU64::new(0);

/// Returns one internally consistent-enough observation of monotonic counters.
#[must_use]
pub fn hyperliquid_network_metrics_snapshot() -> HyperliquidNetworkMetricsSnapshot {
    HyperliquidNetworkMetricsSnapshot {
        http_requests: HTTP_REQUESTS.load(Ordering::Relaxed),
        http_request_bytes: HTTP_REQUEST_BYTES.load(Ordering::Relaxed),
        http_responses: HTTP_RESPONSES.load(Ordering::Relaxed),
        http_response_bytes: HTTP_RESPONSE_BYTES.load(Ordering::Relaxed),
        websocket_inbound_messages: WEBSOCKET_INBOUND_MESSAGES.load(Ordering::Relaxed),
        websocket_inbound_bytes: WEBSOCKET_INBOUND_BYTES.load(Ordering::Relaxed),
        websocket_outbound_messages: WEBSOCKET_OUTBOUND_MESSAGES.load(Ordering::Relaxed),
        websocket_outbound_bytes: WEBSOCKET_OUTBOUND_BYTES.load(Ordering::Relaxed),
        websocket_reconnects: WEBSOCKET_RECONNECTS.load(Ordering::Relaxed),
        websocket_backpressure_events: WEBSOCKET_BACKPRESSURE_EVENTS.load(Ordering::Relaxed),
        websocket_subscription_sends: WEBSOCKET_SUBSCRIPTION_SENDS.load(Ordering::Relaxed),
        websocket_subscription_replays: WEBSOCKET_SUBSCRIPTION_REPLAYS.load(Ordering::Relaxed),
        websocket_duplicate_subscriptions_skipped: WEBSOCKET_DUPLICATE_SUBSCRIPTIONS_SKIPPED
            .load(Ordering::Relaxed),
        websocket_subscription_confirmations: WEBSOCKET_SUBSCRIPTION_CONFIRMATIONS
            .load(Ordering::Relaxed),
        websocket_unsubscription_confirmations: WEBSOCKET_UNSUBSCRIPTION_CONFIRMATIONS
            .load(Ordering::Relaxed),
    }
}

pub(crate) fn record_http_request(bytes: usize) {
    increment(&HTTP_REQUESTS, 1);
    increment(&HTTP_REQUEST_BYTES, bytes_as_u64(bytes));
}

pub(crate) fn record_http_response(bytes: usize) {
    increment(&HTTP_RESPONSES, 1);
    increment(&HTTP_RESPONSE_BYTES, bytes_as_u64(bytes));
}

pub(crate) fn record_websocket_inbound(bytes: usize) {
    increment(&WEBSOCKET_INBOUND_MESSAGES, 1);
    increment(&WEBSOCKET_INBOUND_BYTES, bytes_as_u64(bytes));
}

pub(crate) fn record_websocket_outbound(bytes: usize) {
    increment(&WEBSOCKET_OUTBOUND_MESSAGES, 1);
    increment(&WEBSOCKET_OUTBOUND_BYTES, bytes_as_u64(bytes));
}

pub(crate) fn record_websocket_reconnect() {
    increment(&WEBSOCKET_RECONNECTS, 1);
}

pub(crate) fn record_websocket_backpressure() {
    increment(&WEBSOCKET_BACKPRESSURE_EVENTS, 1);
}

pub(crate) fn record_websocket_subscription_send(replay: bool) {
    increment(&WEBSOCKET_SUBSCRIPTION_SENDS, 1);
    if replay {
        increment(&WEBSOCKET_SUBSCRIPTION_REPLAYS, 1);
    }
}

pub(crate) fn record_websocket_duplicate_subscription_skipped() {
    increment(&WEBSOCKET_DUPLICATE_SUBSCRIPTIONS_SKIPPED, 1);
}

pub(crate) fn record_websocket_subscription_confirmation(unsubscribe: bool) {
    let counter = if unsubscribe {
        &WEBSOCKET_UNSUBSCRIPTION_CONFIRMATIONS
    } else {
        &WEBSOCKET_SUBSCRIPTION_CONFIRMATIONS
    };
    increment(counter, 1);
}

fn increment(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn bytes_as_u64(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_without_retaining_payloads() {
        let before = hyperliquid_network_metrics_snapshot();
        record_http_request(11);
        record_http_response(13);
        record_websocket_inbound(17);
        record_websocket_outbound(19);
        record_websocket_subscription_send(true);
        record_websocket_duplicate_subscription_skipped();

        let after = hyperliquid_network_metrics_snapshot();
        assert_eq!(after.http_requests - before.http_requests, 1);
        assert_eq!(after.http_request_bytes - before.http_request_bytes, 11);
        assert_eq!(after.http_response_bytes - before.http_response_bytes, 13);
        assert_eq!(
            after.websocket_inbound_bytes - before.websocket_inbound_bytes,
            17
        );
        assert_eq!(
            after.websocket_outbound_bytes - before.websocket_outbound_bytes,
            19
        );
        assert_eq!(
            after.websocket_subscription_replays - before.websocket_subscription_replays,
            1
        );
        assert_eq!(
            after.websocket_duplicate_subscriptions_skipped
                - before.websocket_duplicate_subscriptions_skipped,
            1
        );
    }
}
