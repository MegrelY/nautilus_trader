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

//! Opt-in account recovery reports collected without blocking the runner.

use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use nautilus_common::{
    clients::ExecutionClient,
    live::dst,
    messages::{ExecutionEvent, ExecutionReport, execution::TradingCommand},
};
use nautilus_model::{
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, Venue},
    reports::ExecutionMassStatus,
};

use super::ReportTaskOutcome;
use crate::execution::client::LiveExecutionClient;

const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

type MassStatusReportFuture =
    Pin<Box<dyn Future<Output = ReportTaskOutcome<anyhow::Result<Option<ExecutionMassStatus>>>>>>;

/// Monotonic local receipt revisions prevent an old account snapshot from
/// reversing an order/fill processed while its HTTP collection was pending.
#[derive(Debug, Default)]
pub(super) struct MassStatusActivity {
    revisions: HashMap<ClientId, u64>,
}

impl MassStatusActivity {
    pub(super) fn revision(&self, client: &LiveExecutionClient) -> u64 {
        self.revisions
            .get(&client.client_id())
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn unchanged(&self, client: &LiveExecutionClient, captured: u64) -> bool {
        captured != u64::MAX && captured == self.revision(client)
    }

    fn mark(&mut self, client_id: ClientId) {
        let revision = self.revisions.entry(client_id).or_default();
        *revision = revision.saturating_add(1);
    }

    fn scope(&mut self, account: Option<AccountId>, venue: Venue, clients: &[LiveExecutionClient]) {
        for client in clients {
            if account.is_none_or(|account| account == client.account_id())
                && client.handles_order_venue(venue)
            {
                self.mark(client.client_id());
            }
        }
    }

    pub(super) fn order(&mut self, event: &OrderEventAny, clients: &[LiveExecutionClient]) {
        self.scope(event.account_id(), event.instrument_id().venue, clients);
    }

    pub(super) fn event(&mut self, event: &ExecutionEvent, clients: &[LiveExecutionClient]) {
        match event {
            ExecutionEvent::Order(event) => self.order(event, clients),
            ExecutionEvent::OrderSubmittedBatch(batch) => {
                for event in &batch.events {
                    self.scope(Some(event.account_id), event.instrument_id.venue, clients);
                }
            }
            ExecutionEvent::OrderAcceptedBatch(batch) => {
                for event in &batch.events {
                    self.scope(Some(event.account_id), event.instrument_id.venue, clients);
                }
            }
            ExecutionEvent::OrderCanceledBatch(batch) => {
                for event in &batch.events {
                    self.scope(event.account_id, event.instrument_id.venue, clients);
                }
            }
            ExecutionEvent::Report(report) => match report {
                ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _) => {
                    self.scope(Some(report.account_id), report.instrument_id.venue, clients)
                }
                ExecutionReport::Fill(report) => {
                    self.scope(Some(report.account_id), report.instrument_id.venue, clients)
                }
                ExecutionReport::Position(report) => {
                    self.scope(Some(report.account_id), report.instrument_id.venue, clients)
                }
                ExecutionReport::MassStatus(report) => {
                    self.scope(Some(report.account_id), report.venue, clients)
                }
            },
            // Collection itself refreshes account balances; those do not prove
            // an order/position change and must not invalidate every request.
            ExecutionEvent::Account(_) => {}
        }
    }

    pub(super) fn command(&mut self, command: &TradingCommand, clients: &[LiveExecutionClient]) {
        if matches!(
            command,
            TradingCommand::QueryAccount(_) | TradingCommand::QueryOrder(_)
        ) {
            return;
        }
        if let Some(client_id) = command.client_id() {
            self.mark(client_id);
        } else {
            self.scope(None, command.instrument_id().venue, clients);
        }
    }
}

pub(super) struct MassStatusReportTask {
    pub(super) client: LiveExecutionClient,
    activity_revision: u64,
    pub(super) future: MassStatusReportFuture,
}

pub(super) struct MassStatusRetryState {
    pub(super) task: Option<MassStatusReportTask>,
    schedules: HashMap<ClientId, RetrySchedule>,
    next_client: usize,
}

struct RetrySchedule {
    next_due: dst::time::Instant,
    delay: Duration,
}

impl RetrySchedule {
    fn new(now: dst::time::Instant) -> Self {
        Self {
            next_due: now + INITIAL_RETRY_DELAY,
            delay: INITIAL_RETRY_DELAY,
        }
    }

    fn completed(&mut self, now: dst::time::Instant) {
        self.next_due = now + self.delay;
        self.delay = self.delay.saturating_mul(2).min(MAX_RETRY_DELAY);
    }
}

impl MassStatusRetryState {
    pub(super) fn new() -> Self {
        Self {
            task: None,
            schedules: HashMap::new(),
            next_client: 0,
        }
    }

    pub(super) fn start_due(
        &mut self,
        clients: &[LiveExecutionClient],
        activity: &MassStatusActivity,
        now: dst::time::Instant,
        timeout: Duration,
        lookback_mins: Option<u64>,
    ) {
        if self.task.is_some() {
            return;
        }
        for offset in 0..clients.len() {
            let index = (self.next_client + offset) % clients.len();
            let client = &clients[index];
            let client_id = client.client_id();
            if !client.requires_mass_status_reconciliation() {
                self.schedules.remove(&client_id);
                continue;
            }
            let schedule = self
                .schedules
                .entry(client_id)
                .or_insert_with(|| RetrySchedule::new(now));
            if now < schedule.next_due || !client.is_connected() {
                continue;
            }
            self.next_client = (index + 1) % clients.len();
            let report_client = client.clone();
            let deadline = now + timeout;
            self.task = Some(MassStatusReportTask {
                client: client.clone(),
                activity_revision: activity.revision(client),
                future: Box::pin(async move {
                    let remaining = deadline.saturating_duration_since(dst::time::Instant::now());
                    match dst::time::timeout(
                        remaining,
                        report_client.generate_mass_status(lookback_mins),
                    )
                    .await
                    {
                        Ok(result) => ReportTaskOutcome::Completed(result),
                        Err(_) => ReportTaskOutcome::TimedOut,
                    }
                }),
            });
            return;
        }
    }

    // Drop the future before touching the client so cancellation releases its
    // shared report borrow and pending instrument updates can be applied.
    pub(super) fn finish(&mut self, now: dst::time::Instant) -> Option<(LiveExecutionClient, u64)> {
        let task = self.task.take()?;
        let client = task.client;
        drop(task.future);
        client.flush_pending_instruments();
        if let Some(schedule) = self.schedules.get_mut(&client.client_id()) {
            schedule.completed(now);
        }
        Some((client, task.activity_revision))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn backoff_uses_completion_time_and_caps_without_bursts() {
        let now = dst::time::Instant::now();
        let mut schedule = RetrySchedule::new(now);
        assert_eq!(schedule.next_due, now + Duration::from_secs(5));
        let completion = now + Duration::from_secs(90);
        for delay in [5, 10, 20, 40, 60, 60] {
            schedule.completed(completion);
            assert_eq!(schedule.next_due, completion + Duration::from_secs(delay));
        }
    }

    use super::super::{LiveNode, LiveNodeConfig};
    use async_trait::async_trait;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        accounts::AccountAny,
        enums::OmsType,
        identifiers::{AccountId, Venue},
        types::{AccountBalance, MarginBalance},
    };
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    #[cfg(all(feature = "simulation", madsim))]
    async fn advance_clock(duration: Duration) {
        madsim::time::advance(duration);
        madsim::task::yield_now().await;
    }

    #[cfg(not(all(feature = "simulation", madsim)))]
    async fn advance_clock(duration: Duration) {
        tokio::time::advance(duration).await;
    }

    struct RecoveryClient {
        requested: Rc<Cell<bool>>,
        collected: Rc<Cell<usize>>,
        applied: Rc<RefCell<Vec<UUID4>>>,
    }

    #[async_trait(?Send)]
    impl ExecutionClient for RecoveryClient {
        fn is_connected(&self) -> bool {
            true
        }
        fn client_id(&self) -> ClientId {
            ClientId::from("RECOVERY")
        }
        fn account_id(&self) -> AccountId {
            AccountId::from("RECOVERY-001")
        }
        fn venue(&self) -> Venue {
            Venue::from("RECOVERY")
        }
        fn oms_type(&self) -> OmsType {
            OmsType::Netting
        }
        fn get_account(&self) -> Option<AccountAny> {
            None
        }
        fn generate_account_state(
            &self,
            _: Vec<AccountBalance>,
            _: Vec<MarginBalance>,
            _: bool,
            _: UnixNanos,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        fn start(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        fn stop(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        fn requires_mass_status_reconciliation(&self) -> bool {
            self.requested.get()
        }
        fn on_mass_status_reconciled(&self, report_id: UUID4) {
            self.applied.borrow_mut().push(report_id);
            self.requested.set(false);
        }
        async fn generate_mass_status(
            &self,
            _: Option<u64>,
        ) -> anyhow::Result<Option<ExecutionMassStatus>> {
            self.collected.set(self.collected.get() + 1);
            dst::time::sleep(Duration::from_secs(10)).await;
            Ok(Some(ExecutionMassStatus::new(
                self.client_id(),
                self.account_id(),
                self.venue(),
                UnixNanos::default(),
                None,
            )))
        }
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn report_polling_keeps_timers_live_and_notifies_only_after_application() {
        let requested = Rc::new(Cell::new(true));
        let collected = Rc::new(Cell::new(0));
        let applied = Rc::new(RefCell::new(Vec::new()));
        let client = LiveExecutionClient::new(Box::new(RecoveryClient {
            requested: requested.clone(),
            collected: collected.clone(),
            applied: applied.clone(),
        }));
        let clients = vec![client.clone()];
        let mut retries = MassStatusRetryState::new();
        let now = dst::time::Instant::now();
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            now,
            Duration::from_secs(30),
            None,
        );
        assert!(retries.task.is_none());
        advance_clock(INITIAL_RETRY_DELAY).await;
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            dst::time::Instant::now(),
            Duration::from_secs(30),
            None,
        );
        let task = retries.task.as_mut().unwrap();
        tokio::select! {
            biased;
            _ = task.future.as_mut() => panic!("report must still be pending"),
            _ = dst::time::sleep(Duration::from_millis(100)) => {},
        }
        assert_eq!(collected.get(), 1);
        assert!(applied.borrow().is_empty());
        let result = task.future.as_mut().await;
        let (completed_client, captured_revision) =
            retries.finish(dst::time::Instant::now()).unwrap();
        let ReportTaskOutcome::Completed(Ok(Some(report))) = result else {
            panic!("expected collected report");
        };
        let report_id = report.report_id;
        assert!(
            applied.borrow().is_empty(),
            "collection cannot certify cache application"
        );
        let mut node = LiveNode::build(
            "MassRecoveryProof".to_string(),
            Some(LiveNodeConfig::default()),
        )
        .unwrap();
        let mut wrong_account = report.clone();
        wrong_account.account_id = AccountId::from("OTHER-001");
        node.apply_recovery_mass_status(&completed_client, wrong_account, captured_revision)
            .await;
        assert!(applied.borrow().is_empty());
        node.apply_recovery_mass_status(&completed_client, report, captured_revision)
            .await;
        assert_eq!(&*applied.borrow(), &[report_id]);
        assert!(!requested.get());
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            dst::time::Instant::now() + MAX_RETRY_DELAY,
            Duration::from_secs(30),
            None,
        );
        assert!(
            retries.task.is_none(),
            "resolved clients must stop requesting reports"
        );
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn timeout_drops_report_before_client_reuse_and_never_notifies() {
        let applied = Rc::new(RefCell::new(Vec::new()));
        let client = LiveExecutionClient::new(Box::new(RecoveryClient {
            requested: Rc::new(Cell::new(true)),
            collected: Rc::new(Cell::new(0)),
            applied: applied.clone(),
        }));
        let mut retries = MassStatusRetryState::new();
        let clients = vec![client];
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            dst::time::Instant::now(),
            Duration::from_secs(1),
            None,
        );
        advance_clock(INITIAL_RETRY_DELAY).await;
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            dst::time::Instant::now(),
            Duration::from_secs(1),
            None,
        );
        assert!(matches!(
            retries.task.as_mut().unwrap().future.as_mut().await,
            ReportTaskOutcome::TimedOut
        ));
        let (mut client, _) = retries.finish(dst::time::Instant::now()).unwrap();
        client.stop().unwrap(); // Mutably borrows the underlying client after cancellation.
        assert!(applied.borrow().is_empty());
        retries.start_due(
            &clients,
            &MassStatusActivity::default(),
            dst::time::Instant::now(),
            Duration::from_secs(1),
            None,
        );
        assert!(retries.task.is_none(), "timeout must respect backoff");
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn startup_timeout_defers_only_explicit_recovery_clients(#[case] opt_in: bool) {
        let applied = Rc::new(RefCell::new(Vec::new()));
        let requested = Rc::new(Cell::new(opt_in));
        let client = LiveExecutionClient::new(Box::new(RecoveryClient {
            requested: requested.clone(),
            collected: Rc::new(Cell::new(0)),
            applied: applied.clone(),
        }));
        let config = LiveNodeConfig {
            timeout_reconciliation: Duration::from_secs(1),
            ..LiveNodeConfig::default()
        };
        let mut node = LiveNode::build("StartupRecoveryProof".to_string(), Some(config)).unwrap();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(client.clone()))
            .unwrap();
        node.exec_clients.push(client);
        let result = node.perform_startup_reconciliation().await;
        assert_eq!(result.is_ok(), opt_in);
        assert!(applied.borrow().is_empty());
        assert_eq!(requested.get(), opt_in);
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn execution_activity_during_collection_discards_stale_snapshot() {
        let requested = Rc::new(Cell::new(true));
        let applied = Rc::new(RefCell::new(Vec::new()));
        let client = LiveExecutionClient::new(Box::new(RecoveryClient {
            requested: requested.clone(),
            collected: Rc::new(Cell::new(0)),
            applied: applied.clone(),
        }));
        let clients = vec![client.clone()];
        let mut node = LiveNode::build(
            "FreshRecoveryProof".to_string(),
            Some(LiveNodeConfig::default()),
        )
        .unwrap();
        let mut retries = MassStatusRetryState::new();
        retries.start_due(
            &clients,
            &node.mass_status_activity,
            dst::time::Instant::now(),
            Duration::from_secs(30),
            None,
        );
        advance_clock(INITIAL_RETRY_DELAY).await;
        retries.start_due(
            &clients,
            &node.mass_status_activity,
            dst::time::Instant::now(),
            Duration::from_secs(30),
            None,
        );
        let task = retries.task.as_mut().unwrap();
        tokio::select! {
            biased;
            _ = task.future.as_mut() => panic!("report must still be pending"),
            _ = dst::time::sleep(Duration::from_millis(100)) => {},
        }
        // An unrelated account does not prevent this recovery from progressing.
        node.mass_status_activity.scope(
            Some(AccountId::from("OTHER-001")),
            client.venue(),
            &clients,
        );
        assert!(node.mass_status_activity.unchanged(&client, 0));
        // Same scope as a TP/SL fill received while collection is pending.
        node.mass_status_activity
            .scope(Some(client.account_id()), client.venue(), &clients);
        let result = task.future.as_mut().await;
        let (client, captured) = retries.finish(dst::time::Instant::now()).unwrap();
        let ReportTaskOutcome::Completed(Ok(Some(report))) = result else {
            panic!("expected report");
        };
        node.apply_recovery_mass_status(&client, report.clone(), captured)
            .await;
        assert!(applied.borrow().is_empty());
        assert!(requested.get(), "a racing fill must leave recovery pending");
        requested.set(false);
        let current = node.mass_status_activity.revision(&client);
        node.apply_recovery_mass_status(&client, report, current)
            .await;
        assert!(
            applied.borrow().is_empty(),
            "resolved recovery must discard late snapshots too"
        );
    }
}
