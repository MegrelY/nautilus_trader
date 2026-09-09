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

use nautilus_model::reports::ExecutionMassStatus;

const PRIVATE_STATE_GAP_LIMIT: usize = 64;
const PRIVATE_STATE_IDENTITY_LIMIT: usize = 96;
const PRIVATE_STATE_DETAIL_LIMIT: usize = 256;

/// Private Hyperliquid source inspected during execution reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidPrivateStateSource {
    DexEnumeration,
    OpenOrders,
    Fills,
    HistoricalOrders,
    PerpPositions,
    SpotPositions,
}

/// Reason a private Hyperliquid record could not be represented as a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidPrivateStateGapKind {
    SourceFailure,
    ParseFailure,
    UnknownInstrument,
    HistoryIncomplete,
}

/// One bounded private-state omission detected during reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidPrivateStateGap {
    source: HyperliquidPrivateStateSource,
    kind: HyperliquidPrivateStateGapKind,
    dex: Option<String>,
    identity: Option<String>,
    detail: String,
    independent_spot_token: Option<u32>,
}

impl HyperliquidPrivateStateGap {
    pub(crate) fn new(
        source: HyperliquidPrivateStateSource,
        kind: HyperliquidPrivateStateGapKind,
        dex: Option<&str>,
        identity: Option<&str>,
        detail: impl AsRef<str>,
    ) -> Self {
        Self {
            source,
            kind,
            dex: dex.map(|value| bound_text(value, PRIVATE_STATE_IDENTITY_LIMIT)),
            identity: identity.map(|value| bound_text(value, PRIVATE_STATE_IDENTITY_LIMIT)),
            detail: bound_text(detail.as_ref(), PRIVATE_STATE_DETAIL_LIMIT),
            independent_spot_token: None,
        }
    }

    #[must_use]
    pub const fn source(&self) -> HyperliquidPrivateStateSource {
        self.source
    }

    #[must_use]
    pub const fn kind(&self) -> HyperliquidPrivateStateGapKind {
        self.kind
    }

    #[must_use]
    pub fn dex(&self) -> Option<&str> {
        self.dex.as_deref()
    }

    #[must_use]
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    pub(crate) fn with_unreserved_spot_token(mut self, token: Option<u32>) -> Self {
        self.independent_spot_token = token.filter(|token| *token != 0);
        self
    }

    /// A parsed positive spot balance with a concrete non-USDC token and no hold.
    /// Account mode and complete execution coverage must be proven separately.
    pub fn unreserved_spot_token(&self) -> Option<u32> {
        self.independent_spot_token
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Error returned when a mass-status result contains known reports but is incomplete.
#[derive(Debug, thiserror::Error)]
#[error("Hyperliquid mass status is incomplete")]
pub struct HyperliquidIncompleteMassStatus {
    mass_status: ExecutionMassStatus,
    gaps: Vec<HyperliquidPrivateStateGap>,
    omitted_gap_count: usize,
    native_perpetuals_complete: bool,
}

impl HyperliquidIncompleteMassStatus {
    pub(crate) fn new(
        mass_status: ExecutionMassStatus,
        gaps: Vec<HyperliquidPrivateStateGap>,
        omitted_gap_count: usize,
    ) -> Self {
        Self {
            mass_status,
            gaps,
            omitted_gap_count,
            native_perpetuals_complete: false,
        }
    }

    pub(crate) fn prove_native_perpetual_scope(
        mut self,
        starting_mode: Option<&str>,
        ending_mode: Option<&str>,
        fresh_generation: bool,
    ) -> Self {
        let native = |id: nautilus_model::identifiers::InstrumentId| {
            id.symbol.as_str().ends_with("-USD-PERP") && !id.symbol.as_str().contains(':')
        };
        self.native_perpetuals_complete = fresh_generation
            && starting_mode.is_some_and(|mode| matches!(mode, "default" | "disabled"))
            && starting_mode == ending_mode
            && self.omitted_gap_count == 0
            && !self.gaps.is_empty()
            && self.gaps.iter().all(|gap| {
                gap.source() == HyperliquidPrivateStateSource::SpotPositions
                    && gap.kind() == HyperliquidPrivateStateGapKind::UnknownInstrument
                    && gap.unreserved_spot_token().is_some()
            })
            && self
                .mass_status
                .order_reports()
                .values()
                .all(|report| native(report.instrument_id))
            && self
                .mass_status
                .fill_reports()
                .values()
                .flatten()
                .all(|report| native(report.instrument_id));
        self
    }

    /// Does not establish spot ownership, collateral value, or account-wide completeness.
    pub const fn native_perpetuals_complete(&self) -> bool {
        self.native_perpetuals_complete
    }

    #[must_use]
    pub const fn mass_status(&self) -> &ExecutionMassStatus {
        &self.mass_status
    }

    #[must_use]
    pub fn gaps(&self) -> &[HyperliquidPrivateStateGap] {
        &self.gaps
    }

    #[must_use]
    pub const fn omitted_gap_count(&self) -> usize {
        self.omitted_gap_count
    }

    #[must_use]
    pub fn into_parts(self) -> (ExecutionMassStatus, Vec<HyperliquidPrivateStateGap>, usize) {
        (self.mass_status, self.gaps, self.omitted_gap_count)
    }
}

#[derive(Debug)]
pub(crate) struct HyperliquidPrivateStateBatch<T> {
    reports: Vec<T>,
    gaps: Vec<HyperliquidPrivateStateGap>,
    omitted_gap_count: usize,
}

impl<T> Default for HyperliquidPrivateStateBatch<T> {
    fn default() -> Self {
        Self {
            reports: Vec::new(),
            gaps: Vec::new(),
            omitted_gap_count: 0,
        }
    }
}

impl<T> HyperliquidPrivateStateBatch<T> {
    pub(crate) fn from_parts(
        reports: Vec<T>,
        gaps: Vec<HyperliquidPrivateStateGap>,
        omitted_gap_count: usize,
    ) -> Self {
        Self {
            reports,
            gaps,
            omitted_gap_count,
        }
    }

    pub(crate) fn push_report(&mut self, report: T) {
        self.reports.push(report);
    }

    pub(crate) fn push_gap(&mut self, gap: HyperliquidPrivateStateGap) {
        if self.gaps.len() < PRIVATE_STATE_GAP_LIMIT {
            self.gaps.push(gap);
        } else {
            self.omitted_gap_count = self.omitted_gap_count.saturating_add(1);
        }
    }

    pub(crate) fn absorb<U>(&mut self, other: HyperliquidPrivateStateBatch<U>) -> Vec<U> {
        for gap in other.gaps {
            self.push_gap(gap);
        }
        self.omitted_gap_count = self
            .omitted_gap_count
            .saturating_add(other.omitted_gap_count);
        other.reports
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.gaps.is_empty() && self.omitted_gap_count == 0
    }

    pub(crate) fn into_parts(self) -> (Vec<T>, Vec<HyperliquidPrivateStateGap>, usize) {
        (self.reports, self.gaps, self.omitted_gap_count)
    }
}

fn bound_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn spot_isolation_requires_identity_standard_mode_and_complete_fresh_execution() {
        use nautilus_model::identifiers::{AccountId, ClientId, Venue};
        let status = || {
            ExecutionMassStatus::new(
                ClientId::from("HYPERLIQUID"),
                AccountId::from("HYPERLIQUID-001"),
                Venue::from("HYPERLIQUID"),
                nautilus_core::UnixNanos::from(1),
                None,
            )
        };
        let gap = |source, kind, token| {
            HyperliquidPrivateStateGap::new(source, kind, None, Some("NEW"), "missing")
                .with_unreserved_spot_token(token)
        };
        let valid = gap(
            HyperliquidPrivateStateSource::SpotPositions,
            HyperliquidPrivateStateGapKind::UnknownInstrument,
            Some(77),
        );
        for mode in ["default", "disabled"] {
            let result = HyperliquidIncompleteMassStatus::new(status(), vec![valid.clone()], 0)
                .prove_native_perpetual_scope(Some(mode), Some(mode), true);
            assert!(result.native_perpetuals_complete());
            assert_eq!(result.gaps().len(), 1);
            assert!(!result.mass_status().position_reports_complete);
        }
        for (start, end, fresh, omitted, finding) in [
            (
                Some("unifiedAccount"),
                Some("unifiedAccount"),
                true,
                0,
                valid.clone(),
            ),
            (
                Some("portfolioMargin"),
                Some("portfolioMargin"),
                true,
                0,
                valid.clone(),
            ),
            (
                Some("default"),
                Some("unifiedAccount"),
                true,
                0,
                valid.clone(),
            ),
            (None, None, true, 0, valid.clone()),
            (Some("default"), Some("default"), false, 0, valid.clone()),
            (Some("default"), Some("default"), true, 1, valid.clone()),
            (
                Some("default"),
                Some("default"),
                true,
                0,
                gap(
                    HyperliquidPrivateStateSource::SpotPositions,
                    HyperliquidPrivateStateGapKind::UnknownInstrument,
                    None,
                ),
            ),
            (
                Some("default"),
                Some("default"),
                true,
                0,
                gap(
                    HyperliquidPrivateStateSource::SpotPositions,
                    HyperliquidPrivateStateGapKind::UnknownInstrument,
                    Some(0),
                ),
            ),
            (
                Some("default"),
                Some("default"),
                true,
                0,
                gap(
                    HyperliquidPrivateStateSource::OpenOrders,
                    HyperliquidPrivateStateGapKind::UnknownInstrument,
                    Some(77),
                ),
            ),
            (
                Some("default"),
                Some("default"),
                true,
                0,
                gap(
                    HyperliquidPrivateStateSource::SpotPositions,
                    HyperliquidPrivateStateGapKind::ParseFailure,
                    Some(77),
                ),
            ),
        ] {
            assert!(
                !HyperliquidIncompleteMassStatus::new(status(), vec![finding], omitted)
                    .prove_native_perpetual_scope(start, end, fresh)
                    .native_perpetuals_complete()
            );
        }
    }

    #[rstest]
    fn private_state_gap_cap_is_deterministic() {
        let mut batch = HyperliquidPrivateStateBatch::<()>::default();
        for index in 0..(PRIVATE_STATE_GAP_LIMIT + 3) {
            batch.push_gap(HyperliquidPrivateStateGap::new(
                HyperliquidPrivateStateSource::OpenOrders,
                HyperliquidPrivateStateGapKind::UnknownInstrument,
                None,
                Some(&format!("coin-{index}")),
                "not cached",
            ));
        }

        let (_, gaps, omitted) = batch.into_parts();
        assert_eq!(gaps.len(), PRIVATE_STATE_GAP_LIMIT);
        assert_eq!(gaps.first().and_then(|gap| gap.identity()), Some("coin-0"));
        assert_eq!(gaps.last().and_then(|gap| gap.identity()), Some("coin-63"));
        assert_eq!(omitted, 3);
    }

    #[rstest]
    fn private_state_gap_strings_are_bounded() {
        let long = "x".repeat(PRIVATE_STATE_DETAIL_LIMIT + 10);
        let gap = HyperliquidPrivateStateGap::new(
            HyperliquidPrivateStateSource::Fills,
            HyperliquidPrivateStateGapKind::ParseFailure,
            Some(&long),
            Some(&long),
            &long,
        );

        assert_eq!(
            gap.dex().unwrap().chars().count(),
            PRIVATE_STATE_IDENTITY_LIMIT
        );
        assert_eq!(
            gap.identity().unwrap().chars().count(),
            PRIVATE_STATE_IDENTITY_LIMIT
        );
        assert_eq!(gap.detail().chars().count(), PRIVATE_STATE_DETAIL_LIMIT);
    }
}
