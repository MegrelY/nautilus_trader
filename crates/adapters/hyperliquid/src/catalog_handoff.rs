// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded one-shot catalog handoff between co-located data and execution clients.

use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

use nautilus_core::MUTEX_POISONED;
use nautilus_model::{identifiers::InstrumentId, instruments::InstrumentAny};

use crate::common::enums::HyperliquidEnvironment;

const CATALOG_HANDOFF_LIMIT: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogHandoffKey {
    environment: HyperliquidEnvironment,
    http_url: String,
    proxy_url: Option<String>,
}

impl CatalogHandoffKey {
    pub(crate) fn new(
        environment: HyperliquidEnvironment,
        http_url: String,
        proxy_url: Option<String>,
    ) -> Self {
        Self {
            environment,
            http_url,
            proxy_url,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CatalogHandoff {
    pub(crate) requested_instrument_ids: Vec<InstrumentId>,
    pub(crate) instruments: Vec<InstrumentAny>,
}

#[derive(Default)]
struct CatalogHandoffStore {
    entries: VecDeque<(CatalogHandoffKey, CatalogHandoff)>,
}

impl CatalogHandoffStore {
    fn publish(&mut self, key: CatalogHandoffKey, handoff: CatalogHandoff) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|(candidate, _)| candidate == &key)
        {
            self.entries.remove(index);
        }
        self.entries.push_back((key, handoff));
        while self.entries.len() > CATALOG_HANDOFF_LIMIT {
            self.entries.pop_front();
        }
    }

    fn take(&mut self, key: &CatalogHandoffKey) -> Option<CatalogHandoff> {
        let index = self
            .entries
            .iter()
            .position(|(candidate, _)| candidate == key)?;
        self.entries.remove(index).map(|(_, handoff)| handoff)
    }
}

fn catalog_handoffs() -> &'static Mutex<CatalogHandoffStore> {
    static STORE: OnceLock<Mutex<CatalogHandoffStore>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(CatalogHandoffStore::default()))
}

pub(crate) fn publish_catalog_handoff(key: CatalogHandoffKey, handoff: CatalogHandoff) {
    catalog_handoffs()
        .lock()
        .expect(MUTEX_POISONED)
        .publish(key, handoff);
}

pub(crate) fn take_catalog_handoff(key: &CatalogHandoffKey) -> Option<CatalogHandoff> {
    catalog_handoffs().lock().expect(MUTEX_POISONED).take(key)
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    fn key(index: usize) -> CatalogHandoffKey {
        CatalogHandoffKey::new(
            HyperliquidEnvironment::Testnet,
            format!("http://catalog-{index}/info"),
            None,
        )
    }

    fn handoff(index: usize) -> CatalogHandoff {
        CatalogHandoff {
            requested_instrument_ids: vec![InstrumentId::from(format!(
                "ASSET{index}-USD-PERP.HYPERLIQUID"
            ))],
            instruments: Vec::new(),
        }
    }

    #[rstest]
    fn catalog_handoff_is_one_shot_and_bounded() {
        let mut store = CatalogHandoffStore::default();
        for index in 0..=CATALOG_HANDOFF_LIMIT {
            store.publish(key(index), handoff(index));
        }

        assert!(
            store.take(&key(0)).is_none(),
            "oldest entry must be evicted"
        );
        let latest = store.take(&key(CATALOG_HANDOFF_LIMIT)).unwrap();
        assert_eq!(latest.requested_instrument_ids.len(), 1);
        assert!(store.take(&key(CATALOG_HANDOFF_LIMIT)).is_none());
    }
}
