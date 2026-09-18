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

//! Deterministic derivation of a venue client order ID (*cloid*) from a [`ClientOrderId`].
//!
//! Some venues, Hyperliquid among them, name an order by a 128-bit client order ID rather than
//! by our own [`ClientOrderId`] string. The adapter derives that name deterministically as
//! `keccak256(client_order_id)[..16]`, rendered as `0x`-prefixed lowercase hex.
//!
//! The derivation lives here, rather than only in the adapter, because the cache needs it to
//! recognize one specific thing: an order the venue reported under its raw cloid alone, adopted
//! as an *external* order whose client order ID *is* that cloid, is our own order under the
//! venue's name. Because keccak256 is preimage resistant, a client order ID that derives to
//! exactly that string is proof of identity rather than a resemblance.
//!
//! Nothing here depends on an adapter crate, and the adapter derives its cloids through these
//! functions so the two can never drift apart.

use alloy_primitives::keccak256;
use nautilus_core::hex;
use nautilus_model::identifiers::ClientOrderId;

/// The byte length of a venue cloid (128 bits).
pub const CLOID_LEN: usize = 16;

/// The character length of a `0x`-prefixed hex cloid.
pub const CLOID_HEX_LEN: usize = 2 + CLOID_LEN * 2;

/// Derives the raw venue cloid bytes for `client_order_id`.
#[must_use]
pub fn derive_cloid(client_order_id: &ClientOrderId) -> [u8; CLOID_LEN] {
    let hash = keccak256(client_order_id.as_str().as_bytes());
    let mut bytes = [0u8; CLOID_LEN];
    bytes.copy_from_slice(&hash[..CLOID_LEN]);
    bytes
}

/// Derives the venue cloid for `client_order_id` as `0x`-prefixed lowercase hex.
#[must_use]
pub fn derive_cloid_hex(client_order_id: &ClientOrderId) -> String {
    hex::encode_prefixed(derive_cloid(client_order_id))
}

/// Returns whether `candidate` is exactly the venue cloid that `client_order_id` derives to.
///
/// The relation is one directional: `candidate` is the venue's name for `client_order_id`, never
/// the other way around, so it can never be used to argue the two IDs are interchangeable.
#[must_use]
pub fn is_derived_cloid_of(candidate: &ClientOrderId, client_order_id: &ClientOrderId) -> bool {
    let candidate = candidate.as_str();

    candidate.len() == CLOID_HEX_LEN && candidate == derive_cloid_hex(client_order_id)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn derive_cloid_matches_the_keccak256_test_vector() {
        // Keccak-256("abc") is
        // 4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45, whose first 16
        // bytes are the cloid. This pins the derivation to Keccak-256 as Ethereum defines it,
        // not to NIST SHA3-256, which pads differently and would hash "abc" to
        // 3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532.
        let client_order_id = ClientOrderId::new("abc");

        assert_eq!(
            derive_cloid_hex(&client_order_id),
            "0x4e03657aea45a94fc7d47ba826c8d667"
        );
    }

    #[rstest]
    fn derive_cloid_hex_is_prefixed_lowercase_and_deterministic() {
        let client_order_id = ClientOrderId::new("O-19700101-000000-001-001-1");

        let hex = derive_cloid_hex(&client_order_id);

        assert_eq!(hex.len(), CLOID_HEX_LEN);
        assert!(hex.starts_with("0x"));
        assert!(
            hex.chars()
                .skip(2)
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(hex, derive_cloid_hex(&client_order_id));
    }

    #[rstest]
    fn different_client_order_ids_derive_different_cloids() {
        let one = ClientOrderId::new("STP-7fb04249d5880da9e30f2583");
        let other = ClientOrderId::new("STP-7fb04249d5880da9e30f2584");

        assert_ne!(derive_cloid(&one), derive_cloid(&other));
    }

    #[rstest]
    fn is_derived_cloid_of_holds_only_in_one_direction() {
        let client_order_id = ClientOrderId::new("STP-7fb04249d5880da9e30f2583");
        let cloid = ClientOrderId::new(derive_cloid_hex(&client_order_id));

        assert!(is_derived_cloid_of(&cloid, &client_order_id));
        assert!(!is_derived_cloid_of(&client_order_id, &cloid));
        assert!(!is_derived_cloid_of(&client_order_id, &client_order_id));
    }

    #[rstest]
    fn an_unrelated_hex_id_is_not_a_derived_cloid() {
        let client_order_id = ClientOrderId::new("STP-7fb04249d5880da9e30f2583");
        let unrelated = ClientOrderId::new("0x0012329971d895b1c9af61048aa31d87");

        assert!(!is_derived_cloid_of(&unrelated, &client_order_id));
    }
}
