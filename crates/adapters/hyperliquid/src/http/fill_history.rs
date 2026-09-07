//! Bounded, overlapping traversal of the venue's retained account fill history.

use std::collections::BTreeMap;

use serde_json::Value;

pub(super) const FILL_PAGE_LIMIT: usize = 2_000;
const FILL_RETENTION_LIMIT: usize = 10_000;
pub(super) const FILL_HISTORY_REQUEST_LIMIT: usize = 12;

/// Walk from the retained beginning so a short requested interval cannot hide
/// eviction at the 10,000-fill retention boundary. Keep the end fixed even while
/// newer fills arrive. Every full page is repeated at its final timestamp;
/// advancing by one millisecond would silently lose equal-timestamp fills.
pub(super) struct FillHistory {
    requested_start: u64,
    end: u64,
    cursor: u64,
    records: BTreeMap<String, Value>,
    boundary: Vec<String>,
    oldest: Option<u64>,
}

impl FillHistory {
    pub(super) fn new(requested_start: u64, end: u64) -> Self {
        Self {
            requested_start,
            end,
            cursor: 0,
            records: BTreeMap::new(),
            boundary: Vec::new(),
            oldest: None,
        }
    }

    pub(super) const fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return true only when the retained history and requested start are proven.
    pub(super) fn accept(&mut self, response: Value) -> Result<bool, String> {
        let page = response.as_array().ok_or("Expected a fill array")?;
        if page.len() > FILL_PAGE_LIMIT {
            return Err("Fill page exceeds the venue response limit".into());
        }
        let mut page_keys = BTreeMap::new();
        let mut newest = self.cursor;
        for value in page {
            let time = value
                .get("time")
                .and_then(Value::as_u64)
                .ok_or("Fill has no exact timestamp")?;
            if time < self.cursor || time > self.end {
                return Err("Fill falls outside the fixed history interval".into());
            }
            let key = fill_identity(value)?;
            if let Some(previous) = self.records.get(&key) {
                if previous != value {
                    // Neither version has authority when this snapshot
                    // contradicts itself. Do not expose the first one as a
                    // known report through an incomplete mass-status result.
                    self.records.remove(&key);
                    return Err("Conflicting payloads for the same venue fill identity".into());
                }
            } else {
                if self.records.len() == FILL_RETENTION_LIMIT {
                    return Err("Fill traversal exceeds the retained-history budget".into());
                }
                self.records.insert(key.clone(), value.clone());
            }
            page_keys.insert(key, time);
            self.oldest = Some(self.oldest.map_or(time, |oldest| oldest.min(time)));
            newest = newest.max(time);
        }
        if self.boundary.iter().any(|key| !page_keys.contains_key(key)) {
            return Err("Fill history lost the overlapping page boundary".into());
        }
        if page.len() < FILL_PAGE_LIMIT {
            if self.records.len() >= FILL_RETENTION_LIMIT
                && self
                    .oldest
                    .is_some_and(|oldest| self.requested_start <= oldest)
            {
                return Err("Requested fill history reaches the venue retention boundary".into());
            }
            return Ok(true);
        }
        if newest <= self.cursor {
            return Err("Saturated fill timestamp cannot advance safely".into());
        }
        self.cursor = newest;
        self.boundary = page_keys
            .into_iter()
            .filter_map(|(key, time)| (time == newest).then_some(key))
            .collect();
        Ok(false)
    }

    pub(super) fn into_records(self) -> Vec<Value> {
        let mut values: Vec<_> = self.records.into_values().collect();
        values.sort_by_key(|value| {
            (
                value.get("time").and_then(Value::as_u64),
                value.get("oid").and_then(Value::as_u64),
                value.get("tid").and_then(Value::as_u64),
            )
        });
        values
    }
}

fn fill_identity(value: &Value) -> Result<String, String> {
    let coin = value
        .get("coin")
        .and_then(Value::as_str)
        .ok_or("Fill has no coin")?;
    let oid = value
        .get("oid")
        .and_then(Value::as_u64)
        .ok_or("Fill has no order identity")?;
    if let Some(tid) = value.get("tid").and_then(Value::as_u64) {
        return Ok(format!("{coin}:{oid}:tid:{tid}"));
    }
    // Preserve compatibility with venue records lacking tid. Do not include
    // price, size or fees: changed economics must conflict rather than duplicate.
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .ok_or("Fill has no hash")?;
    let time = value
        .get("time")
        .and_then(Value::as_u64)
        .ok_or("Fill has no time")?;
    let position = value
        .get("startPosition")
        .and_then(Value::as_str)
        .ok_or("Fill has no starting position")?;
    Ok(format!("{coin}:{oid}:{hash}:{time}:{position}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn fill(id: u64, time: u64) -> Value {
        json!({"coin": "BTC", "oid": id, "tid": id, "time": time, "fee": "0.1"})
    }

    #[test]
    fn pagination_keeps_equal_timestamp_fills_and_exact_duplicates() {
        let mut history = FillHistory::new(0, 10_000);
        let first: Vec<_> = (0..2_000).map(|id| fill(id, id / 2)).collect();
        assert!(!history.accept(json!(first)).unwrap());
        assert_eq!(history.cursor(), 999);
        assert!(
            history
                .accept(json!([fill(1998, 999), fill(1999, 999), fill(2000, 999)]))
                .unwrap()
        );
        assert_eq!(history.into_records().len(), 2_001);
    }

    #[test]
    fn saturated_equal_timestamp_holds_without_skipping_a_millisecond() {
        let mut history = FillHistory::new(0, 10);
        let page: Vec<_> = (0..2_000).map(|id| fill(id, 1)).collect();
        assert!(!history.accept(json!(page)).unwrap());
        assert!(
            history
                .accept(json!(page))
                .unwrap_err()
                .contains("cannot advance")
        );
    }

    #[test]
    fn changed_fill_fee_is_a_conflict() {
        let mut history = FillHistory::new(0, 10_000);
        let page: Vec<_> = (0..2_000).map(|id| fill(id, id)).collect();
        assert!(!history.accept(json!(page)).unwrap());
        let mut changed = fill(1999, 1999);
        changed["fee"] = json!("0.2");
        assert!(
            history
                .accept(json!([changed]))
                .unwrap_err()
                .contains("Conflicting")
        );
        assert_eq!(history.into_records().len(), 1_999);
    }

    #[test]
    fn identical_duplicates_within_one_page_are_applied_once() {
        let mut history = FillHistory::new(0, 10);
        assert!(history.accept(json!([fill(1, 1), fill(1, 1)])).unwrap());
        assert_eq!(history.into_records().len(), 1);
    }

    #[test]
    fn missing_overlap_and_out_of_interval_records_hold() {
        let mut history = FillHistory::new(0, 10_000);
        let page: Vec<_> = (0..2_000).map(|id| fill(id, id)).collect();
        assert!(!history.accept(json!(page)).unwrap());
        assert!(
            history
                .accept(json!([fill(2000, 2000)]))
                .unwrap_err()
                .contains("boundary")
        );
        let mut history = FillHistory::new(0, 1);
        assert!(
            history
                .accept(json!([fill(1, 2)]))
                .unwrap_err()
                .contains("interval")
        );
    }

    #[test]
    fn retention_boundary_is_incomplete_only_when_required_interval_reaches_it() {
        for (start, expected_complete) in [(0, false), (1, false), (2, true)] {
            let mut history = FillHistory::new(start, 20_000);
            loop {
                let cursor = history.cursor().max(1);
                let page: Vec<_> = (cursor..=10_000)
                    .take(FILL_PAGE_LIMIT)
                    .map(|id| fill(id, id))
                    .collect();
                match history.accept(json!(page)) {
                    Ok(false) => continue,
                    result => {
                        assert_eq!(result.is_ok(), expected_complete);
                        break;
                    }
                }
            }
        }
    }
}
