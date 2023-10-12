use serde::{Deserialize, Serialize};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

/// Range map is semantically similar to BTreeMap as it maps some ordered keys to values.
/// However, range map efficiently compacts the range of keys with same value into a single entry.
/// Performance of such map depends a lot on the fragmentation of the keys.
///
/// Range map is very efficient when many continuous keys maps to the same value.
/// When keys are fragmented, the performance of RangeMap is similar to that of BTreeMap.
#[derive(Serialize, Deserialize, Default)]
pub struct RangeMap<K: Ord, V> {
    map: BTreeMap<K, RangeItem<K, V>>,
}

#[derive(Serialize, Deserialize)]
struct RangeItem<K, V> {
    start_included: K,
    end_excluded: K,
    v: Option<V>,
}

impl<K: Ord + Copy + PartialEq, V: Clone> RangeMap<K, V> {
    /// Updates the range given a mutation function.
    /// Mutation function receives the sub-range and a mutable reference to the corresponding value.
    /// If value in the sub-range was not defined before, the value contains None.
    /// If mutation function sets value to None, the entry is deleted.
    ///
    /// Mutation function may be invoked multiple times if multiple ranges overlap with the given range.
    #[allow(clippy::comparison_chain)]
    pub fn mutate_range<F>(&mut self, mut range: Range<K>, mut f: F)
    where
        F: FnMut(Range<K>, &mut Option<V>),
    {
        loop {
            // Finds range item that starts with given key K.
            //
            // If some range(there can only be at most one) starts with k, return that range.
            //
            // If some range contains k(there can only be at most one), but does not start with k,
            // split this range so that second half starts with k, return that second half.
            //
            // If no range contains k, return None.
            //
            // Unfortunately, with borrow check limitations this can not be easily moved into a separate function
            let start = {
                let k = range.start;
                let lower_bound = self.map.range_mut(..=k).last();
                if let Some((lower_bound_k, lower_bound_v)) = lower_bound {
                    if *lower_bound_k == k {
                        Some(lower_bound_v)
                    } else if lower_bound_v.end_excluded > k {
                        // Split the range
                        let new_item = RangeItem {
                            start_included: k,
                            end_excluded: lower_bound_v.end_excluded,
                            v: lower_bound_v.v.clone(),
                        };
                        lower_bound_v.end_excluded = k;
                        let Entry::Vacant(va) = self.map.entry(k) else {
                            panic!("Entry must be vacant")
                        };
                        Some(va.insert(new_item))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            // After this block overlap contains borrow from existing map that overlaps with requested range
            // and next_range (Optionally) contains range that has not yet been processed
            let mut next_range = None;
            let overlap = if let Some(start) = start {
                if start.end_excluded == range.end {
                    start
                } else if start.end_excluded > range.end {
                    // Start range includes requested range, split it
                    let new_key = range.end;
                    let new_item = RangeItem {
                        start_included: new_key,
                        end_excluded: start.end_excluded,
                        v: start.v.clone(),
                    };
                    start.end_excluded = range.end;
                    self.map.insert(new_key, new_item);
                    self.map.get_mut(&range.start).unwrap() // Re-borrowing same value
                } else {
                    // New range requests part of start range and some other range
                    next_range = Some(start.end_excluded..range.end);
                    start
                }
            } else {
                // At this point no existing range contains the start of requested range
                // But is there some range in the map that contains the tail of requested range?
                let upper_bound = self.map.range(range.clone()).next();
                if let Some((upper_bound_start, _)) = upper_bound {
                    next_range = Some(*upper_bound_start..range.end);
                    let new_item = RangeItem {
                        start_included: range.start,
                        end_excluded: *upper_bound_start,
                        v: None,
                    };
                    let Entry::Vacant(va) = self.map.entry(range.start) else {
                        panic!("Entry must be vacant")
                    };
                    va.insert(new_item)
                } else {
                    // No overlap at all with existing ranges
                    let new_item = RangeItem {
                        start_included: range.start,
                        end_excluded: range.end,
                        v: None,
                    };
                    let Entry::Vacant(va) = self.map.entry(range.start) else {
                        panic!("Entry must be vacant")
                    };
                    va.insert(new_item)
                }
            };
            f(overlap.start_included..overlap.end_excluded, &mut overlap.v);
            if overlap.v.is_none() {
                // Remove key, unwrap ensures invariant that this key exists
                let remove = overlap.start_included;
                self.map.remove(&remove).unwrap();
            }
            if let Some(next_range) = next_range {
                range = next_range;
            } else {
                break;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K: fmt::Debug + Ord, V: fmt::Debug> fmt::Debug for RangeMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for range in self.map.values() {
            write!(
                f,
                "{:?}..{:?}:{:?}",
                range.start_included,
                range.end_excluded,
                range
                    .v
                    .as_ref()
                    .expect("Should not persist values with None")
            )?;
        }
        write!(f, "}}")
    }
}

impl<K: fmt::Display + Ord, V: fmt::Display> fmt::Display for RangeMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for range in self.map.values() {
            write!(
                f,
                "{}..{}:{},",
                range.start_included,
                range.end_excluded,
                range
                    .v
                    .as_ref()
                    .expect("Should not persist values with None")
            )?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_map() {
        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(1..2, |_, s| replace_check(s, None, Some(11)));
        assert_eq!(range_map.to_string(), "{1..2:11,5..10:18,}");
        range_map.mutate_range(1..2, |_, s| replace_check(s, Some(11), Some(12)));
        assert_eq!(range_map.to_string(), "{1..2:12,5..10:18,}");
        range_map.mutate_range(2..5, |_, s| replace_check(s, None, Some(14)));
        assert_eq!(range_map.to_string(), "{1..2:12,2..5:14,5..10:18,}");
        range_map.mutate_range(3..8, |_, s| *s = Some(22));
        assert_eq!(
            range_map.to_string(),
            "{1..2:12,2..3:14,3..5:22,5..8:22,8..10:18,}"
        );

        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(5..6, |_, s| replace_check(s, Some(18), Some(19)));
        assert_eq!(range_map.to_string(), "{5..6:19,6..10:18,}");

        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(6..8, |_, s| replace_check(s, Some(18), Some(19)));
        assert_eq!(range_map.to_string(), "{5..6:18,6..8:19,8..10:18,}");

        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(6..12, |_, s| *s = Some(19));
        assert_eq!(range_map.to_string(), "{5..6:18,6..10:19,10..12:19,}");

        // Test deletion
        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(6..8, |_, s| replace_check(s, Some(18), None));
        assert_eq!(range_map.to_string(), "{5..6:18,8..10:18,}");

        let mut range_map: RangeMap<u64, u64> = RangeMap::default();
        range_map.mutate_range(5..10, |_, s| replace_check(s, None, Some(18)));
        assert_eq!(range_map.to_string(), "{5..10:18,}");
        range_map.mutate_range(5..10, |_, s| replace_check(s, Some(18), None));
        assert_eq!(range_map.to_string(), "{}");
    }

    #[track_caller]
    fn replace_check(r: &mut Option<u64>, expect: Option<u64>, new: Option<u64>) {
        assert_eq!(*r, expect);
        *r = new;
    }
}
