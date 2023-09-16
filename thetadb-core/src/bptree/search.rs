/// Result of a binary search algorithm.
///
/// # Cases
///
/// * `Ok(index)` - The index of the key if found.
/// * `Err(index)` - The index where the key should be inserted.
pub(crate) type SearchIndex = Result<usize, usize>;

/// Generates code for a binary search algorithm that searches for a given key in a collection.
///
/// Returns the index of the key if found, or and error indicating where the key should be
/// inserted to maintain a sorted order.
///
/// # Arguments
///
/// * `key` - The value you're searching for in the collection.
/// * `len` - The length of the collection.
/// * `obtain` - An expression that takes an index and returns the value at that index in the collection.
///
/// # Examples
///
/// ```
/// use thetadb::search;
///
/// let slice = &[0, 2, 4, 6, 8];
///
/// assert_eq!(
///     Ok(1),
///     search!(2, slice.len(), idx => slice[idx]),
/// );
/// assert_eq!(
///     Err(2),
///     search!(3, slice.len(), idx => slice[idx]),
/// );
/// ```
#[macro_export]
macro_rules! search {
    ($key:expr, $len:expr, $index:ident => $obtain:expr) => {{
        let mut result = None;
        let mut range = 0..$len;

        while !range.is_empty() {
            let $index = (range.start + range.end) / 2;
            let record = $obtain;

            if record < $key {
                range.start = $index + 1;
            } else if record > $key {
                range.end = $index;
            } else {
                result = Some($index);
                break;
            }
        }

        result.ok_or_else(|| range.start)
    }};
}

#[cfg(test)]
mod tests {
    use super::SearchIndex;

    #[test]
    fn test_binary_search() {
        fn search<T: PartialOrd>(key: T, slice: &[T]) -> SearchIndex {
            search!(&key, slice.len(), idx => &slice[idx])
        }

        assert_eq!(search(1, &[1, 2, 3, 4, 5]), Ok(0));
        assert_eq!(search(2, &[1, 2, 3, 4, 5]), Ok(1));
        assert_eq!(search(3, &[1, 2, 3, 4, 5]), Ok(2));

        assert_eq!(search(4, &[1, 2, 3, 4, 5, 6]), Ok(3));
        assert_eq!(search(5, &[1, 2, 3, 4, 5, 6]), Ok(4));

        assert_eq!(search(0, &[1, 3, 5, 7]), Err(0));
        assert_eq!(search(2, &[1, 3, 5, 7]), Err(1));
        assert_eq!(search(8, &[1, 3, 5, 7]), Err(4));

        assert_eq!(search(6, &[1, 3, 5, 7, 9]), Err(3));
        assert_eq!(search(8, &[1, 3, 5, 7, 9]), Err(4));
    }
}
