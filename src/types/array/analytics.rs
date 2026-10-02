/// Generates a same-variant `Column op Literal` comparison kernel
/// (`eq_vec`/`lt_vec`/…). `$ord_op`/`$ord_val` describe the cross-variant
/// fallback: it routes through `compare_scalar_at` and keeps the same
/// ordering predicate (e.g. `<=` means `ord != Ordering::Greater`).
macro_rules! cmp_vec_fn {
    ($fn_name:ident, $op:tt, $ord_op:tt, $ord_val:ident) => {
        pub fn $fn_name(&self, value: &ScalarValue) -> Vec<bool> {
            // Zero-copy views / dictionary encodings compare through their
            // materialized form; row indexes are preserved by `flattened`.
            if matches!(
                self,
                ColumnArray::Selected { .. } | ColumnArray::Dictionary { .. }
            ) {
                return self.flattened().$fn_name(value);
            }
            let num_rows = self.len();
            let bitmap = self.bitmap();
            match (self, value) {
                (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && (data[i] != 0) == *v)
                    .collect(),
                (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Int16 { data, .. }, ScalarValue::Int16(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Int8 { data, .. }, ScalarValue::Int8(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::UInt64 { data, .. }, ScalarValue::UInt64(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::UInt32 { data, .. }, ScalarValue::UInt32(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::UInt16 { data, .. }, ScalarValue::UInt16(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::UInt8 { data, .. }, ScalarValue::UInt8(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Float64 { data, .. }, ScalarValue::Float64(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Float32 { data, .. }, ScalarValue::Float32(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Decimal { data, .. }, ScalarValue::Decimal(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Date { data, .. }, ScalarValue::Date(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Time { data, .. }, ScalarValue::Time(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                    .collect(),
                (ColumnArray::Timestamp { data, .. }, ScalarValue::Timestamp(Some(v))) => {
                    (0..num_rows)
                        .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                        .collect()
                }
                (ColumnArray::Interval { data, .. }, ScalarValue::Interval(Some(v))) => {
                    (0..num_rows)
                        .map(|i| !bitmap.is_null(i) && data[i] $op *v)
                        .collect()
                }
                (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                    .map(|i| !bitmap.is_null(i) && data.value(i) $op v.as_str())
                    .collect(),
                _ if value.numeric_f64().is_some() => (0..num_rows)
                    .map(|i| {
                        !bitmap.is_null(i)
                            && self
                                .compare_scalar_at(i, value)
                                .is_some_and(|ord| ord $ord_op std::cmp::Ordering::$ord_val)
                    })
                    .collect(),
                _ => vec![false; num_rows],
            }
        }
    };
}

// One type table drives BOTH equality-count functions. The deleted
// variant used to cover only Int64/Int32/VarChar while count_eq covered
// every type, so the COUNT fast path's `raw - deleted` arithmetic silently
// counted deleted rows for every other column type.
macro_rules! impl_eq_counts {

        ($($variant:ident => $scalar:ident),+ $(,)?) => {
            pub fn count_eq(&self, value: &ScalarValue) -> u64 {
                let num_rows = self.len();
                let bitmap = self.bitmap();
                match (self, value) {
                    $(
                        (ColumnArray::$variant { data, .. }, ScalarValue::$scalar(Some(v))) => {
                            (0..num_rows)
                                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                                .count() as u64
                        }
                    )+
                    (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => (0..num_rows)
                        .filter(|&i| !bitmap.is_null(i) && (data[i] != 0) == *v)
                        .count() as u64,
                    _ => 0,
                }
            }

            /// Count `count_eq`-matching rows whose absolute row id is marked
            /// deleted. Type coverage MUST stay identical to [`Self::count_eq`]
            /// — both are generated from the same table above for that reason.
            pub fn count_eq_deleted(
                &self,
                value: &ScalarValue,
                rg_start_row: u64,
                delete_bitmap: &[u8],
            ) -> u64 {
                let num_rows = self.len();
                let bitmap = self.bitmap();
                let mut deleted_count: u64 = 0;
                match (self, value) {
                    $(
                        (ColumnArray::$variant { data, .. }, ScalarValue::$scalar(Some(v))) => {
                            for i in 0..num_rows {
                                if !bitmap.is_null(i) && data[i] == *v {
                                    let row_id = rg_start_row + i as u64;
                                    let byte = (row_id / 8) as usize;
                                    let bit = (row_id % 8) as u8;
                                    if delete_bitmap
                                        .get(byte)
                                        .is_some_and(|b| (b & (1u8 << bit)) != 0)
                                    {
                                        deleted_count += 1;
                                    }
                                }
                            }
                        }
                    )+
                    (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => {
                        for i in 0..num_rows {
                            if !bitmap.is_null(i) && (data[i] != 0) == *v {
                                let row_id = rg_start_row + i as u64;
                                let byte = (row_id / 8) as usize;
                                let bit = (row_id % 8) as u8;
                                if delete_bitmap
                                    .get(byte)
                                    .is_some_and(|b| (b & (1u8 << bit)) != 0)
                                {
                                    deleted_count += 1;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                deleted_count
            }
        };
}

impl ColumnArray {
    impl_eq_counts!(
        Int64 => Int64,
        Int32 => Int32,
        Int16 => Int16,
        Int8 => Int8,
        UInt64 => UInt64,
        UInt32 => UInt32,
        UInt16 => UInt16,
        UInt8 => UInt8,
        VarChar => VarChar,
        Float64 => Float64,
        Float32 => Float32,
        Decimal => Decimal,
        Date => Date,
        Timestamp => Timestamp,
        Time => Time,
        Interval => Interval,
    );

    /// Column-wide minimum over non-null values (vectorized, no per-cell boxing).
    /// Returns None when the column has no non-null values.
    pub fn col_min(&self) -> Option<ScalarValue> {
        let n = self.len();
        let bitmap = self.bitmap();
        macro_rules! numeric_min {
            ($data:expr, $variant:ident) => {{
                let mut best: Option<_> = None;
                for (i, v) in $data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if &b <= v => b,
                            _ => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::$variant(Some(v)))
            }};
        }
        match self {
            ColumnArray::Selected { .. } | ColumnArray::Dictionary { .. } | ColumnArray::Rle { .. } => {
                self.flattened().col_min()
            }
            ColumnArray::Int64 { data, .. } => numeric_min!(data, Int64),
            ColumnArray::Int32 { data, .. } => numeric_min!(data, Int32),
            ColumnArray::Int16 { data, .. } => numeric_min!(data, Int16),
            ColumnArray::Int8 { data, .. } => numeric_min!(data, Int8),
            ColumnArray::UInt64 { data, .. } => numeric_min!(data, UInt64),
            ColumnArray::UInt32 { data, .. } => numeric_min!(data, UInt32),
            ColumnArray::UInt16 { data, .. } => numeric_min!(data, UInt16),
            ColumnArray::UInt8 { data, .. } => numeric_min!(data, UInt8),
            ColumnArray::Float64 { data, .. } => {
                let mut best: Option<f64> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) => b.min(*v),
                            None => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::Float64(Some(v)))
            }
            ColumnArray::Float32 { data, .. } => {
                let mut best: Option<f32> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) => b.min(*v),
                            None => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::Float32(Some(v)))
            }
            ColumnArray::Decimal { data, .. } => numeric_min!(data, Decimal),
            ColumnArray::Date { data, .. } => numeric_min!(data, Date),
            ColumnArray::Time { data, .. } => numeric_min!(data, Time),
            ColumnArray::Timestamp { data, .. } => numeric_min!(data, Timestamp),
            ColumnArray::Interval { data, .. } => numeric_min!(data, Interval),
            ColumnArray::Bool { data, .. } => {
                let mut saw_false = false;
                let mut saw_true = false;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        if *v != 0 {
                            saw_true = true;
                        } else {
                            saw_false = true;
                        }
                    }
                }
                if saw_false {
                    Some(ScalarValue::Bool(Some(false)))
                } else if saw_true {
                    Some(ScalarValue::Bool(Some(true)))
                } else {
                    None
                }
            }
            ColumnArray::VarChar { data, .. } => {
                let mut best: Option<&str> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if b <= v => b,
                            _ => v,
                        });
                    }
                }
                best.map(|v| ScalarValue::VarChar(Some(v.to_owned())))
            }
            ColumnArray::Blob { .. } | ColumnArray::Vector { .. } => None,
        }
    }

    /// Column-wide maximum over non-null values (vectorized, no per-cell boxing).
    pub fn col_max(&self) -> Option<ScalarValue> {
        let n = self.len();
        let bitmap = self.bitmap();
        macro_rules! numeric_max {
            ($data:expr, $variant:ident) => {{
                let mut best: Option<_> = None;
                for (i, v) in $data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if &b >= v => b,
                            _ => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::$variant(Some(v)))
            }};
        }
        match self {
            ColumnArray::Selected { .. } | ColumnArray::Dictionary { .. } | ColumnArray::Rle { .. } => {
                self.flattened().col_max()
            }
            ColumnArray::Int64 { data, .. } => numeric_max!(data, Int64),
            ColumnArray::Int32 { data, .. } => numeric_max!(data, Int32),
            ColumnArray::Int16 { data, .. } => numeric_max!(data, Int16),
            ColumnArray::Int8 { data, .. } => numeric_max!(data, Int8),
            ColumnArray::UInt64 { data, .. } => numeric_max!(data, UInt64),
            ColumnArray::UInt32 { data, .. } => numeric_max!(data, UInt32),
            ColumnArray::UInt16 { data, .. } => numeric_max!(data, UInt16),
            ColumnArray::UInt8 { data, .. } => numeric_max!(data, UInt8),
            ColumnArray::Float64 { data, .. } => {
                let mut best: Option<f64> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) => b.max(*v),
                            None => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::Float64(Some(v)))
            }
            ColumnArray::Float32 { data, .. } => {
                let mut best: Option<f32> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) => b.max(*v),
                            None => *v,
                        });
                    }
                }
                best.map(|v| ScalarValue::Float32(Some(v)))
            }
            ColumnArray::Decimal { data, .. } => numeric_max!(data, Decimal),
            ColumnArray::Date { data, .. } => numeric_max!(data, Date),
            ColumnArray::Time { data, .. } => numeric_max!(data, Time),
            ColumnArray::Timestamp { data, .. } => numeric_max!(data, Timestamp),
            ColumnArray::Interval { data, .. } => numeric_max!(data, Interval),
            ColumnArray::Bool { data, .. } => {
                let mut saw_false = false;
                let mut saw_true = false;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        if *v != 0 {
                            saw_true = true;
                        } else {
                            saw_false = true;
                        }
                    }
                }
                if saw_true {
                    Some(ScalarValue::Bool(Some(true)))
                } else if saw_false {
                    Some(ScalarValue::Bool(Some(false)))
                } else {
                    None
                }
            }
            ColumnArray::VarChar { data, .. } => {
                let mut best: Option<&str> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if b >= v => b,
                            _ => v,
                        });
                    }
                }
                best.map(|v| ScalarValue::VarChar(Some(v.to_owned())))
            }
            ColumnArray::Blob { .. } | ColumnArray::Vector { .. } => None,
        }
    }

    /// Count of non-null values in this column (vectorized).
    pub fn col_count_non_null(&self) -> u64 {
        (self.len() as u64).saturating_sub(self.bitmap().null_count() as u64)
    }

    /// Column-wide sum as i64 for integer columns (vectorized). Returns None if
    /// all values are null or the column type is not integer.
    pub fn col_sum_i64(&self) -> Option<i64> {
        self.col_sum_i64_range(0, self.len())
    }

    /// Sum of `[offset, offset + count)` as i64 (vectorized, wrapping),
    /// skipping null rows. Returns None when the range holds no non-null
    /// value or the column type is not i64-viewable — the same contract as
    /// [`Self::col_sum_i64`].
    pub fn col_sum_i64_range(&self, offset: usize, count: usize) -> Option<i64> {
        // Run-form columns read through their materialized expansion; the
        // lazy flatten is cached, so repeated range sums over the same column
        // expand once.
        if let ColumnArray::Rle { .. } = self {
            return self.flattened().col_sum_i64_range(offset, count);
        }
        let bitmap = self.bitmap();
        macro_rules! int_sum_range {
            ($data:expr, $cast:ty) => {{
                let data = $data.as_slice();
                let end = (offset + count).min(data.len());
                if offset >= end {
                    return None;
                }
                if bitmap.null_count() == 0 {
                    // Null-free fast path: a branch-free wrapping-add fold the
                    // compiler can auto-vectorize.
                    let mut sum: i64 = 0;
                    for &v in &data[offset..end] {
                        sum = sum.wrapping_add(v as $cast as i64);
                    }
                    Some(sum)
                } else {
                    sum_valid_range_words(data, bitmap, offset, end, |v| v as $cast as i64)
                }
            }};
        }
        match self {
            ColumnArray::Int64 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::Int32 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::Int16 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::Int8 { data, .. } => int_sum_range!(data, i64),
            // u64 saturates into i64 like the original whole-column kernel.
            ColumnArray::UInt64 { data, .. } => {
                let bitmap = self.bitmap();
                let values: &[u64] = data;
                let end = (offset + count).min(values.len());
                if offset >= end {
                    return None;
                }
                if bitmap.null_count() == 0 {
                    let mut sum: i64 = 0;
                    for &v in &values[offset..end] {
                        sum = sum.wrapping_add(i64::try_from(v).unwrap_or(i64::MAX));
                    }
                    Some(sum)
                } else {
                    sum_valid_range_words(values, bitmap, offset, end, |v| {
                        i64::try_from(v).unwrap_or(i64::MAX)
                    })
                }
            }
            ColumnArray::UInt32 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::UInt16 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::UInt8 { data, .. } => int_sum_range!(data, i64),
            ColumnArray::Decimal { data, .. } => int_sum_range!(data, i64),
            _ => None,
        }
    }

    /// Column-wide sum as f64 for float columns (vectorized). Returns None if
    /// all values are null or the column type is not float.
    pub fn col_sum_f64(&self) -> Option<f64> {
        if let ColumnArray::Rle { .. } = self {
            return self.flattened().col_sum_f64();
        }
        let bitmap = self.bitmap();
        match self {
            ColumnArray::Float64 { data, .. } => {
                if bitmap.null_count() == 0 {
                    (!data.is_empty()).then(|| data.iter().sum::<f64>())
                } else {
                    sum_valid_words_f64(data, bitmap, |v| *v)
                }
            }
            ColumnArray::Float32 { data, .. } => {
                if bitmap.null_count() == 0 {
                    (!data.is_empty()).then(|| data.iter().map(|&v| v as f64).sum())
                } else {
                    sum_valid_words_f64(data, bitmap, |v| *v as f64)
                }
            }
            _ => None,
        }
    }

    cmp_vec_fn!(eq_vec, ==, ==, Equal);
    cmp_vec_fn!(neq_vec, !=, !=, Equal);
    cmp_vec_fn!(lt_vec, <, ==, Less);
    cmp_vec_fn!(le_vec, <=, !=, Greater);
    cmp_vec_fn!(gt_vec, >, ==, Greater);
    cmp_vec_fn!(ge_vec, >=, !=, Less);

    pub fn is_null_vec(&self) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        (0..num_rows).map(|i| bitmap.is_null(i)).collect()
    }

    pub fn is_not_null_vec(&self) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        (0..num_rows).map(|i| !bitmap.is_null(i)).collect()
    }

    pub fn filter_by_mask(&self, mask: &[bool]) -> ColumnArray {
        let mut new_col = ColumnArray::empty(self.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&self.scalar(i)).unwrap();
            }
        }
        new_col
    }

    pub fn filter_by_mask_fast(&self, mask: &[bool], match_count: usize) -> ColumnArray {
        macro_rules! filter_copy_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let mut new_data = Vec::with_capacity(match_count);
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for (i, &keep) in mask.iter().enumerate() {
                    if keep {
                        new_data.push($data[i]);
                        if $bitmap.is_null(i) {
                            new_bitmap.push_null();
                        } else {
                            new_bitmap.push_valid();
                        }
                    }
                }
                ColumnArray::$variant {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }};
        }

        match self {
            ColumnArray::Bool { data, bitmap } => filter_copy_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => filter_copy_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => filter_copy_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => filter_copy_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => filter_copy_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => filter_copy_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => filter_copy_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => filter_copy_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => filter_copy_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => filter_copy_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => filter_copy_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => filter_copy_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => filter_copy_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => filter_copy_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => filter_copy_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => filter_copy_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => {
                let mut new_data = Utf8Buffer::with_capacity(match_count);
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for (i, &keep) in mask.iter().enumerate() {
                    if keep {
                        new_data.push_str(data.value(i));
                        if bitmap.is_null(i) {
                            new_bitmap.push_null();
                        } else {
                            new_bitmap.push_valid();
                        }
                    }
                }
                ColumnArray::VarChar {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
            _ => self.filter_by_mask(mask),
        }
    }

    /// Gather the rows at `selected` positions into a new column (typed
    /// copy, no per-row scalar boxing). `selected` must be sorted ascending
    /// and within bounds — callers build it from an ordered mask walk.
    pub fn gather_rows(&self, selected: &[usize]) -> ColumnArray {
        macro_rules! gather_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let mut new_data = Vec::with_capacity(selected.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &row in selected {
                    new_data.push($data[row]);
                    if $bitmap.is_null(row) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::$variant {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }};
        }

        match self {
            ColumnArray::Bool { data, bitmap } => gather_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => gather_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => gather_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => gather_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => gather_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => gather_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => gather_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => gather_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => gather_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => gather_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => gather_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => gather_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => gather_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => gather_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => gather_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => gather_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => {
                let mut new_data = Utf8Buffer::with_capacity(selected.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &row in selected {
                    new_data.push_str(data.value(row));
                    if bitmap.is_null(row) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::VarChar {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
            _ => {
                let mut new_col = ColumnArray::empty(self.data_type());
                for &row in selected {
                    new_col.push(&self.scalar(row)).unwrap();
                }
                new_col
            }
        }
    }

    pub fn slice_fast(&self, start: usize, end: usize) -> ColumnArray {
        macro_rules! slice_copy_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let new_data = $data[start..end].to_vec();
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for i in start..end {
                    if $bitmap.is_null(i) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::$variant {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }};
        }

        match self {
            ColumnArray::Bool { data, bitmap } => slice_copy_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => slice_copy_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => slice_copy_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => slice_copy_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => slice_copy_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => slice_copy_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => slice_copy_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => slice_copy_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => slice_copy_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => slice_copy_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => slice_copy_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => slice_copy_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => slice_copy_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => slice_copy_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => slice_copy_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => slice_copy_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => {
                let mut out = ColumnArray::with_capacity(crate::protocol::types::DataType::VarChar, end - start);
                for i in start..end {
                    if bitmap.is_null(i) { out.push_null_value(); } else { out.push(&ScalarValue::VarChar(Some(data.value(i).to_owned()))).unwrap(); }
                }
                out
            },
            _ => {
                let mut new_col = ColumnArray::empty(self.data_type());
                for i in start..end {
                    new_col.push(&self.scalar(i)).unwrap();
                }
                new_col
            }
        }
    }

    /// Gather rows at the given indices (random access). All indices must be
    /// `< self.len()`. This is the workhorse for the per-symbol inverted index
    /// gather path: instead of scanning the whole column it copies only the
    /// requested rows, turning a full-scan into an O(matched) operation.
    pub fn take(&self, indices: &[u32]) -> ColumnArray {
        let mut out = ColumnArray::empty(self.data_type());
        self.gather_into(indices, &mut out)
            .expect("take: gather_into cannot fail for a fresh column of the same type");
        out
    }

    /// Gather `indices` from `self` and append them to `dst`, which must hold
    /// the same data type. Fuses what `take` + `extend` used to do (two copies
    /// plus a temporary column) into a single pass that writes straight into
    /// the destination.
    ///
    /// Ascending index runs (posting lists are sorted, so full-chunk gathers
    /// are one run) are copied with `extend_from_slice`/bulk-valid bitmaps.
    pub fn gather_into(&self, indices: &[u32], dst: &mut ColumnArray) -> Result<()> {
        if self.data_type() != dst.data_type() {
            return Err(TaibaiError::Internal(format!(
                "gather_into: destination holds {} but source is {}",
                dst.data_type().display_name(),
                self.data_type().display_name()
            )));
        }
        if indices.is_empty() {
            return Ok(());
        }
        // A source column without nulls lets the gather extend the output
        // bitmap in bulk instead of a branch per row, which is the common case
        // for ingested hot data. Detecting "no nulls" scans the whole source
        // bitmap, so only take the fast path when the gather is dense enough
        // to amortize that scan; sparse gathers (e.g. ten rows out of a 64Ki
        // chunk) keep the per-row loop.
        let all_valid = indices.len() * 128 >= self.len() && self.bitmap().is_all_valid();

        // Ascending runs of indices are copied with memcpy; everything else
        // falls back to per-row pushes.
        macro_rules! gather_fixed {
            ($src_data:expr, $src_bitmap:expr, $dst_data:expr, $dst_bitmap:expr) => {{
                $dst_data.reserve($dst_data.len() + indices.len());
                if all_valid {
                    let mut i = 0usize;
                    while i < indices.len() {
                        let first = indices[i] as usize;
                        let mut run = 1usize;
                        while i + run < indices.len() && indices[i + run] == indices[i + run - 1] + 1
                        {
                            run += 1;
                        }
                        if run >= 4 {
                            $dst_data.extend_from_slice(&$src_data[first..first + run]);
                        } else {
                            for &idx in &indices[i..i + run] {
                                $dst_data.push($src_data[idx as usize]);
                            }
                        }
                        $dst_bitmap.extend_valid(run);
                        i += run;
                    }
                } else {
                    for &idx in indices {
                        let i = idx as usize;
                        $dst_data.push($src_data[i]);
                        if $src_bitmap.is_null(i) {
                            $dst_bitmap.push_null();
                        } else {
                            $dst_bitmap.push_valid();
                        }
                    }
                }
                Ok(())
            }};
        }

        // Variable-width payload: per-row copy is unavoidable (offsets differ),
        // but the destination is reserved up front so the loop is
        // allocation-free, and the bitmap still uses the bulk path.
        macro_rules! gather_var {
            ($src:expr, $src_bitmap:expr, $dst:expr, $dst_bitmap:expr, $push:ident) => {{
                let total_bytes: usize = indices
                    .iter()
                    .map(|&idx| {
                        let i = idx as usize;
                        ($src.offsets[i + 1] - $src.offsets[i]) as usize
                    })
                    .sum();
                $dst.reserve_values(indices.len(), total_bytes);
                if all_valid {
                    $dst_bitmap.extend_valid(indices.len());
                    for &idx in indices {
                        let i = idx as usize;
                        $dst.$push($src.value(i));
                    }
                } else {
                    for &idx in indices {
                        let i = idx as usize;
                        $dst.$push($src.value(i));
                        if $src_bitmap.is_null(i) {
                            $dst_bitmap.push_null();
                        } else {
                            $dst_bitmap.push_valid();
                        }
                    }
                }
                Ok(())
            }};
        }

        match (self, dst) {
            (
                ColumnArray::Bool { data, bitmap },
                ColumnArray::Bool {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Int8 { data, bitmap },
                ColumnArray::Int8 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Int16 { data, bitmap },
                ColumnArray::Int16 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Int32 { data, bitmap },
                ColumnArray::Int32 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Int64 { data, bitmap },
                ColumnArray::Int64 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::UInt8 { data, bitmap },
                ColumnArray::UInt8 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::UInt16 { data, bitmap },
                ColumnArray::UInt16 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::UInt32 { data, bitmap },
                ColumnArray::UInt32 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::UInt64 { data, bitmap },
                ColumnArray::UInt64 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Float32 { data, bitmap },
                ColumnArray::Float32 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Float64 { data, bitmap },
                ColumnArray::Float64 {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Decimal { data, bitmap },
                ColumnArray::Decimal {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Date { data, bitmap },
                ColumnArray::Date {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Time { data, bitmap },
                ColumnArray::Time {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Timestamp { data, bitmap },
                ColumnArray::Timestamp {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::Interval { data, bitmap },
                ColumnArray::Interval {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_fixed!(data, bitmap, dst_data, dst_bitmap),
            (
                ColumnArray::VarChar { data, bitmap },
                ColumnArray::VarChar {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_var!(data, bitmap, dst_data, dst_bitmap, push_str),
            (
                ColumnArray::Blob { data, bitmap },
                ColumnArray::Blob {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => gather_var!(data, bitmap, dst_data, dst_bitmap, push_slice),
            (
                ColumnArray::Vector { data, bitmap },
                ColumnArray::Vector {
                    data: dst_data,
                    bitmap: dst_bitmap,
                },
            ) => {
                dst_data.reserve(dst_data.len() + indices.len());
                for &idx in indices {
                    dst_data.push(data[idx as usize].clone());
                }
                if all_valid {
                    dst_bitmap.extend_valid(indices.len());
                } else {
                    for &idx in indices {
                        if bitmap.is_null(idx as usize) {
                            dst_bitmap.push_null();
                        } else {
                            dst_bitmap.push_valid();
                        }
                    }
                }
                Ok(())
            }
            _ => Err(TaibaiError::Internal(
                "gather_into: column type mismatch".to_string(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Vectorized column-vs-column comparison and arithmetic kernels.
//
// Semantics contract: these kernels must produce byte-identical results to the
// per-row scalar paths (`eval_comparison` / `eval_binary_scalar` in
// `src/sql/execution/operators/filter.rs`). Any node shape not covered here
// falls back to the scalar path, so coverage can grow incrementally without
// behavior risk.
// ---------------------------------------------------------------------------

/// Comparison operator for the typed column kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Arithmetic operator for the typed column kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[inline]
fn compare_op_matches(ord: Option<std::cmp::Ordering>, op: CompareOp) -> bool {
    use std::cmp::Ordering;
    match op {
        CompareOp::Eq => ord == Some(Ordering::Equal),
        // Incomparable values (NaN) still count as "not equal".
        CompareOp::Neq => ord != Some(Ordering::Equal),
        CompareOp::Lt => ord == Some(Ordering::Less),
        CompareOp::Lte => ord != Some(Ordering::Greater),
        CompareOp::Gt => ord == Some(Ordering::Greater),
        CompareOp::Gte => ord != Some(Ordering::Less),
    }
}

/// Three-valued result of a typed column-vs-column comparison: the two-valued
/// match mask plus the NULL bitmap marking rows where the result is NULL
/// (either operand NULL, or NaN for float columns — mirroring the scalar
/// path, where an incomparable NaN comparison yields Bool(None)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCompare {
    pub mask: Vec<bool>,
    pub nulls: NullBitmap,
}

fn cmp_mask_ord<T: Ord>(
    l: &[T],
    lb: &NullBitmap,
    r: &[T],
    rb: &NullBitmap,
    op: CompareOp,
) -> ColumnCompare {
    let mut nulls = NullBitmap::with_capacity(l.len());
    let mask = (0..l.len())
        .map(|i| {
            if lb.is_null(i) || rb.is_null(i) {
                nulls.push_null();
                return false;
            }
            nulls.push_valid();
            compare_op_matches(Some(l[i].cmp(&r[i])), op)
        })
        .collect();
    ColumnCompare { mask, nulls }
}

fn cmp_mask_f32(
    l: &[f32],
    lb: &NullBitmap,
    r: &[f32],
    rb: &NullBitmap,
    op: CompareOp,
) -> ColumnCompare {
    let mut nulls = NullBitmap::with_capacity(l.len());
    let mask = (0..l.len())
        .map(|i| {
            if lb.is_null(i) || rb.is_null(i) || l[i].is_nan() || r[i].is_nan() {
                nulls.push_null();
                return false;
            }
            nulls.push_valid();
            compare_op_matches(l[i].partial_cmp(&r[i]), op)
        })
        .collect();
    ColumnCompare { mask, nulls }
}

fn cmp_mask_f64(
    l: &[f64],
    lb: &NullBitmap,
    r: &[f64],
    rb: &NullBitmap,
    op: CompareOp,
) -> ColumnCompare {
    let mut nulls = NullBitmap::with_capacity(l.len());
    let mask = (0..l.len())
        .map(|i| {
            if lb.is_null(i) || rb.is_null(i) || l[i].is_nan() || r[i].is_nan() {
                nulls.push_null();
                return false;
            }
            nulls.push_valid();
            compare_op_matches(l[i].partial_cmp(&r[i]), op)
        })
        .collect();
    ColumnCompare { mask, nulls }
}

fn cmp_mask_str(
    l: &Utf8Buffer,
    lb: &NullBitmap,
    r: &Utf8Buffer,
    rb: &NullBitmap,
    op: CompareOp,
) -> ColumnCompare {
    let mut nulls = NullBitmap::with_capacity(l.len());
    let mask = (0..l.len())
        .map(|i| {
            if lb.is_null(i) || rb.is_null(i) {
                nulls.push_null();
                return false;
            }
            nulls.push_valid();
            compare_op_matches(Some(l.value(i).cmp(r.value(i))), op)
        })
        .collect();
    ColumnCompare { mask, nulls }
}

impl ColumnArray {
    /// Element-wise three-valued comparison of two same-variant columns.
    /// Returns `None` when the variants differ or the element type has no
    /// typed kernel (Blob, Vector, Bool); callers fall back to the scalar
    /// path.
    pub fn compare_column(&self, op: CompareOp, other: &ColumnArray) -> Option<ColumnCompare> {
        use ColumnArray as C;
        if self.len() != other.len() {
            return None;
        }
        let out = match (self, other) {
            (C::Int8 { data: l, bitmap: lb }, C::Int8 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Int16 { data: l, bitmap: lb }, C::Int16 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Int32 { data: l, bitmap: lb }, C::Int32 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Int64 { data: l, bitmap: lb }, C::Int64 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::UInt8 { data: l, bitmap: lb }, C::UInt8 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::UInt16 { data: l, bitmap: lb }, C::UInt16 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::UInt32 { data: l, bitmap: lb }, C::UInt32 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::UInt64 { data: l, bitmap: lb }, C::UInt64 { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Decimal { data: l, bitmap: lb }, C::Decimal { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Date { data: l, bitmap: lb }, C::Date { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Time { data: l, bitmap: lb }, C::Time { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Timestamp { data: l, bitmap: lb }, C::Timestamp { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Interval { data: l, bitmap: lb }, C::Interval { data: r, bitmap: rb }) => {
                cmp_mask_ord(l, lb, r, rb, op)
            }
            (C::Float32 { data: l, bitmap: lb }, C::Float32 { data: r, bitmap: rb }) => {
                cmp_mask_f32(l, lb, r, rb, op)
            }
            (C::Float64 { data: l, bitmap: lb }, C::Float64 { data: r, bitmap: rb }) => {
                cmp_mask_f64(l, lb, r, rb, op)
            }
            (C::VarChar { data: l, bitmap: lb }, C::VarChar { data: r, bitmap: rb }) => {
                cmp_mask_str(l, lb, r, rb, op)
            }
            _ => return None,
        };
        Some(out)
    }

    /// True for variants whose values participate in `integer_i128`
    /// arithmetic (all signed/unsigned integers plus unscaled Decimal).
    fn is_integer_class(&self) -> bool {
        matches!(
            self,
            ColumnArray::Int8 { .. }
                | ColumnArray::Int16 { .. }
                | ColumnArray::Int32 { .. }
                | ColumnArray::Int64 { .. }
                | ColumnArray::UInt8 { .. }
                | ColumnArray::UInt16 { .. }
                | ColumnArray::UInt32 { .. }
                | ColumnArray::UInt64 { .. }
                | ColumnArray::Decimal { .. }
        )
    }

    fn is_float_class(&self) -> bool {
        matches!(
            self,
            ColumnArray::Float32 { .. } | ColumnArray::Float64 { .. }
        )
    }

    /// Per-row `integer_i128` equivalent over the column's physical data.
    fn integer_i128_at(&self, i: usize) -> Option<i128> {
        if self.bitmap().is_null(i) {
            return None;
        }
        let value = match self {
            ColumnArray::Int8 { data, .. } => data[i] as i128,
            ColumnArray::Int16 { data, .. } => data[i] as i128,
            ColumnArray::Int32 { data, .. } => data[i] as i128,
            ColumnArray::Int64 { data, .. } => data[i] as i128,
            ColumnArray::UInt8 { data, .. } => data[i] as i128,
            ColumnArray::UInt16 { data, .. } => data[i] as i128,
            ColumnArray::UInt32 { data, .. } => data[i] as i128,
            ColumnArray::UInt64 { data, .. } => data[i] as i128,
            ColumnArray::Decimal { data, .. } => data[i] as i128,
            _ => return None,
        };
        Some(value)
    }

    /// Element-wise arithmetic on two columns. Mirrors `eval_binary_scalar`:
    /// integer-class pairs widen to i128 and produce Int64 (overflow and
    /// zero-divisor become NULL), division always produces Float64, and any
    /// NULL operand produces a NULL row. Returns `None` for unsupported
    /// variant combinations.
    pub fn arith_column(&self, op: ArithOp, other: &ColumnArray) -> Option<ColumnArray> {
        if self.len() != other.len() {
            return None;
        }
        if op == ArithOp::Div {
            // Division mirrors the scalar path, which cannot evaluate
            // date/time operands: keep those off the vectorized kernel.
            if !(self.is_integer_class() || self.is_float_class()) {
                return None;
            }
            return self.arith_numeric_f64(op, |i| other.numeric_f64_at(i));
        }
        if self.is_integer_class() && other.is_integer_class() {
            return Some(self.arith_integer(op, |i| other.integer_i128_at(i)));
        }
        if self.is_float_class() && other.is_float_class() {
            return self.arith_numeric_f64(op, |i| other.numeric_f64_at(i));
        }
        // Mixed float x integer (and anything else) stays on the scalar path.
        None
    }

    /// Element-wise arithmetic between a column and a scalar literal.
    /// A NULL literal yields an all-NULL Int64 column, exactly like the
    /// scalar path's early return.
    pub fn arith_scalar(&self, op: ArithOp, literal: &ScalarValue) -> Option<ColumnArray> {
        if literal.is_null() {
            return Some(ColumnArray::Int64 {
                data: vec![0; self.len()],
                bitmap: NullBitmap::from_bools(&vec![true; self.len()]),
            });
        }
        if op == ArithOp::Div {
            // The scalar path cannot evaluate date/time operands; keep this
            // kernel byte-identical with it by refusing those columns.
            if !(self.is_integer_class() || self.is_float_class()) {
                return None;
            }
            let value = literal.numeric_f64()?;
            return self.arith_numeric_f64(op, |_| Some(value));
        }
        if self.is_integer_class() && literal.integer_i128().is_some() {
            let value = literal.integer_i128()?;
            return Some(self.arith_integer(op, |_| Some(value)));
        }
        if (self.is_integer_class() || self.is_float_class())
            && literal.numeric_f64().is_some()
        {
            let value = literal.numeric_f64()?;
            return self.arith_numeric_f64(op, |_| Some(value));
        }
        None
    }

    /// Integer-class arithmetic: i128 intermediates, Int64 output, checked
    /// overflow/zero-divisor → NULL. `rhs_at` covers both column and scalar
    /// right-hand sides.
    fn arith_integer(&self, op: ArithOp, rhs_at: impl Fn(usize) -> Option<i128>) -> ColumnArray {
        let len = self.len();
        let mut data = vec![0i64; len];
        let mut bitmap = NullBitmap::with_capacity(len);
        for (i, slot) in data.iter_mut().enumerate() {
            let value = match (self.integer_i128_at(i), rhs_at(i)) {
                (Some(l), Some(r)) => {
                    let raw = match op {
                        ArithOp::Add => l.checked_add(r),
                        ArithOp::Sub => l.checked_sub(r),
                        ArithOp::Mul => l.checked_mul(r),
                        ArithOp::Mod => {
                            if r == 0 {
                                None
                            } else {
                                Some(l % r)
                            }
                        }
                        ArithOp::Div => None,
                    };
                    raw.and_then(|v| i64::try_from(v).ok())
                }
                _ => None,
            };
            match value {
                Some(v) => {
                    *slot = v;
                    bitmap.push_valid();
                }
                None => bitmap.push_null(),
            }
        }
        ColumnArray::Int64 { data, bitmap }
    }

    /// f64 arithmetic (division and float-class ops): Float64 output, NULL on
    /// missing operands; zero divisor → NULL to match `eval_binary_scalar`.
    fn arith_numeric_f64(
        &self,
        op: ArithOp,
        rhs_at: impl Fn(usize) -> Option<f64>,
    ) -> Option<ColumnArray> {
        let len = self.len();
        let mut data = vec![0f64; len];
        let mut bitmap = NullBitmap::with_capacity(len);
        for (i, slot) in data.iter_mut().enumerate() {
            let value = match (self.numeric_f64_at(i), rhs_at(i)) {
                (Some(l), Some(r)) => match op {
                    ArithOp::Div => {
                        if r == 0.0 {
                            None
                        } else {
                            Some(l / r)
                        }
                    }
                    ArithOp::Add => Some(l + r),
                    ArithOp::Sub => Some(l - r),
                    ArithOp::Mul => Some(l * r),
                    ArithOp::Mod => {
                        if r == 0.0 {
                            None
                        } else {
                            Some(l % r)
                        }
                    }
                },
                _ => None,
            };
            match value {
                Some(v) => {
                    *slot = v;
                    bitmap.push_valid();
                }
                None => bitmap.push_null(),
            }
        }
        Some(ColumnArray::Float64 { data, bitmap })
    }

    /// Element-wise logical NOT on a Bool column; NULL rows stay NULL (their
    /// payload bytes are left at 0).
    pub fn bool_not(&self) -> Option<ColumnArray> {
        if let ColumnArray::Bool { data, bitmap } = self {
            Some(ColumnArray::Bool {
                data: data
                    .iter()
                    .enumerate()
                    .map(|(i, v)| if bitmap.is_null(i) { 0 } else { v ^ 1 })
                    .collect(),
                bitmap: bitmap.clone(),
            })
        } else {
            None
        }
    }

    /// Build a Bool column from a row mask (all rows valid).
    pub fn bool_column_from_mask(mask: Vec<bool>) -> ColumnArray {
        let bitmap = NullBitmap::new_valid(mask.len());
        ColumnArray::Bool {
            data: mask.into_iter().map(u8::from).collect(),
            bitmap,
        }
    }
}


/// Word-at-a-time valid-row sum over `[offset, end)` for fixed-width data.
fn sum_valid_range_words<T: Copy>(
    data: &[T],
    bitmap: &crate::types::bitmap::NullBitmap,
    offset: usize,
    end: usize,
    cvt: impl Fn(T) -> i64,
) -> Option<i64> {
    let mut sum: i64 = 0;
    let mut any = false;
    for (word_idx, &word) in bitmap.words().iter().enumerate() {
        let start = (word_idx * 64).max(offset);
        let word_end = (word_idx * 64 + 64).min(end);
        if start >= word_end {
            continue;
        }
        if word == 0 {
            for &v in &data[start..word_end] {
                sum = sum.wrapping_add(cvt(v));
            }
            any = true;
        } else {
            for (bit, &v) in data[start..word_end].iter().enumerate() {
                if word & (1u64 << (start + bit) % 64) == 0 {
                    sum = sum.wrapping_add(cvt(v));
                    any = true;
                }
            }
        }
    }
    any.then_some(sum)
}

/// Word-at-a-time valid-row sum for float data.
fn sum_valid_words_f64<T: Copy>(
    data: &[T],
    bitmap: &crate::types::bitmap::NullBitmap,
    cvt: impl Fn(&T) -> f64,
) -> Option<f64> {
    let mut sum = 0.0f64;
    let mut any = false;
    for (word_idx, &word) in bitmap.words().iter().enumerate() {
        let start = word_idx * 64;
        if start >= data.len() {
            break;
        }
        let end = (start + 64).min(data.len());
        if word == 0 {
            for v in &data[start..end] {
                sum += cvt(v);
            }
            any = true;
        } else if word != u64::MAX {
            for (bit, v) in data[start..end].iter().enumerate() {
                if word & (1u64 << bit) == 0 {
                    sum += cvt(v);
                    any = true;
                }
            }
        }
    }
    any.then_some(sum)
}
