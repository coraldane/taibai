/// Generates a same-variant `Column op Literal` comparison kernel
/// (`eq_vec`/`lt_vec`/…). `$ord_op`/`$ord_val` describe the cross-variant
/// fallback: it routes through `compare_scalar_at` and keeps the same
/// ordering predicate (e.g. `<=` means `ord != Ordering::Greater`).
macro_rules! cmp_vec_fn {
    ($fn_name:ident, $op:tt, $ord_op:tt, $ord_val:ident) => {
        pub fn $fn_name(&self, value: &ScalarValue) -> Vec<bool> {
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

impl ColumnArray {
    pub fn count_eq(&self, value: &ScalarValue) -> u64 {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int16 { data, .. }, ScalarValue::Int16(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int8 { data, .. }, ScalarValue::Int8(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt64 { data, .. }, ScalarValue::UInt64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt32 { data, .. }, ScalarValue::UInt32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt16 { data, .. }, ScalarValue::UInt16(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt8 { data, .. }, ScalarValue::UInt8(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Float64 { data, .. }, ScalarValue::Float64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Float32 { data, .. }, ScalarValue::Float32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Decimal { data, .. }, ScalarValue::Decimal(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Date { data, .. }, ScalarValue::Date(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Timestamp { data, .. }, ScalarValue::Timestamp(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            _ => 0,
        }
    }

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
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
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
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
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
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
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
        let n = self.len();
        let bitmap = self.bitmap();
        let mut count: u64 = 0;
        for i in 0..n {
            if !bitmap.is_null(i) {
                count += 1;
            }
        }
        count
    }

    /// Column-wide sum as i64 for integer columns (vectorized). Returns None if
    /// all values are null or the column type is not integer.
    pub fn col_sum_i64(&self) -> Option<i64> {
        let n = self.len();
        let bitmap = self.bitmap();
        macro_rules! int_sum {
            ($data:expr, $cast:ty) => {{
                let mut sum: i64 = 0;
                let mut any = false;
                for (i, v) in $data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        sum = sum.wrapping_add(*v as $cast as i64);
                        any = true;
                    }
                }
                if any {
                    Some(sum)
                } else {
                    None
                }
            }};
        }
        match self {
            ColumnArray::Int64 { data, .. } => int_sum!(data, i64),
            ColumnArray::Int32 { data, .. } => int_sum!(data, i64),
            ColumnArray::Int16 { data, .. } => int_sum!(data, i64),
            ColumnArray::Int8 { data, .. } => int_sum!(data, i64),
            ColumnArray::UInt64 { data, .. } => {
                let mut sum: i64 = 0;
                let mut any = false;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        sum = sum.wrapping_add(i64::try_from(*v).unwrap_or(i64::MAX));
                        any = true;
                    }
                }
                if any {
                    Some(sum)
                } else {
                    None
                }
            }
            ColumnArray::UInt32 { data, .. } => int_sum!(data, i64),
            ColumnArray::UInt16 { data, .. } => int_sum!(data, i64),
            ColumnArray::UInt8 { data, .. } => int_sum!(data, i64),
            ColumnArray::Decimal { data, .. } => int_sum!(data, i64),
            _ => None,
        }
    }

    /// Column-wide sum as f64 for float columns (vectorized). Returns None if
    /// all values are null or the column type is not float.
    pub fn col_sum_f64(&self) -> Option<f64> {
        let n = self.len();
        let bitmap = self.bitmap();
        match self {
            ColumnArray::Float64 { data, .. } => {
                let mut sum = 0.0f64;
                let mut any = false;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        sum += *v;
                        any = true;
                    }
                }
                if any {
                    Some(sum)
                } else {
                    None
                }
            }
            ColumnArray::Float32 { data, .. } => {
                let mut sum = 0.0f64;
                let mut any = false;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        sum += *v as f64;
                        any = true;
                    }
                }
                if any {
                    Some(sum)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    cmp_vec_fn!(eq_vec, ==, ==, Equal);
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
        macro_rules! take_copy_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &idx in indices {
                    let i = idx as usize;
                    new_data.push($data[i]);
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
            ColumnArray::Bool { data, bitmap } => take_copy_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => take_copy_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => take_copy_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => take_copy_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => take_copy_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => take_copy_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => take_copy_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => take_copy_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => take_copy_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => take_copy_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => take_copy_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => take_copy_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => take_copy_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => take_copy_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => take_copy_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => take_copy_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => {
                let mut new_data = Utf8Buffer::with_capacity(indices.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &idx in indices {
                    let i = idx as usize;
                    new_data.push_str(data.value(i));
                    if bitmap.is_null(i) {
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
            ColumnArray::Blob { data, bitmap } => {
                let mut new_data = BinaryBuffer::with_capacity(indices.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &idx in indices {
                    let i = idx as usize;
                    new_data.push_slice(data.value(i));
                    if bitmap.is_null(i) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::Blob {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
            ColumnArray::Vector { data, bitmap } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for &idx in indices {
                    let i = idx as usize;
                    new_data.push(data[i].clone());
                    if bitmap.is_null(i) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::Vector {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
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
            let value = literal.numeric_f64()?;
            return self.arith_numeric_f64(op, |_| Some(value));
        }
        if self.is_integer_class() && literal.integer_i128().is_some() {
            let value = literal.integer_i128()?;
            return Some(self.arith_integer(op, |_| Some(value)));
        }
        if literal.numeric_f64().is_some() {
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
