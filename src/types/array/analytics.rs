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
                let mut best: Option<&String> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if b <= v => b,
                            _ => v,
                        });
                    }
                }
                best.map(|v| ScalarValue::VarChar(Some(v.clone())))
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
                let mut best: Option<&String> = None;
                for (i, v) in data.iter().enumerate().take(n) {
                    if !bitmap.is_null(i) {
                        best = Some(match best {
                            Some(b) if b >= v => b,
                            _ => v,
                        });
                    }
                }
                best.map(|v| ScalarValue::VarChar(Some(v.clone())))
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

    pub fn eq_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && (data[i] != 0) == *v)
                .collect(),
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] == *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] == *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] == v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Equal))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn lt_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] < *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] < *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] < v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Less))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn le_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] <= *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] <= *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] <= v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && self
                            .scalar(i)
                            .numeric_cmp(value)
                            .is_some_and(|ord| ord != std::cmp::Ordering::Greater)
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn gt_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] > *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] > *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] > v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Greater))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn ge_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] >= *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] >= *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] >= v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && self
                            .scalar(i)
                            .numeric_cmp(value)
                            .is_some_and(|ord| ord != std::cmp::Ordering::Less)
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

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
                let mut new_data = Vec::with_capacity(match_count);
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for (i, &keep) in mask.iter().enumerate() {
                    if keep {
                        new_data.push(data[i].clone());
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
            ColumnArray::VarChar { data, bitmap } => slice_copy_column!(VarChar, data, bitmap),
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
                ColumnArray::VarChar {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
            ColumnArray::Blob { data, bitmap } => {
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
