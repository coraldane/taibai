use crate::storage::segment::{ZoneMap, RangeOp, RangeClause, RowGroupPredicate, ScanStats};
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

pub trait PredicateMatcher: Send + Sync {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool;
    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool;
    fn matches_column_value(&self, val: &ScalarValue) -> bool;
    fn column_index(&self) -> usize;
    fn is_fast_scan_compatible(&self) -> bool {
        false
    }
    fn as_any(&self) -> &dyn std::any::Any;
}

pub struct EqMatcher {
    column_index: usize,
    value: ScalarValue,
}

impl EqMatcher {
    pub fn new(column_index: usize, value: ScalarValue) -> Self {
        Self { column_index, value }
    }

    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

impl PredicateMatcher for EqMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        let Some(zm) = zone_maps.get(self.column_index) else { return true };
        if zm.row_count > 0 && zm.null_count == zm.row_count {
            return false;
        }
        if zm.min_value.is_null() || zm.max_value.is_null() {
            return true;
        }
        !scalar_lt(&self.value, &zm.min_value) && !scalar_lt(&zm.max_value, &self.value)
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        let val = batch.scalar(row_idx, self.column_index).unwrap_or(ScalarValue::Int64(None));
        scalar_eq(&val, &self.value)
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        scalar_eq(val, &self.value)
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    fn is_fast_scan_compatible(&self) -> bool {
        !self.value.is_null()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct RangeMatcher {
    column_index: usize,
    op: RangeOp,
    value: ScalarValue,
}

impl RangeMatcher {
    pub fn new(column_index: usize, op: RangeOp, value: ScalarValue) -> Self {
        Self { column_index, op, value }
    }
}

impl PredicateMatcher for RangeMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        let Some(zm) = zone_maps.get(self.column_index) else { return true };
        if zm.row_count > 0 && zm.null_count == zm.row_count {
            return false;
        }
        if zm.min_value.is_null() || zm.max_value.is_null() {
            return true;
        }
        clause_overlaps_range_impl(self.op, &self.value, &zm.min_value, &zm.max_value)
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        let val = batch.scalar(row_idx, self.column_index).unwrap_or(ScalarValue::Int64(None));
        match self.op {
            RangeOp::Eq => scalar_eq(&val, &self.value),
            RangeOp::Lt => scalar_lt(&val, &self.value),
            RangeOp::Le => scalar_le(&val, &self.value),
            RangeOp::Gt => scalar_lt(&self.value, &val),
            RangeOp::Ge => scalar_le(&self.value, &val),
        }
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        match self.op {
            RangeOp::Eq => scalar_eq(val, &self.value),
            RangeOp::Lt => scalar_lt(val, &self.value),
            RangeOp::Le => scalar_le(val, &self.value),
            RangeOp::Gt => scalar_lt(&self.value, val),
            RangeOp::Ge => scalar_le(&self.value, val),
        }
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    fn is_fast_scan_compatible(&self) -> bool {
        !self.value.is_null()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct InMatcher {
    column_index: usize,
    values: Vec<ScalarValue>,
    sorted_values: Vec<ScalarValue>,
}

impl InMatcher {
    pub fn new(column_index: usize, values: Vec<ScalarValue>) -> Self {
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| compare_scalars_for_sort(a, b));
        Self { column_index, values, sorted_values: sorted }
    }

    fn binary_search_contains(&self, val: &ScalarValue) -> bool {
        if val.is_null() {
            return false;
        }
        let sorted = &self.sorted_values;
        let mut lo = 0usize;
        let mut hi = sorted.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match compare_scalars_for_sort(val, &sorted[mid]) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
        false
    }
}

impl PredicateMatcher for InMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        let Some(zm) = zone_maps.get(self.column_index) else { return true };
        if zm.row_count > 0 && zm.null_count == zm.row_count {
            return false;
        }
        if zm.min_value.is_null() || zm.max_value.is_null() {
            return true;
        }
        for v in &self.sorted_values {
            if !scalar_lt(v, &zm.min_value) && !scalar_lt(&zm.max_value, v) {
                return true;
            }
        }
        false
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        let val = batch.scalar(row_idx, self.column_index).unwrap_or(ScalarValue::Int64(None));
        self.binary_search_contains(&val)
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        self.binary_search_contains(val)
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    fn is_fast_scan_compatible(&self) -> bool {
        !self.sorted_values.is_empty() && !self.sorted_values.iter().any(|v| v.is_null())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct AndMatcher {
    matchers: Vec<Box<dyn PredicateMatcher>>,
}

impl AndMatcher {
    pub fn new(matchers: Vec<Box<dyn PredicateMatcher>>) -> Self {
        Self { matchers }
    }

    pub fn from_clauses(clauses: &[RangeClause]) -> Self {
        let matchers: Vec<Box<dyn PredicateMatcher>> = clauses.iter()
            .map(|c| Box::new(RangeMatcher::new(c.column_index, c.op, c.value.clone())) as Box<dyn PredicateMatcher>)
            .collect();
        Self { matchers }
    }
}

impl PredicateMatcher for AndMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        self.matchers.iter().all(|m| m.matches_row_group(zone_maps))
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        self.matchers.iter().all(|m| m.matches_row(row_idx, batch))
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        self.matchers.iter().all(|m| m.matches_column_value(val))
    }

    fn column_index(&self) -> usize {
        self.matchers.first().map(|m| m.column_index()).unwrap_or(0)
    }

    fn is_fast_scan_compatible(&self) -> bool {
        self.matchers.iter().all(|m| m.is_fast_scan_compatible())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct OrMatcher {
    matchers: Vec<Box<dyn PredicateMatcher>>,
}

impl OrMatcher {
    pub fn new(matchers: Vec<Box<dyn PredicateMatcher>>) -> Self {
        Self { matchers }
    }
}

impl PredicateMatcher for OrMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        self.matchers.iter().any(|m| m.matches_row_group(zone_maps))
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        self.matchers.iter().any(|m| m.matches_row(row_idx, batch))
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        self.matchers.iter().any(|m| m.matches_column_value(val))
    }

    fn column_index(&self) -> usize {
        self.matchers.first().map(|m| m.column_index()).unwrap_or(0)
    }

    fn is_fast_scan_compatible(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct NotMatcher {
    inner: Box<dyn PredicateMatcher>,
}

impl NotMatcher {
    pub fn new(inner: Box<dyn PredicateMatcher>) -> Self {
        Self { inner }
    }
}

impl PredicateMatcher for NotMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        !self.inner.matches_row_group(zone_maps)
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        !self.inner.matches_row(row_idx, batch)
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        !self.inner.matches_column_value(val)
    }

    fn column_index(&self) -> usize {
        self.inner.column_index()
    }

    fn is_fast_scan_compatible(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct IsNullMatcher {
    column_index: usize,
}

impl IsNullMatcher {
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl PredicateMatcher for IsNullMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        let Some(zm) = zone_maps.get(self.column_index) else { return true };
        zm.null_count > 0
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        let val = batch.scalar(row_idx, self.column_index).unwrap_or(ScalarValue::Int64(None));
        val.is_null()
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        val.is_null()
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    fn is_fast_scan_compatible(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct IsNotNullMatcher {
    column_index: usize,
}

impl IsNotNullMatcher {
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl PredicateMatcher for IsNotNullMatcher {
    fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        let Some(zm) = zone_maps.get(self.column_index) else { return true };
        zm.null_count < zm.row_count
    }

    fn matches_row(&self, row_idx: usize, batch: &RecordBatch) -> bool {
        let val = batch.scalar(row_idx, self.column_index).unwrap_or(ScalarValue::Int64(None));
        !val.is_null()
    }

    fn matches_column_value(&self, val: &ScalarValue) -> bool {
        !val.is_null()
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    fn is_fast_scan_compatible(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct FastScanEQ {
    column_index: usize,
    value: ScalarValue,
    matcher: EqMatcher,
}

impl FastScanEQ {
    pub fn new(column_index: usize, value: ScalarValue) -> Self {
        Self {
            column_index,
            value: value.clone(),
            matcher: EqMatcher::new(column_index, value),
        }
    }

    pub fn can_use_binary_search(&self, batch: &RecordBatch, key_indices: &[usize]) -> bool {
        if self.value.is_null() {
            return false;
        }
        key_indices.contains(&self.column_index)
    }

    pub fn binary_search_range(&self, col: &ColumnArray) -> Option<(usize, usize)> {
        if self.value.is_null() {
            return None;
        }
        let num_rows = col.len();
        if num_rows == 0 {
            return Some((0, 0));
        }
        let first_match = self.find_first_match(col)?;
        let last_match = self.find_last_match(col, first_match);
        Some((first_match, last_match + 1))
    }

    fn find_first_match(&self, col: &ColumnArray) -> Option<usize> {
        let num_rows = col.len();
        let mut lo = 0usize;
        let mut hi = num_rows;
        let mut found: Option<usize> = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let val = col.scalar(mid);
            match compare_scalars_for_sort(&val, &self.value) {
                std::cmp::Ordering::Equal => {
                    found = Some(mid);
                    hi = mid;
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        found
    }

    fn find_last_match(&self, col: &ColumnArray, start: usize) -> usize {
        let num_rows = col.len();
        let mut lo = start;
        let mut hi = num_rows;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let val = col.scalar(mid);
            match compare_scalars_for_sort(&val, &self.value) {
                std::cmp::Ordering::Equal => lo = mid + 1,
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        lo.saturating_sub(1)
    }

    pub fn scan_batch(&self, batch: &RecordBatch) -> RecordBatch {
        filter_batch_by_predicate(batch, &self.matcher)
    }

    pub fn scan_batch_sorted(&self, batch: &RecordBatch, key_indices: &[usize]) -> Option<RecordBatch> {
        if !self.can_use_binary_search(batch, key_indices) {
            return Some(self.scan_batch(batch));
        }
        let col = batch.column(self.column_index)?;
        let range = self.binary_search_range(col)?;
        let (start, end) = range;
        if start >= end {
            return Some(RecordBatch::new(batch.schema.clone()));
        }
        Some(split_batch(batch, start, end))
    }

    pub fn matcher(&self) -> &dyn PredicateMatcher {
        &self.matcher
    }

    pub fn column_index(&self) -> usize {
        self.column_index
    }

    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

pub struct MultiPredicateFastScan {
    matchers: Vec<Box<dyn PredicateMatcher>>,
    primary_column: usize,
}

impl MultiPredicateFastScan {
    pub fn new(matchers: Vec<Box<dyn PredicateMatcher>>) -> Self {
        let primary_column = matchers.first().map(|m| m.column_index()).unwrap_or(0);
        Self { matchers, primary_column }
    }

    pub fn from_row_group_predicate(pred: &RowGroupPredicate) -> Self {
        let matchers = AndMatcher::from_clauses(&pred.clauses);
        Self::new(vec![Box::new(matchers)])
    }

    pub fn add_matcher(&mut self, matcher: Box<dyn PredicateMatcher>) {
        self.matchers.push(matcher);
    }

    pub fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        self.matchers.iter().all(|m| m.matches_row_group(zone_maps))
    }

    pub fn scan_batch(&self, batch: &RecordBatch) -> RecordBatch {
        if batch.num_rows() == 0 {
            return batch.clone();
        }
        let mask: Vec<bool> = (0..batch.num_rows())
            .map(|i| self.matchers.iter().all(|m| m.matches_row(i, batch)))
            .collect();
        apply_filter_mask(batch, &mask)
    }

    pub fn scan_batch_with_stats(&self, batch: &RecordBatch, stats: &mut ScanStats) -> RecordBatch {
        let result = self.scan_batch(batch);
        stats.rows_scanned += batch.num_rows() as u64;
        stats.rows_filtered_by_delete_bitmap += (batch.num_rows() - result.num_rows()) as u64;
        result
    }

    pub fn try_fast_scan_sorted(&self, batch: &RecordBatch, key_indices: &[usize]) -> Option<RecordBatch> {
        if self.matchers.len() != 1 {
            return None;
        }
        let matcher = &self.matchers[0];
        if !matcher.is_fast_scan_compatible() {
            return None;
        }
        let matcher_col = matcher.column_index();
        if !key_indices.contains(&matcher_col) {
            return None;
        }
        if let Some(eq_matcher) = self.try_get_eq_matcher() {
            let fast_scan = FastScanEQ::new(eq_matcher.column_index(), eq_matcher.value().clone());
            return fast_scan.scan_batch_sorted(batch, key_indices);
        }
        None
    }

    fn try_get_eq_matcher(&self) -> Option<&EqMatcher> {
        if self.matchers.len() != 1 {
            return None;
        }
        self.matchers[0].as_any().downcast_ref::<EqMatcher>()
    }

    pub fn matchers(&self) -> &[Box<dyn PredicateMatcher>] {
        &self.matchers
    }

    pub fn primary_column(&self) -> usize {
        self.primary_column
    }
}

fn filter_batch_by_predicate(batch: &RecordBatch, matcher: &dyn PredicateMatcher) -> RecordBatch {
    if batch.num_rows() == 0 {
        return batch.clone();
    }
    let mask: Vec<bool> = (0..batch.num_rows())
        .map(|i| matcher.matches_row(i, batch))
        .collect();
    apply_filter_mask(batch, &mask)
}

fn apply_filter_mask(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&col.scalar(i)).unwrap();
            }
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

fn split_batch(batch: &RecordBatch, start: usize, end: usize) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for i in start..end {
            new_col.push(&col.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    compare_scalars_for_sort(a, b) == std::cmp::Ordering::Equal
}

fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    compare_scalars_for_sort(a, b) == std::cmp::Ordering::Less
}

fn scalar_le(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    matches!(compare_scalars_for_sort(a, b), std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
}

fn clause_overlaps_range_impl(op: RangeOp, val: &ScalarValue, min_v: &ScalarValue, max_v: &ScalarValue) -> bool {
    match op {
        RangeOp::Eq => !scalar_lt(val, min_v) && !scalar_lt(max_v, val),
        RangeOp::Lt => scalar_lt(min_v, val),
        RangeOp::Le => !scalar_lt(val, min_v),
        RangeOp::Gt => scalar_lt(val, max_v),
        RangeOp::Ge => !scalar_lt(max_v, val),
    }
}

fn compare_scalars_for_sort(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Less;
    }
    if b.is_null() {
        return std::cmp::Ordering::Greater;
    }
    match (a, b) {
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

impl RowGroupPredicate {
    pub fn to_multi_predicate_fast_scan(&self) -> MultiPredicateFastScan {
        MultiPredicateFastScan::from_row_group_predicate(self)
    }
}