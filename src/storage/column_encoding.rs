use crate::types::array::ColumnArray;

pub fn encode_column_direct_range(col: &ColumnArray, start: usize, len: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + len * col.data_type().fixed_size().unwrap_or(8));
    data.extend_from_slice(&0u32.to_le_bytes());
    let end = start + len;

    match col {
        ColumnArray::Int32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Int64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Float64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Decimal { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::VarChar { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        ColumnArray::Timestamp { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Bool { data: vals, .. } => {
            data.extend_from_slice(&vals[start..end]);
        }
        ColumnArray::Int8 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Int16 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 2) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::UInt8 { data: vals, .. } => {
            data.extend_from_slice(&vals[start..end]);
        }
        ColumnArray::UInt16 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 2) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::UInt32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::UInt64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Float32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Date { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Time { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Interval { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        ColumnArray::Blob { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
        }
        ColumnArray::Vector { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    data.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }

    data
}