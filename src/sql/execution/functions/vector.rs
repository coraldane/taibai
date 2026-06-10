/// Vector SQL functions for vector similarity search.
use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

fn get_vector(args: &[ScalarValue], idx: usize) -> Option<Vec<f32>> {
    match args.get(idx) {
        Some(ScalarValue::Vector(Some(v))) => Some(v.clone()),
        Some(ScalarValue::VarChar(Some(s))) => {
            let floats: Result<Vec<f32>> = s
                .split(',')
                .map(|p| {
                    p.trim().parse::<f32>().map_err(|e| {
                        crate::common::error::TaibaiError::Sql(format!(
                            "invalid vector element: {}",
                            e
                        ))
                    })
                })
                .collect();
            floats.ok()
        }
        _ => None,
    }
}

/// VECTOR_DISTANCE(a, b, metric) where metric is "l2", "cosine", "dot", or "manhattan"
pub fn vector_distance(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = get_vector(args, 0);
    let b = get_vector(args, 1);
    let metric = match args.get(2) {
        Some(ScalarValue::VarChar(Some(s))) => s.to_lowercase(),
        _ => "l2".to_string(),
    };

    match (a, b) {
        (Some(a), Some(b)) if a.len() == b.len() => {
            let dist = match metric.as_str() {
                "l2" | "euclidean" => a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt(),
                "cosine" => {
                    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                    if norm_a == 0.0 || norm_b == 0.0 {
                        1.0
                    } else {
                        1.0 - dot / (norm_a * norm_b)
                    }
                }
                "dot" => {
                    -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>() // negated for sorting
                }
                "manhattan" => a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum(),
                _ => {
                    return Err(crate::common::error::TaibaiError::Sql(format!(
                        "unknown distance metric '{}'",
                        metric
                    )))
                }
            };
            Ok(ScalarValue::Float64(Some(dist as f64)))
        }
        _ => Ok(ScalarValue::Float64(None)),
    }
}

/// VECTOR_NORM(a) - L2 norm of a vector
pub fn vector_norm(args: &[ScalarValue]) -> Result<ScalarValue> {
    match get_vector(args, 0) {
        Some(v) => {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            Ok(ScalarValue::Float64(Some(norm as f64)))
        }
        None => Ok(ScalarValue::Float64(None)),
    }
}

/// VECTOR_NORMALIZE(a) - unit vector
pub fn vector_normalize(args: &[ScalarValue]) -> Result<ScalarValue> {
    match get_vector(args, 0) {
        Some(v) => {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm == 0.0 {
                Ok(ScalarValue::Vector(Some(v)))
            } else {
                Ok(ScalarValue::Vector(Some(
                    v.iter().map(|x| x / norm).collect(),
                )))
            }
        }
        None => Ok(ScalarValue::Vector(None)),
    }
}

/// VECTOR_ADD(a, b)
pub fn vector_add(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = get_vector(args, 0);
    let b = get_vector(args, 1);
    match (a, b) {
        (Some(a), Some(b)) if a.len() == b.len() => Ok(ScalarValue::Vector(Some(
            a.iter().zip(b.iter()).map(|(x, y)| x + y).collect(),
        ))),
        _ => Ok(ScalarValue::Vector(None)),
    }
}

/// VECTOR_SUB(a, b)
pub fn vector_sub(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = get_vector(args, 0);
    let b = get_vector(args, 1);
    match (a, b) {
        (Some(a), Some(b)) if a.len() == b.len() => Ok(ScalarValue::Vector(Some(
            a.iter().zip(b.iter()).map(|(x, y)| x - y).collect(),
        ))),
        _ => Ok(ScalarValue::Vector(None)),
    }
}

/// VECTOR_SCALE(a, scalar)
pub fn vector_scale(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = get_vector(args, 0);
    let scalar = match args.get(1) {
        Some(ScalarValue::Float64(Some(v))) => *v as f32,
        Some(ScalarValue::Float32(Some(v))) => *v,
        Some(ScalarValue::Int64(Some(v))) => *v as f32,
        _ => return Ok(ScalarValue::Vector(None)),
    };
    match a {
        Some(a) => Ok(ScalarValue::Vector(Some(
            a.iter().map(|x| x * scalar).collect(),
        ))),
        None => Ok(ScalarValue::Vector(None)),
    }
}

/// VECTOR_DOT(a, b)
pub fn vector_dot(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = get_vector(args, 0);
    let b = get_vector(args, 1);
    match (a, b) {
        (Some(a), Some(b)) if a.len() == b.len() => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            Ok(ScalarValue::Float64(Some(dot as f64)))
        }
        _ => Ok(ScalarValue::Float64(None)),
    }
}
