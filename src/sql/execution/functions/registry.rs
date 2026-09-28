/// Function Registry - O(1) lookup for SQL functions.
/// Inspired by taibai's FunctionRegistry + DuckDB's function catalog.
use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::types::scalar::ScalarValue;

/// Function handler signature.
pub type FunctionHandler = fn(&[ScalarValue]) -> Result<ScalarValue>;

/// The global function registry.
pub struct FunctionRegistry {
    functions: HashMap<String, FunctionHandler>,
    aliases: HashMap<String, String>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
            aliases: HashMap::new(),
        };
        registry.register_defaults();
        registry
    }

    /// Register all default functions.
    fn register_defaults(&mut self) {
        // Math functions
        self.register_scalar("abs", super::math::abs);
        self.register_scalar("ceil", super::math::ceil);
        self.register_alias("ceiling", "ceil");
        self.register_scalar("floor", super::math::floor);
        self.register_scalar("round", super::math::round);
        self.register_scalar("sqrt", super::math::sqrt);
        self.register_scalar("pow", super::math::pow);
        self.register_alias("power", "pow");
        self.register_scalar("log", super::math::log);
        self.register_scalar("ln", super::math::ln);
        self.register_scalar("log10", super::math::log10);
        self.register_scalar("log2", super::math::log2);
        self.register_scalar("exp", super::math::exp);
        self.register_scalar("sin", super::math::sin);
        self.register_scalar("cos", super::math::cos);
        self.register_scalar("tan", super::math::tan);
        self.register_scalar("asin", super::math::asin);
        self.register_scalar("acos", super::math::acos);
        self.register_scalar("atan", super::math::atan);
        self.register_scalar("atan2", super::math::atan2);
        self.register_scalar("sign", super::math::sign);
        self.register_scalar("mod", super::math::modulo);
        self.register_scalar("truncate", super::math::truncate_func);
        self.register_scalar("pi", super::math::pi);

        // String functions
        self.register_scalar("length", super::string::length);
        self.register_alias("len", "length");
        self.register_scalar("upper", super::string::upper);
        self.register_alias("ucase", "upper");
        self.register_scalar("lower", super::string::lower);
        self.register_alias("lcase", "lower");
        self.register_scalar("trim", super::string::trim);
        self.register_scalar("ltrim", super::string::ltrim);
        self.register_scalar("rtrim", super::string::rtrim);
        self.register_scalar("substring", super::string::substring);
        self.register_alias("substr", "substring");
        self.register_scalar("concat", super::string::concat);
        self.register_scalar("concat_ws", super::string::concat_ws);
        self.register_scalar("replace", super::string::replace);
        self.register_scalar("reverse", super::string::reverse);
        self.register_scalar("left", super::string::left);
        self.register_scalar("right", super::string::right);
        self.register_scalar("lpad", super::string::lpad);
        self.register_scalar("rpad", super::string::rpad);
        self.register_scalar("instr", super::string::instr);
        self.register_alias("locate", "instr");
        self.register_scalar("repeat", super::string::repeat);
        self.register_scalar("space", super::string::space);

        // Date functions
        self.register_scalar("now", super::date::now);
        self.register_scalar("current_date", super::date::current_date);
        self.register_scalar("current_time", super::date::current_time);
        self.register_scalar("current_timestamp", super::date::current_timestamp);
        self.register_scalar("year", super::date::year);
        self.register_scalar("month", super::date::month);
        self.register_scalar("day", super::date::day);
        self.register_scalar("hour", super::date::hour);
        self.register_scalar("minute", super::date::minute);
        self.register_scalar("second", super::date::second);
        self.register_scalar("dayofweek", super::date::dayofweek);
        self.register_scalar("dayofyear", super::date::dayofyear);
        self.register_scalar("week", super::date::week);
        self.register_scalar("quarter", super::date::quarter);
        self.register_scalar("date_format", super::date::date_format);
        self.register_scalar("last_day", super::date::last_day);

        // Control functions
        self.register_scalar("coalesce", super::control::coalesce);
        self.register_scalar("nullif", super::control::nullif);
        self.register_scalar("ifnull", super::control::ifnull);
        self.register_scalar("isnull", super::control::isnull_func);

        // Conversion functions
        self.register_scalar("cast", super::conversion::cast);
        self.register_scalar("to_int", super::conversion::to_int);
        self.register_scalar("to_float", super::conversion::to_float);
        self.register_scalar("to_string", super::conversion::to_string);
        self.register_scalar("to_bool", super::conversion::to_bool);

        // Vector functions
        self.register_scalar("vector_distance", super::vector::vector_distance);
        self.register_scalar("vector_norm", super::vector::vector_norm);
        self.register_scalar("vector_normalize", super::vector::vector_normalize);
        self.register_scalar("vector_add", super::vector::vector_add);
        self.register_scalar("vector_sub", super::vector::vector_sub);
        self.register_scalar("vector_scale", super::vector::vector_scale);
        self.register_scalar("vector_dot", super::vector::vector_dot);
    }

    fn register_scalar(&mut self, name: &str, handler: FunctionHandler) {
        self.functions.insert(name.to_lowercase(), handler);
    }

    fn register_alias(&mut self, alias: &str, canonical: &str) {
        self.aliases
            .insert(alias.to_lowercase(), canonical.to_lowercase());
    }

    /// Look up a function by name (case-insensitive, with alias support).
    pub fn get(&self, name: &str) -> Option<FunctionHandler> {
        let lower = name.to_lowercase();
        // Try direct lookup first
        if let Some(&handler) = self.functions.get(&lower) {
            return Some(handler);
        }
        // Try alias
        if let Some(alias) = self.aliases.get(&lower) {
            return self.functions.get(alias).copied();
        }
        None
    }

    /// Execute a function by name with the given arguments.
    pub fn execute(&self, name: &str, args: &[ScalarValue]) -> Result<ScalarValue> {
        match self.get(name) {
            Some(handler) => handler(args),
            None => Err(TaibaiError::Sql(format!("unknown function '{}'", name))),
        }
    }

    /// Check if a function exists.
    pub fn exists(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to convert function arguments to f64.
pub fn args_to_f64(args: &[ScalarValue], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| match v {
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        _ => None,
    })
}

/// Helper to get a string argument.
pub fn args_to_string(args: &[ScalarValue], idx: usize) -> Option<String> {
    args.get(idx).and_then(|v| match v {
        ScalarValue::VarChar(Some(v)) => Some(v.clone()),
        _ => None,
    })
}

/// Helper to get an integer argument.
pub fn args_to_i64(args: &[ScalarValue], idx: usize) -> Option<i64> {
    args.get(idx).and_then(|v| match v {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        _ => None,
    })
}
