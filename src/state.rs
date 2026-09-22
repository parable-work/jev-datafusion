use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow_schema::extension::ExtensionType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion_functions_json::{JsonUnionEncoder, JsonUnionValue};
use parquet::variant::{unshred_variant, Variant, VariantArray, VariantMetadata, VariantType};
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{value::RawValue, Value};

const MAX_NESTING: usize = 64;
pub(crate) const MAX_BYTES: usize = 1_048_576;

fn size_error() -> DataFusionError {
    DataFusionError::Execution("Jev state/request exceeds the local 1048576-byte JSON limit; reduce the input (provider context limits also apply)".into())
}

fn check_depth(depth: usize, container: bool) -> Result<()> {
    if depth + usize::from(container) > MAX_NESTING {
        return datafusion::common::exec_err!("Jev state exceeds 64 nesting levels");
    }
    Ok(())
}

pub(crate) fn state_json(value: ScalarValue) -> Result<Value> {
    convert_state(value, None)
}

pub(crate) fn state_json_with_field(value: ScalarValue, field: Option<&Field>) -> Result<Value> {
    match field {
        Some(field) => convert_state(value, Some(field)),
        None => state_json(value),
    }
}

fn convert_state(value: ScalarValue, field: Option<&Field>) -> Result<Value> {
    if let ScalarValue::Dictionary(_, value) = value {
        return state_json_with_field(*value, field);
    }
    let value = match value {
        value if is_json_field(field) => nested_value(value, field, 0)?,
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => {
            parse_structured_text(&text)?.unwrap_or(Value::String(text))
        }
        value => nested_value(value, field, 0)?,
    };
    if !matches!(value, Value::String(_) | Value::Object(_) | Value::Array(_)) {
        return datafusion::common::exec_err!("Jev state must be text, a JSON object or array, Struct, Map, or JSON-compatible Variant");
    }
    canonical_bytes(&value)?;
    Ok(value)
}

fn nested_value(value: ScalarValue, field: Option<&Field>, depth: usize) -> Result<Value> {
    check_depth(depth, false)?;
    check_scalar_container(&value)?;
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let ScalarValue::Dictionary(_, value) = value {
        return nested_value(*value, field, depth);
    }
    if is_json_field(field) {
        return match value {
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)) => json_text_value(&text, depth),
            _ => {
                datafusion::common::exec_err!("Jev arrow.json state must have UTF-8 string storage")
            }
        };
    }
    if field.is_some_and(|field| field.extension_type_name() == Some(VariantType::NAME)) {
        let ScalarValue::Struct(array) = value else {
            return datafusion::common::exec_err!(
                "Jev Parquet Variant state must have Struct storage"
            );
        };
        return variant_array_value(array.as_ref(), depth);
    }
    check_depth(
        depth,
        matches!(
            &value,
            ScalarValue::Struct(_)
                | ScalarValue::Map(_)
                | ScalarValue::List(_)
                | ScalarValue::LargeList(_)
                | ScalarValue::FixedSizeList(_)
        ),
    )?;
    let value = match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Value::String(value),
        ScalarValue::Boolean(Some(value)) => Value::Bool(value),
        ScalarValue::Int8(Some(value)) => Value::from(value),
        ScalarValue::Int16(Some(value)) => Value::from(value),
        ScalarValue::Int32(Some(value)) => Value::from(value),
        ScalarValue::Int64(Some(value)) => Value::from(value),
        ScalarValue::UInt8(Some(value)) => Value::from(value),
        ScalarValue::UInt16(Some(value)) => Value::from(value),
        ScalarValue::UInt32(Some(value)) => Value::from(value),
        ScalarValue::UInt64(Some(value)) => Value::from(value),
        ScalarValue::Decimal32(Some(value), _, scale) => decimal_value(value, scale)?,
        ScalarValue::Decimal64(Some(value), _, scale) => decimal_value(value, scale)?,
        ScalarValue::Decimal128(Some(value), _, scale) => decimal_value(value, scale)?,
        ScalarValue::Decimal256(Some(value), _, scale) => decimal_value(value, scale)?,
        ScalarValue::Float16(Some(value)) if value.is_finite() => Value::from(value.to_f64()),
        ScalarValue::Float32(Some(value)) if value.is_finite() => Value::from(f64::from(value)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Value::from(value),
        ScalarValue::Struct(array) => {
            let mut fields = BTreeMap::new();
            for (field, column) in array.fields().iter().zip(array.columns()) {
                let value = nested_value(ScalarValue::try_from_array(column, 0)?, Some(field), depth + 1)?;
                fields.insert(field.name().clone(), value);
            }
            Value::Object(fields.into_iter().collect())
        }
        ScalarValue::Map(array) => {
            let entries = array.value(0);
            let mut fields = BTreeMap::new();
            for row in 0..entries.len() {
                let key = nested_value(ScalarValue::try_from_array(entries.column(0), row)?, Some(entries.fields()[0].as_ref()), depth + 1)?;
                let Value::String(key) = key else {
                    return datafusion::common::exec_err!("Jev state Map keys must be non-NULL UTF-8 strings");
                };
                let value = nested_value(ScalarValue::try_from_array(entries.column(1), row)?, Some(entries.fields()[1].as_ref()), depth + 1)?;
                if fields.insert(key, value).is_some() {
                    return datafusion::common::exec_err!("Jev state Map has duplicate keys");
                }
            }
            Value::Object(fields.into_iter().collect())
        }
        ScalarValue::List(array) => return list_value(array.value(0), array.data_type(), depth + 1),
        ScalarValue::LargeList(array) => return list_value(array.value(0), array.data_type(), depth + 1),
        ScalarValue::FixedSizeList(array) => return list_value(array.value(0), array.data_type(), depth + 1),
        value @ ScalarValue::Union(..) => return json_union_value(value, depth),
        value => return datafusion::common::exec_err!("Jev state cannot represent {:?} losslessly in JSON; cast explicitly to a supported form", value.data_type()),
    };
    Ok(value)
}

fn check_scalar_container(value: &ScalarValue) -> Result<()> {
    let rows = match value {
        ScalarValue::Struct(array) => {
            check_struct_fields(array)?;
            array.len()
        }
        ScalarValue::Map(array) => array.len(),
        ScalarValue::List(array) => array.len(),
        ScalarValue::LargeList(array) => array.len(),
        ScalarValue::FixedSizeList(array) => array.len(),
        _ => return Ok(()),
    };
    if rows != 1 {
        return datafusion::common::exec_err!(
            "Jev state ScalarValue containers must contain exactly one row"
        );
    }
    Ok(())
}

fn check_struct_fields(array: &StructArray) -> Result<()> {
    let mut names = BTreeSet::new();
    if array
        .fields()
        .iter()
        .any(|field| !names.insert(field.name()))
    {
        return datafusion::common::exec_err!("Jev state Struct has duplicate field names");
    }
    Ok(())
}

fn variant_array_value(array: &StructArray, depth: usize) -> Result<Value> {
    let invalid =
        || DataFusionError::Execution("Jev state contains invalid Parquet Variant data".into());
    // The pinned codec's shallow constructor/iterators can panic on corrupt
    // input, and its fallible constructor recursively validates without a depth
    // bound. First walk with our bound inside this narrow codec panic boundary;
    // then fully validate the bounded tree with the upstream fallible decoder.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let metadata = array.column_by_name("metadata").ok_or_else(invalid)?;
        if metadata.is_null(0) {
            return Err(invalid());
        }
        let metadata = binary_value(metadata.as_ref(), 0)?;
        // Unshredding also eagerly validates encoded fragments. Bound those
        // fragments and the physical Arrow tree before entering that kernel.
        preflight_variant_storage(array, 0, metadata, 0)?;
        let variant = VariantArray::try_new(array).map_err(|_| invalid())?;
        if variant.metadata_field().is_null(0) {
            return Err(invalid());
        }
        let variant = unshred_variant(&variant).map_err(|_| invalid())?;
        let Some(value) = variant.value_field().filter(|value| value.is_valid(0)) else {
            return Ok(Value::Null);
        };
        decode_variant(metadata, binary_value(value.as_ref(), 0)?, depth)
    }))
    .map_err(|_| invalid())?
}

fn decode_variant(metadata: &[u8], bytes: &[u8], depth: usize) -> Result<Value> {
    let invalid =
        || DataFusionError::Execution("Jev state contains invalid Parquet Variant data".into());
    let parsed_metadata = VariantMetadata::try_new(metadata).map_err(|_| invalid())?;
    if bytes.is_empty() {
        return Err(invalid());
    }
    let result = variant_value(Variant::new_with_metadata(parsed_metadata, bytes), depth)?;
    canonical_bytes(&result)?;
    Variant::try_new(metadata, bytes).map_err(|_| invalid())?;
    Ok(result)
}

fn binary_value(array: &dyn Array, row: usize) -> Result<&[u8]> {
    Ok(match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(row),
        DataType::LargeBinary => array.as_binary::<i64>().value(row),
        DataType::BinaryView => array.as_binary_view().value(row),
        _ => {
            return datafusion::common::exec_err!(
                "Jev Parquet Variant metadata/value fields must have binary storage"
            )
        }
    })
}

fn preflight_variant_storage(
    array: &dyn Array,
    row: usize,
    metadata: &[u8],
    physical_depth: usize,
) -> Result<()> {
    // Each logical shredded container adds a wrapper Struct and a typed_value
    // container (plus a list-element wrapper). Keep the upstream recursive Arrow
    // kernels bounded; decoded JSON still has the exact 64-container limit.
    if physical_depth > 3 * MAX_NESTING {
        return check_depth(MAX_NESTING + 1, false);
    }
    if array.is_null(row) {
        return Ok(());
    }
    let children = match array.data_type() {
        DataType::Struct(_) => {
            let array = array.as_struct();
            check_struct_fields(array)?;
            for (field, column) in array.fields().iter().zip(array.columns()) {
                if field.name() == "value"
                    && matches!(
                        column.data_type(),
                        DataType::Binary | DataType::BinaryView | DataType::LargeBinary
                    )
                    && column.is_valid(row)
                {
                    decode_variant(metadata, binary_value(column.as_ref(), row)?, 0)?;
                } else if !(physical_depth == 0 && field.name() == "metadata") {
                    preflight_variant_storage(column.as_ref(), row, metadata, physical_depth + 1)?;
                }
            }
            return Ok(());
        }
        DataType::List(_) => Some(array.as_list::<i32>().value(row)),
        DataType::LargeList(_) => Some(array.as_list::<i64>().value(row)),
        DataType::ListView(_) => Some(array.as_list_view::<i32>().value(row)),
        DataType::LargeListView(_) => Some(array.as_list_view::<i64>().value(row)),
        DataType::FixedSizeList(_, _) => Some(array.as_fixed_size_list().value(row)),
        _ => None,
    };
    if let Some(children) = children {
        for row in 0..children.len() {
            preflight_variant_storage(children.as_ref(), row, metadata, physical_depth + 1)?;
        }
    }
    Ok(())
}

fn json_union_value(value: ScalarValue, depth: usize) -> Result<Value> {
    let array = value.to_array()?;
    let encoder = JsonUnionEncoder::from_union(array.as_union().clone())
        .ok_or_else(|| DataFusionError::Execution("Jev state only supports the datafusion-functions-json Union type; cast other Unions explicitly".into()))?;
    Ok(match encoder.get_value(0) {
        JsonUnionValue::JsonNull => Value::Null,
        JsonUnionValue::Bool(value) => Value::Bool(value),
        JsonUnionValue::Int(value) => Value::from(value),
        JsonUnionValue::Float(value) if value.is_finite() => Value::from(value),
        JsonUnionValue::Float(_) => {
            return datafusion::common::exec_err!("Jev state requires finite JSON numbers")
        }
        JsonUnionValue::Str(value) => Value::String(value.into()),
        JsonUnionValue::Array(text) | JsonUnionValue::Object(text) => json_text_value(text, depth)?,
    })
}

fn is_json_field(field: Option<&Field>) -> bool {
    field.is_some_and(|field| {
        field.extension_type_name() == Some(arrow_schema::extension::Json::NAME)
    })
}

fn json_text_value(text: &str, depth: usize) -> Result<Value> {
    if text.len() > MAX_BYTES {
        return Err(size_error());
    }
    let raw = serde_json::from_str::<&RawValue>(text)
        .map_err(|_| DataFusionError::Execution("Jev input contains invalid JSON".into()))?;
    raw_json_value(raw, depth)
}

/// Parse JSON without discarding duplicate keys or reinterpreting ordinary
/// object keys as serde_json's internal Number/RawValue markers. The returned
/// Value is already the schema-owned GenericJSON type; assign it directly.
pub(crate) fn parse_json_value(text: &str) -> Result<Value> {
    let value = json_text_value(text, 0)?;
    canonical_bytes(&value)?;
    Ok(value)
}

fn decimal_value(integer: impl std::fmt::Display, scale: i8) -> Result<Value> {
    canonical_number(&format!("{integer}e{}", -i16::from(scale))).map(Value::Number)
}

fn list_value(array: ArrayRef, data_type: &DataType, depth: usize) -> Result<Value> {
    let field = match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.as_ref())
        }
        _ => None,
    };
    (0..array.len())
        .map(|row| nested_value(ScalarValue::try_from_array(&array, row)?, field, depth))
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

fn variant_value(value: Variant<'_, '_>, depth: usize) -> Result<Value> {
    check_depth(
        depth,
        matches!(&value, Variant::List(_) | Variant::Object(_)),
    )?;
    Ok(match value {
        Variant::Null => Value::Null,
        Variant::BooleanTrue => Value::Bool(true),
        Variant::BooleanFalse => Value::Bool(false),
        Variant::Int8(value) => Value::from(value),
        Variant::Int16(value) => Value::from(value),
        Variant::Int32(value) => Value::from(value),
        Variant::Int64(value) => Value::from(value),
        Variant::Float(value) if value.is_finite() => Value::from(f64::from(value)),
        Variant::Double(value) if value.is_finite() => Value::from(value),
        Variant::Decimal4(value) => decimal_value(value.integer(), value.scale() as i8)?,
        Variant::Decimal8(value) => decimal_value(value.integer(), value.scale() as i8)?,
        Variant::Decimal16(value) => decimal_value(value.integer(), value.scale() as i8)?,
        Variant::String(value) => Value::String(value.into()),
        Variant::ShortString(value) => Value::String(value.as_str().into()),
        Variant::List(values) => Value::Array(values.iter().map(|value| variant_value(value, depth + 1)).collect::<Result<_>>()?),
        Variant::Object(values) => {
            let mut fields = serde_json::Map::new();
            for (key, value) in values.iter() {
                if fields.insert(key.into(), variant_value(value, depth + 1)?).is_some() {
                    return datafusion::common::exec_err!("Jev state Variant has duplicate object keys");
                }
            }
            Value::Object(fields)
        }
        _ => return datafusion::common::exec_err!("Jev state cannot represent this Parquet Variant type losslessly in JSON; cast timestamps, dates, binary, UUIDs, or non-finite numbers explicitly to a supported form"),
    })
}

pub(crate) fn structured_text(text: String) -> Value {
    parse_structured_text(&text)
        .ok()
        .flatten()
        .unwrap_or(Value::String(text))
}

fn parse_structured_text(text: &str) -> Result<Option<Value>> {
    if text.len() > MAX_BYTES {
        return Err(size_error());
    }
    let Ok(raw) = serde_json::from_str::<&RawValue>(text) else {
        return Ok(None);
    };
    if !raw.get().starts_with(['{', '[']) {
        return Ok(None);
    }
    parse_json_value(text).map(Some)
}

fn raw_json_value(raw: &RawValue, depth: usize) -> Result<Value> {
    check_depth(depth, raw.get().starts_with(['{', '[']))?;
    let invalid =
        |error: serde_json::Error| DataFusionError::Execution(format!("Invalid Jev JSON: {error}"));
    match raw.get().as_bytes()[0] {
        b'{' => {
            let fields: JsonFields = serde_json::from_str(raw.get()).map_err(invalid)?;
            fields
                .0
                .into_iter()
                .map(|(key, value)| Ok((key, raw_json_value(&value, depth + 1)?)))
                .collect::<Result<serde_json::Map<_, _>>>()
                .map(Value::Object)
        }
        b'[' => {
            let values: Vec<&RawValue> = serde_json::from_str(raw.get()).map_err(invalid)?;
            values
                .into_iter()
                .map(|value| raw_json_value(value, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        _ => serde_json::from_str(raw.get()).map_err(invalid),
    }
}

// Keep serde_json's actual syntax/number parser, but reject duplicate names
// before its default object deserializer can discard an earlier value.
struct JsonFields(BTreeMap<String, Box<RawValue>>);

impl<'de> Deserialize<'de> for JsonFields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = JsonFields;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom(
                            "Jev state JSON has duplicate object keys",
                        ));
                    }
                }
                Ok(JsonFields(fields))
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

pub(crate) fn canonical_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut output = BoundedJson(Vec::new());
    write_json(value, 0, &mut output)?;
    Ok(output.0)
}

struct BoundedJson(Vec<u8>);

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES - self.0.len() {
            return Err(std::io::Error::other(size_error().to_string()));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_json(value: &Value, depth: usize, output: &mut BoundedJson) -> Result<()> {
    check_depth(depth, matches!(value, Value::Object(_) | Value::Array(_)))?;
    let serialize_error = |_| size_error();
    match value {
        Value::Object(fields) => {
            output.write_all(b"{").map_err(serialize_error)?;
            for (index, (key, value)) in fields
                .iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .enumerate()
            {
                if index > 0 {
                    output.write_all(b",").map_err(serialize_error)?;
                }
                serde_json::to_writer(&mut *output, key).map_err(|_| size_error())?;
                output.write_all(b":").map_err(serialize_error)?;
                write_json(value, depth + 1, output)?;
            }
            output.write_all(b"}").map_err(serialize_error)?;
        }
        Value::Array(values) => {
            output.write_all(b"[").map_err(serialize_error)?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.write_all(b",").map_err(serialize_error)?;
                }
                write_json(value, depth + 1, output)?;
            }
            output.write_all(b"]").map_err(serialize_error)?;
        }
        Value::Number(number) => {
            output
                .write_all(
                    canonical_number(&number.to_string())?
                        .to_string()
                        .as_bytes(),
                )
                .map_err(serialize_error)?;
        }
        value => serde_json::to_writer(output, value).map_err(|_| size_error())?,
    }
    Ok(())
}

// Normalize decimal coefficients directly: converting through f64 would round
// large integers and decimals before the request or its cache key is produced.
fn canonical_number(text: &str) -> Result<serde_json::Number> {
    let invalid = || DataFusionError::Execution("Jev JSON number exponent is out of range".into());
    let negative = text.starts_with('-');
    let unsigned = text.trim_start_matches('-');
    let (coefficient, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((coefficient, exponent)) => {
            (coefficient, exponent.parse::<i64>().map_err(|_| invalid())?)
        }
        None => (unsigned, 0),
    };
    let fraction = coefficient
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits = coefficient.replace('.', "");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(serde_json::Number::from(0));
    }
    let significant = digits.trim_end_matches('0');
    let exponent = exponent
        .checked_sub(fraction as i64)
        .and_then(|exponent| exponent.checked_add((digits.len() - significant.len()) as i64))
        .ok_or_else(invalid)?;
    let point = (significant.len() as i64)
        .checked_add(exponent)
        .ok_or_else(invalid)?;
    let mut normalized = if negative {
        String::from("-")
    } else {
        String::new()
    };
    if (1..=21).contains(&point) {
        let point = point as usize;
        if point >= significant.len() {
            normalized.push_str(significant);
            normalized.extend(std::iter::repeat_n('0', point - significant.len()));
        } else {
            normalized.push_str(&significant[..point]);
            normalized.push('.');
            normalized.push_str(&significant[point..]);
        }
    } else if (-5..=0).contains(&point) {
        normalized.push_str("0.");
        normalized.extend(std::iter::repeat_n('0', (-point) as usize));
        normalized.push_str(significant);
    } else {
        normalized.push_str(&significant[..1]);
        if significant.len() > 1 {
            normalized.push('.');
            normalized.push_str(&significant[1..]);
        }
        normalized.push('e');
        normalized.push_str(&point.checked_sub(1).ok_or_else(invalid)?.to_string());
    }
    normalized.parse().map_err(|_| invalid())
}
