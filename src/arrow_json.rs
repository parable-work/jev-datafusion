//! Materialize judgment JSON into the Arrow return types of the SQL functions.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::{Map, Value};

pub fn build_array(field: &Field, values: &[Value]) -> Result<ArrayRef, String> {
    match field.data_type() {
        DataType::Utf8 => build_utf8(field, values),
        DataType::Float64 => build_f64(field, values),
        DataType::Struct(_) | DataType::Map(_, _) => build_nested(field, values),
        other => Err(format!(
            "field {:?} has unsupported Arrow data type {other:?}",
            field.name()
        )),
    }
}

fn row_error(field: &Field, row: usize, message: impl AsRef<str>) -> String {
    format!("row {row} field {:?}: {}", field.name(), message.as_ref())
}

fn build_utf8(field: &Field, values: &[Value]) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for (row, value) in values.iter().enumerate() {
        match value {
            Value::Null => builder.append_null(),
            Value::String(value) => builder.append_value(value),
            other => {
                return Err(row_error(
                    field,
                    row,
                    format!("expected Utf8 string, got {other:?}"),
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_f64(field: &Field, values: &[Value]) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
            continue;
        }
        match value.as_f64().filter(|value| value.is_finite()) {
            Some(value) => builder.append_value(value),
            None => return Err(row_error(field, row, "expected Float64 number")),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_nested(field: &Field, values: &[Value]) -> Result<ArrayRef, String> {
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let normalized = values
        .iter()
        .enumerate()
        .map(|(row, value)| {
            let mut wrapper = Map::with_capacity(1);
            wrapper.insert(
                field.name().clone(),
                normalize_value(value, field.data_type(), field.name())
                    .map_err(|error| row_error(field, row, error))?,
            );
            Ok(Value::Object(wrapper))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut encoded = Vec::new();
    for (index, value) in normalized.iter().enumerate() {
        if index > 0 {
            encoded.push(b'\n');
        }
        serde_json::to_writer(&mut encoded, value).map_err(|error| {
            format!("failed to encode nested field {:?}: {error}", field.name())
        })?;
    }
    let mut decoder = arrow::json::ReaderBuilder::new(schema.clone())
        .with_batch_size(values.len().max(1))
        .build_decoder()
        .map_err(|error| format!("failed to build nested decoder: {error}"))?;
    let consumed = decoder
        .decode(&encoded)
        .map_err(|error| format!("failed to decode nested field {:?}: {error}", field.name()))?;
    if consumed != encoded.len() {
        return Err(format!(
            "nested field {:?} did not consume every row",
            field.name()
        ));
    }
    let batch = decoder
        .flush()
        .map_err(|error| {
            format!(
                "failed to materialize nested field {:?}: {error}",
                field.name()
            )
        })?
        .unwrap_or_else(|| RecordBatch::new_empty(schema));
    if batch.num_rows() != values.len() {
        return Err(format!(
            "nested field {:?} produced {} rows for {} inputs",
            field.name(),
            batch.num_rows(),
            values.len()
        ));
    }
    Ok(batch.column(0).clone())
}

fn normalize_value(value: &Value, data_type: &DataType, path: &str) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let normalized = match data_type {
        DataType::Utf8 => match value {
            Value::String(value) => Value::String(value.clone()),
            other => return Err(format!("{path} expected Utf8 string, got {other:?}")),
        },
        DataType::Float64 => match value.as_f64().filter(|number| number.is_finite()) {
            Some(number) => Value::Number(
                serde_json::Number::from_f64(number)
                    .ok_or_else(|| format!("{path} expected Float64 number"))?,
            ),
            None => return Err(format!("{path} expected Float64 number, got {value:?}")),
        },
        DataType::Struct(fields) => match value.as_object() {
            Some(map) => {
                let mut output = Map::with_capacity(fields.len());
                for field in fields {
                    if let Some(child) = map.get(field.name()) {
                        let child_path = format!("{path}.{}", field.name());
                        output.insert(
                            field.name().clone(),
                            normalize_value(child, field.data_type(), &child_path)?,
                        );
                    }
                }
                Value::Object(output)
            }
            None => return Err(format!("{path} expected object, got {value:?}")),
        },
        DataType::Map(entries, _) => {
            let value_type = match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields[1].data_type(),
                other => return Err(format!("{path} has invalid Arrow Map entries {other:?}")),
            };
            match value.as_object() {
                Some(map) => Value::Object(
                    map.iter()
                        .map(|(key, child)| {
                            normalize_value(child, value_type, &format!("{path}.{key}"))
                                .map(|value| (key.clone(), value))
                        })
                        .collect::<Result<Map<_, _>, _>>()?,
                ),
                None => return Err(format!("{path} expected object map, got {value:?}")),
            }
        }
        other => {
            return Err(format!(
                "{path} has unsupported nested Arrow type {other:?}"
            ))
        }
    };
    Ok(normalized)
}
