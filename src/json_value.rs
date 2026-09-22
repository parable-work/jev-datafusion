//! JSON field adapter that keeps serde_json's number and raw-value markers as
//! ordinary object fields, including when the input is `serde_json::from_value`.

use std::{collections::HashMap, hash::Hash};

use serde::{
    de::{DeserializeOwned, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{value::RawValue, Map, Value};

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: JsonField,
{
    T::deserialize_json(deserializer)
}

pub trait JsonField: Sized {
    fn deserialize_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;
}

struct Field<T>(T);

impl<'de, T: JsonField> Deserialize<'de> for Field<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_json(deserializer).map(Self)
    }
}

impl JsonField for Value {
    fn deserialize_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        parse_raw(&raw, 0).map_err(serde::de::Error::custom)
    }
}

impl<T: JsonField> JsonField for Option<T> {
    fn deserialize_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<Field<T>>::deserialize(deserializer).map(|value| value.map(|field| field.0))
    }
}

impl<T: JsonField> JsonField for Vec<T> {
    fn deserialize_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<Field<T>>::deserialize(deserializer)
            .map(|values| values.into_iter().map(|field| field.0).collect())
    }
}

impl<K: DeserializeOwned + Eq + Hash, T: JsonField> JsonField for HashMap<K, T> {
    fn deserialize_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        HashMap::<K, Field<T>>::deserialize(deserializer).map(|values| {
            values
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect()
        })
    }
}

pub fn parse_value(text: &str) -> serde_json::Result<Value> {
    let raw = serde_json::from_str::<&RawValue>(text)?;
    parse_raw(raw, 0)
}

fn parse_raw(raw: &RawValue, depth: usize) -> serde_json::Result<Value> {
    if depth >= 127 && raw.get().starts_with(['{', '[']) {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "recursion limit exceeded",
        ));
    }
    match raw.get().as_bytes()[0] {
        b'{' => {
            let mut deserializer = serde_json::Deserializer::from_str(raw.get());
            deserializer.deserialize_map(ObjectVisitor { depth })
        }
        b'[' => serde_json::from_str::<Vec<Box<RawValue>>>(raw.get())?
            .into_iter()
            .map(|value| parse_raw(&value, depth + 1))
            .collect::<serde_json::Result<Vec<_>>>()
            .map(Value::Array),
        _ => serde_json::from_str(raw.get()),
    }
}

struct ObjectVisitor {
    depth: usize,
}

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut entries: M) -> Result<Value, M::Error> {
        let mut fields = Map::new();
        while let Some((name, raw)) = entries.next_entry::<String, Box<RawValue>>()? {
            fields.insert(
                name,
                parse_raw(&raw, self.depth + 1).map_err(serde::de::Error::custom)?,
            );
        }
        Ok(Value::Object(fields))
    }
}
