//! Request and result types for the System One JSON contract.
//!
//! The SQL functions speak this shape. A server adapter maps it onto that
//! server's HTTP endpoint. Model ids are opaque strings.

use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::json_value;

pub const TYPESAFE_DEFAULT_MODEL: &str = "jev-1.13.0";
pub const OPENROUTER_DEFAULT_MODEL: &str = "typesafe/jev-1.13";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevArgumentName {
    State,
    Instructions,
    Criteria,
    Questions,
    Model,
}

impl JevArgumentName {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Instructions => "instructions",
            Self::Criteria => "criteria",
            Self::Questions => "questions",
            Self::Model => "model",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JevQuestionType {
    Noul,
    Choice,
    Score,
}

impl JevQuestionType {
    pub const ALL: [Self; 3] = [Self::Noul, Self::Choice, Self::Score];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Noul => "noul",
            Self::Choice => "choice",
            Self::Score => "score",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JevOnError {
    #[default]
    Fail,
    Null,
}

impl fmt::Display for JevOnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Fail => "fail",
            Self::Null => "null",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JevQuestion {
    #[serde(rename = "type")]
    pub r#type: JevQuestionType,
    #[serde(deserialize_with = "json_value::deserialize")]
    pub instructions: Value,
    #[serde(
        default,
        deserialize_with = "json_value::deserialize",
        skip_serializing_if = "Option::is_none"
    )]
    pub criteria: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JevRequest {
    #[serde(deserialize_with = "json_value::deserialize")]
    pub state: Value,
    pub questions: HashMap<String, JevQuestion>,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JevChoiceResult {
    pub label: String,
    pub confidence: f64,
    pub probabilities: HashMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JevScoreResult {
    pub score: f64,
    pub confidence: f64,
    pub probabilities: HashMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JevSessionOptions {
    pub model: String,
    pub on_error: JevOnError,
}

impl Default for JevSessionOptions {
    fn default() -> Self {
        Self {
            model: TYPESAFE_DEFAULT_MODEL.to_owned(),
            on_error: JevOnError::Fail,
        }
    }
}
