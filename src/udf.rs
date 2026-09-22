use std::{
    any::Any,
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use arrow::datatypes::Fields;
use arrow::datatypes::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    execution::session_state::SessionStateBuilder,
    logical_expr::{
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use serde_json::Value;

use crate::arrow_json::build_array;
use crate::contract::{
    JevChoiceResult, JevOnError, JevQuestion, JevQuestionType, JevRequest, JevScoreResult,
};

use crate::parameter_names;
use crate::state::{canonical_bytes, parse_json_value, state_json_with_field, structured_text};
use crate::{
    execution::{batch_cache, fingerprint, BatchCache, JevPhysicalOptimizer},
    session::{validate_model, JevConfig},
    JevHttpError,
};
use crate::{
    metrics::{current_metrics, JevMetrics},
    provider::JevProvider,
};

#[derive(Debug, Clone)]
pub struct JevUdf {
    name: &'static str,
    signature: Signature,
    provider: Arc<JevProvider>,
    output: DataType,
}

impl PartialEq for JevUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && Arc::ptr_eq(&self.provider, &other.provider)
    }
}
impl Eq for JevUdf {}
impl Hash for JevUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        Arc::as_ptr(&self.provider).hash(state);
    }
}

impl JevUdf {
    pub fn new(name: &'static str, provider: Arc<JevProvider>) -> Result<Self> {
        let names = parameter_names(name)
            .ok_or_else(|| DataFusionError::Plan("unknown Jev function".into()))?;
        let signature =
            Signature::any(names.len(), Volatility::Stable).with_parameter_names(names)?;
        let output = match name {
            "noul" => DataType::Float64,
            "ask" => DataType::Utf8,
            "choice" => choice_type(),
            "score" => score_type(),
            _ => return datafusion::common::plan_err!("unknown Jev function"),
        };
        Ok(Self {
            name,
            signature,
            provider,
            output,
        })
    }
}

fn probability_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ])),
            false,
        )),
        false,
    )
}

fn choice_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("label", DataType::Utf8, true),
        Field::new("confidence", DataType::Float64, true),
        Field::new("probabilities", probability_map(), true),
    ]))
}

fn score_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("confidence", DataType::Float64, true),
        Field::new("probabilities", probability_map(), true),
    ]))
}

impl ScalarUDFImpl for JevUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if self.provider.api_key.is_none() {
            return datafusion::common::plan_err!(
                "{} requires {} before planning a Jev query",
                self.name,
                self.provider.key_name
            );
        }
        let expected = self.signature.parameter_names.as_ref().map_or(0, Vec::len);
        if args.arg_fields.len() != expected {
            return datafusion::common::plan_err!(
                "{} expects {expected} arguments after optional defaults are filled",
                self.name
            );
        }
        for (index, field) in args.arg_fields.iter().enumerate().skip(1) {
            if !matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            ) {
                return datafusion::common::plan_err!(
                    "{} argument {} must be UTF-8 text or NULL",
                    self.name,
                    index + 1
                );
            }
        }
        for (index, value) in args.scalar_arguments.iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let result = match index {
                0 if !value.is_null() => state_json_with_field(
                    (*value).clone(),
                    args.arg_fields.first().map(AsRef::as_ref),
                )
                .map(|_| ()),
                1 => required_text(
                    (*value).clone(),
                    if self.name == "ask" {
                        "questions"
                    } else {
                        "instructions"
                    },
                )
                .map(|_| ()),
                index if index == expected - 1 => optional_text((*value).clone(), "model")
                    .and_then(|value| validate_model(value.as_deref())),
                _ => Ok(()),
            };
            result.map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let validated = if self.name == "ask" {
            args.scalar_arguments
                .get(1)
                .copied()
                .flatten()
                .map(|value| {
                    let text = required_text(value.clone(), "questions")?;
                    parse_questions(&text).map(|_| ())
                })
        } else {
            args.scalar_arguments
                .get(2)
                .copied()
                .flatten()
                .map(|value| {
                    let criteria = parse_criteria(optional_text(value.clone(), "criteria")?)?;
                    validate_criteria(self.name, criteria.as_ref())
                })
        };
        if let Some(result) = validated {
            result.map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        Ok(Arc::new(Field::new(self.name, self.output.clone(), true)))
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::common::not_impl_err!("{} requires asynchronous execution", self.name)
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for JevUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expected = self.signature.parameter_names.as_ref().map_or(0, Vec::len);
        if args.args.len() != expected {
            return datafusion::common::exec_err!(
                "{} expects {expected} arguments after optional defaults are filled",
                self.name
            );
        }
        let settings = JevConfig::options(&args.config_options);
        let metrics = current_metrics();
        let cache = batch_cache()?;
        let mut results = Vec::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            match self
                .invoke_row(&args, row, &settings.model, &metrics, &cache)
                .await
            {
                Ok(value) => results.push(value),
                Err(error) => {
                    metrics.failures.add(1);
                    if settings.on_error != JevOnError::Null || is_fatal(&error) {
                        return Err(error);
                    }
                    results.push(Value::Null);
                }
            }
        }
        Ok(ColumnarValue::Array(
            build_array(&args.return_field, &results).map_err(DataFusionError::Execution)?,
        ))
    }
}

impl JevUdf {
    /// Validate each row before fingerprinting or spending. SQL NULL state is
    /// a successful NULL result, while invalid state/arguments are row errors.
    fn row_request(
        &self,
        values: &[ColumnarValue],
        state_field: Option<&Field>,
        row: usize,
        default_model: &str,
    ) -> Result<Option<JevRequest>> {
        let state = value_at(&values[0], row)?;
        if state.is_null() {
            return Ok(None);
        }
        let state = state_json_with_field(state, state_field)?;
        let questions = if self.name == "ask" {
            let text = required_text(value_at(&values[1], row)?, "questions")?;
            parse_questions(&text)?
        } else {
            let instructions =
                structured_text(required_text(value_at(&values[1], row)?, "instructions")?);
            let criteria = parse_criteria(optional_text(value_at(&values[2], row)?, "criteria")?)?;
            validate_criteria(self.name, criteria.as_ref())?;
            let question = JevQuestion {
                r#type: serde_json::from_value(Value::String(self.name.into()))
                    .map_err(json_error)?,
                instructions,
                criteria,
            };
            HashMap::from([("q0".into(), question)])
        };
        let model = optional_text(value_at(&values[values.len() - 1], row)?, "model")?
            .unwrap_or_else(|| default_model.to_owned());
        validate_model(Some(&model))?;
        Ok(Some(JevRequest {
            state,
            questions,
            model,
        }))
    }

    async fn invoke_row(
        &self,
        args: &ScalarFunctionArgs,
        row: usize,
        default_model: &str,
        metrics: &JevMetrics,
        cache: &Arc<Mutex<BatchCache>>,
    ) -> Result<Value> {
        let Some(request) = self.row_request(
            &args.args,
            args.arg_fields.first().map(AsRef::as_ref),
            row,
            default_model,
        )?
        else {
            return Ok(Value::Null);
        };
        let key = fingerprint(
            canonical_bytes(&serde_json::to_value(&request).map_err(json_error)?)?,
            args.config_options.clone(),
        )?;
        let cached = cache.lock().map_err(cache_error)?.get(&key);
        let raw = if let Some(raw) = cached {
            metrics.cache_hits.add(1);
            raw.ok_or_else(|| DataFusionError::Execution("cached Jev request failed".into()))?
        } else {
            cache.lock().map_err(cache_error)?.prepare()?;
            match self.provider.send(&request, self.name, metrics).await {
                Ok(raw) => cache
                    .lock()
                    .map_err(cache_error)?
                    .insert(key, Some(raw))?
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing Jev response after insertion".into())
                    })?,
                Err(error) => {
                    if !is_fatal(&error) {
                        cache.lock().map_err(cache_error)?.insert(key, None)?;
                    }
                    return Err(error);
                }
            }
        };
        // Keep raw bytes even when this typed wrapper cannot decode them:
        // another wrapper (including ask) can consume the same request.
        self.response_value(&raw)
    }

    fn response_value(&self, raw: &str) -> Result<Value> {
        if self.name == "ask" {
            return Ok(Value::String(raw.to_owned()));
        }
        let answer = crate::json_value::parse_value(raw).map_err(json_error)?;
        match self.name {
            "noul" => {
                let probability = answer["answers"]["q0"]["noul"].as_f64().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Jev noul answer must be a probability from 0 to 1".into(),
                    )
                })?;
                validate_probability(probability, "noul answer")?;
                Ok(Value::from(probability))
            }
            "choice" => {
                let mut answer = answer["answers"]["q0"]
                    .as_object()
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Execution("invalid Jev choice answer".into())
                    })?;
                let label = answer.get("choice").cloned().unwrap_or(Value::Null);
                answer.insert("label".into(), label);
                let typed: JevChoiceResult = decode_response(self.name, Value::Object(answer))?;
                validate_response_probabilities(self.name, typed.confidence, &typed.probabilities)?;
                serde_json::to_value(typed).map_err(json_error)
            }
            _ => {
                let typed: JevScoreResult =
                    decode_response(self.name, answer["answers"]["q0"].clone())?;
                validate_response_probabilities(self.name, typed.confidence, &typed.probabilities)?;
                serde_json::to_value(typed).map_err(json_error)
            }
        }
    }
}

/// Decode through the generated type, reporting only its top-level field.
/// Provider values and user-supplied probability keys must not enter errors.
fn decode_response<T: serde::de::DeserializeOwned>(function: &str, value: Value) -> Result<T> {
    serde_path_to_error::deserialize(value).map_err(|error| {
        let field = error
            .path()
            .iter()
            .next()
            .map(ToString::to_string)
            .unwrap_or_else(|| "result".into());
        DataFusionError::Execution(format!("invalid Jev {function} response field {field}"))
    })
}

fn validate_probability(value: f64, field: &str) -> Result<()> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        return Ok(());
    }
    datafusion::common::exec_err!("Jev {field} is outside 0.0..=1.0")
}

fn validate_response_probabilities(
    function: &str,
    confidence: f64,
    probabilities: &HashMap<String, f64>,
) -> Result<()> {
    validate_probability(confidence, &format!("{function} confidence"))?;
    for probability in probabilities.values() {
        validate_probability(*probability, &format!("{function} probabilities"))?;
    }
    Ok(())
}

// Do not expand a scalar rubric into one copy per row: large literal
// criteria remain one scalar and only the current request owns a copy.
fn value_at(value: &ColumnarValue, row: usize) -> Result<ScalarValue> {
    match value {
        ColumnarValue::Scalar(value) => Ok(value.clone()),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row),
    }
}

fn cache_error<T>(_: std::sync::PoisonError<T>) -> DataFusionError {
    DataFusionError::Internal("Jev batch cache unavailable".into())
}

fn is_fatal(error: &DataFusionError) -> bool {
    match error.find_root() {
        DataFusionError::Execution(_) => false,
        DataFusionError::External(error) => error
            .downcast_ref::<JevHttpError>()
            .is_some_and(|error| error.fatal),
        // Resource, planning, and engine errors are not failed judgments and
        // must never be hidden by the row-level null policy.
        _ => true,
    }
}

fn json_error(_: serde_json::Error) -> DataFusionError {
    DataFusionError::Execution("invalid Jev JSON value".into())
}

fn parse_criteria(text: Option<String>) -> Result<Option<Value>> {
    text.map(|text| {
        parse_json_value(&text).map_err(|_| {
            DataFusionError::Execution(
                "Jev criteria must be valid JSON without duplicate keys".into(),
            )
        })
    })
    .transpose()
}

fn validate_criteria(kind: &str, value: Option<&Value>) -> Result<()> {
    match (kind, value) {
        ("noul", None) => Ok(()),
        ("noul", Some(Value::Object(criteria)))
            if criteria.keys().all(|key| key == "true" || key == "false") =>
        {
            Ok(())
        }
        ("choice", Some(Value::Object(criteria))) if !criteria.is_empty() => Ok(()),
        ("score", Some(Value::Array(criteria))) if criteria.len() >= 2 => Ok(()),
        ("noul", _) => datafusion::common::exec_err!(
            "noul criteria must be a JSON object with true/false rubrics, or SQL NULL"
        ),
        ("choice", _) => {
            datafusion::common::exec_err!("choice criteria must be a nonempty JSON object")
        }
        ("score", _) => datafusion::common::exec_err!(
            "score criteria must be a JSON array with at least two ordered levels"
        ),
        _ => datafusion::common::exec_err!("unknown Jev criteria type"),
    }
}

fn parse_questions(text: &str) -> Result<HashMap<String, JevQuestion>> {
    let value = parse_json_value(text).map_err(|error| {
        DataFusionError::Execution(format!("ask questions must be valid JSON: {error}"))
    })?;
    let questions: HashMap<String, JevQuestion> = serde_json::from_value(value).map_err(|_| {
        DataFusionError::Execution("ask questions must be a JSON map of typed questions".into())
    })?;
    if questions.is_empty() {
        return datafusion::common::exec_err!("ask questions cannot be empty");
    }
    for question in questions.values() {
        let criteria = question
            .criteria
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(json_error)?;
        validate_criteria(question.r#type.as_str(), criteria.as_ref())?;
    }
    Ok(questions)
}

fn optional_text(value: ScalarValue, name: &str) -> Result<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value)
        }
        value if value.is_null() => Ok(None),
        _ => datafusion::common::exec_err!("Jev {name} must be UTF-8 text"),
    }
}

fn required_text(value: ScalarValue, name: &str) -> Result<String> {
    optional_text(value, name)?
        .ok_or_else(|| DataFusionError::Execution(format!("Jev {name} cannot be NULL")))
}

pub fn register_jev(context: &SessionContext, provider: Arc<JevProvider>) -> Result<()> {
    let names: Vec<_> = JevQuestionType::ALL
        .iter()
        .map(JevQuestionType::as_str)
        .chain(std::iter::once("ask"))
        .collect();
    // Validate the complete registration before changing the session.
    for name in &names {
        if context.state().scalar_functions().contains_key(*name) {
            return datafusion::common::plan_err!(
                "Jev cannot replace already registered function {name}"
            );
        }
    }
    let functions = names
        .into_iter()
        .map(|name| {
            Ok(Arc::new(
                AsyncScalarUDF::new(Arc::new(JevUdf::new(name, provider.clone())?))
                    .into_scalar_udf(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let state_ref = context.state_ref();
    let mut state = state_ref.write();
    *state = SessionStateBuilder::new_from_existing(state.clone())
        .with_optimizer_rule(Arc::new(crate::joins::JevInnerJoinFilters {
            functions: functions.clone(),
        }))
        .with_physical_optimizer_rule(Arc::new(JevPhysicalOptimizer {
            functions: functions.clone(),
        }))
        .build();
    if state
        .config()
        .options()
        .extensions
        .get::<JevConfig>()
        .is_none()
    {
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(JevConfig::try_from_options(crate::JevSessionOptions {
                model: provider.default_model.clone(),
                on_error: JevOnError::Fail,
            })?);
    }
    drop(state);
    for function in functions {
        context.register_udf(function.as_ref().clone());
    }
    Ok(())
}
