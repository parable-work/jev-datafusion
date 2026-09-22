use std::any::Any;

use crate::contract::{JevOnError, JevSessionOptions};
use datafusion::common::{
    config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions},
    Result,
};

/// DataFusion's configuration adapter for the generated session contract.
/// Deep cloning keeps SET changes out of already planned queries.
#[derive(Debug, Clone, Default)]
pub struct JevConfig(pub JevSessionOptions);

impl JevConfig {
    /// Validate generated request options before inserting them into an
    /// isolated DataFusion context. Existing tuple construction remains valid.
    pub fn try_from_options(options: JevSessionOptions) -> Result<Self> {
        validate_model(Some(&options.model))?;
        Ok(Self(options))
    }

    pub(crate) fn options(config: &ConfigOptions) -> JevSessionOptions {
        config
            .extensions
            .get::<Self>()
            .map(|extension| extension.0.clone())
            .unwrap_or_default()
    }
}

impl ConfigExtension for JevConfig {
    const PREFIX: &'static str = "jev";
}

impl ExtensionOptions for JevConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "on_error" => {
                self.0.on_error = match value {
                    "fail" => JevOnError::Fail,
                    "null" => JevOnError::Null,
                    _ => {
                        return datafusion::common::config_err!("jev.on_error must be fail or null")
                    }
                };
            }
            "model" => {
                validate_model(Some(value)).map_err(|_| {
                    datafusion::common::config_datafusion_err!("jev.model cannot be empty")
                })?;
                self.0.model = value.to_owned();
            }
            _ => return datafusion::common::config_err!("unknown Jev setting: {key}"),
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: "jev.on_error".into(),
                value: Some(self.0.on_error.to_string()),
                description: "fail aborts the query; null returns NULL for recoverable row errors",
            },
            ConfigEntry {
                key: "jev.model".into(),
                value: Some(self.0.model.clone()),
                description: "Default model for Jev calls that omit the model argument",
            },
        ]
    }
}

pub(crate) fn validate_model(model: Option<&str>) -> Result<()> {
    if model.is_some_and(|model| model.trim().is_empty()) {
        return datafusion::common::exec_err!("Jev model cannot be empty");
    }
    Ok(())
}
