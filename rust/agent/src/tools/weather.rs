//! A dummy `get_weather` tool used to exercise the full tool pipeline:
//! schema generation -> provider format -> typed deserialization -> execution.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::error::AgentError;
use crate::tool::{Tool, ToolCallMetadata};

/// Model-supplied parameters for [`GetWeatherTool`].
#[derive(Debug, Deserialize, JsonSchema)]
pub struct WeatherParams {
    /// City or location to report the weather for.
    pub location: String,
}

/// Harness-supplied unit preference for [`GetWeatherTool`].
///
/// This is a `RuntimeParams` (not exposed to the model): the harness picks the
/// unit, defaulting to Fahrenheit when nothing is injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemperatureUnit {
    Celsius,
    #[default]
    Fahrenheit,
}

/// A canned weather tool: returns a fixed forecast string for any location,
/// rendered in the harness-selected temperature unit.
pub struct GetWeatherTool;

#[async_trait]
impl Tool for GetWeatherTool {
    type ModelSuppliedParams = WeatherParams;
    type RuntimeParams = TemperatureUnit;

    fn name(&self) -> &str {
        "get_weather"
    }

    fn description(&self) -> &str {
        "Get the current weather for a location."
    }

    async fn call(
        &self,
        params: Self::ModelSuppliedParams,
        unit: Self::RuntimeParams,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        let temperature = match unit {
            TemperatureUnit::Celsius => "22C",
            TemperatureUnit::Fahrenheit => "72F",
        };
        Ok((
            format!("It is {temperature} and sunny in {}.", params.location),
            None,
        ))
    }
}
