//! The tool abstraction.
//!
//! You implement [`Tool`] (a typed trait with associated `ModelSuppliedParams`
//! and `RuntimeParams`); a blanket impl turns any `Tool` into the object-safe
//! [`DynTool`] stored in a [`ToolSet`]. The params JSON schema is derived
//! automatically from `ModelSuppliedParams` via `schemars` and rendered through
//! [`ProviderFormat::format_tool`] -- you never write a schema or any provider
//! conversion by hand.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::AgentError;
use crate::provider::ProviderFormat;

/// Optional structured metadata a tool can attach to its result.
///
/// Extension point for later milestones (e.g. citing chunk ids); no variants
/// exist yet, so tools return `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolCallMetadata {}

/// THE trait you implement to define a tool.
///
/// `ModelSuppliedParams` are decoded from the model's tool call;
/// `RuntimeParams` are injected by the harness (Python's `overrides`) and
/// default to `()` when nothing is injected. The JSON schema advertised to the
/// provider is derived from `ModelSuppliedParams`.
#[async_trait]
pub trait Tool: Send + Sync + 'static {
    type ModelSuppliedParams: DeserializeOwned + JsonSchema + Send;
    type RuntimeParams: Default + Send + Sync + 'static;

    fn name(&self) -> &str;
    fn description(&self) -> &str;
    async fn call(
        &self,
        params: Self::ModelSuppliedParams,
        runtime: Self::RuntimeParams,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError>;
}

/// Object-safe form of a tool, held type-erased in a [`ToolSet`].
///
/// Provided automatically by the blanket impl below for every `T: Tool` --
/// there is no wrapper struct and you never implement this directly.
#[async_trait]
pub trait DynTool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    /// Render this tool's definition (name, description, and the generated
    /// params JSON schema) into `provider`'s wire format.
    fn to_provider_format(&self, provider: ProviderFormat) -> Value;
    /// Decode `params` into the tool's `ModelSuppliedParams`, downcast
    /// `runtime` into its `RuntimeParams` (or default when `None`), and call it.
    async fn call_json(
        &self,
        params: Value,
        runtime: Option<Box<dyn Any + Send>>,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError>;
}

#[async_trait]
impl<T: Tool> DynTool for T {
    fn name(&self) -> &str {
        Tool::name(self)
    }

    fn description(&self) -> &str {
        Tool::description(self)
    }

    fn to_provider_format(&self, provider: ProviderFormat) -> Value {
        provider.format_tool(
            Tool::name(self),
            Tool::description(self),
            params_input_schema::<T::ModelSuppliedParams>(),
        )
    }

    async fn call_json(
        &self,
        params: Value,
        runtime: Option<Box<dyn Any + Send>>,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        let params: T::ModelSuppliedParams = serde_json::from_value(params)?;
        let runtime: T::RuntimeParams = match runtime {
            Some(boxed) => *boxed.downcast::<T::RuntimeParams>().map_err(|_| {
                AgentError::ToolRuntimeParamsTypeMismatch {
                    tool: Tool::name(self).to_string(),
                }
            })?,
            None => T::RuntimeParams::default(),
        };
        self.call(params, runtime).await
    }
}

/// Generate a clean JSON-Schema object for a params type.
///
/// Subschemas are inlined and the `$schema`/`title` metadata keys stripped so
/// the result is a plain object suitable for provider `input_schema` fields.
fn params_input_schema<P: JsonSchema>() -> Value {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.inline_subschemas = true;
        s.meta_schema = None;
    });
    let root = settings.into_generator().into_root_schema_for::<P>();
    let mut value = serde_json::to_value(root).expect("schema serializes to JSON");
    if let Some(obj) = value.as_object_mut() {
        obj.remove("title");
        obj.remove("$schema");
    }
    value
}

/// An ordered registry of tools, keyed by name.
///
/// Iteration order follows registration order (via `IndexMap`), which keeps the
/// emitted provider `tools` array stable across runs -- useful for prompt
/// caching -- while still giving O(1) name lookup for resolving tool calls.
#[derive(Default)]
pub struct ToolSet {
    tools: IndexMap<String, Arc<dyn DynTool>>,
}

impl ToolSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a tool (replacing any existing tool with the same name).
    pub fn add<T: Tool>(&mut self, tool: T) {
        let tool: Arc<dyn DynTool> = Arc::new(tool);
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Look up a tool by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DynTool>> {
        self.tools.get(name).cloned()
    }

    /// Render every registered tool's schema into `provider`'s wire format,
    /// preserving registration order. Schemas are generated on demand.
    pub fn get_formats(&self, provider: ProviderFormat) -> Vec<Value> {
        self.tools
            .values()
            .map(|tool| tool.to_provider_format(provider))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::weather::GetWeatherTool;
    use serde_json::json;

    #[test]
    fn get_formats_anthropic_shape() {
        let mut toolset = ToolSet::new();
        toolset.add(GetWeatherTool);

        let formats = toolset.get_formats(ProviderFormat::Anthropic);
        assert_eq!(formats.len(), 1);

        let format = &formats[0];
        assert_eq!(format["name"], "get_weather");
        assert!(format["description"].is_string());

        let schema = &format["input_schema"];
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["location"]["type"], "string");
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "location"));
    }

    #[tokio::test]
    async fn call_json_defaults_runtime_params() {
        let tool = GetWeatherTool;
        let (text, meta) = DynTool::call_json(&tool, json!({ "location": "Paris" }), None)
            .await
            .expect("call succeeds");
        assert_eq!(text, "It is 72F and sunny in Paris.");
        assert!(meta.is_none());
    }

    #[tokio::test]
    async fn call_json_uses_injected_runtime_params() {
        use crate::tools::weather::TemperatureUnit;

        let tool = GetWeatherTool;
        let unit: Box<dyn Any + Send> = Box::new(TemperatureUnit::Celsius);
        let (text, _meta) = DynTool::call_json(&tool, json!({ "location": "Paris" }), Some(unit))
            .await
            .expect("call succeeds");
        assert_eq!(text, "It is 22C and sunny in Paris.");
    }

    #[tokio::test]
    async fn call_json_wrong_runtime_params_type_errors() {
        let tool = GetWeatherTool;
        let wrong: Box<dyn Any + Send> = Box::new(42_i32);
        let err = DynTool::call_json(&tool, json!({ "location": "Paris" }), Some(wrong))
            .await
            .expect_err("type mismatch should error, not panic");
        assert!(matches!(
            err,
            AgentError::ToolRuntimeParamsTypeMismatch { .. }
        ));
    }
}
