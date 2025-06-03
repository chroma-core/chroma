use std::{collections::HashMap, fs, path::PathBuf};

use serde::{Deserialize, Serialize};

use errors::MeteringRegistryError;

pub mod errors;
pub mod utils;

pub const REGISTRY_FILE_NAME: &str = "chroma_metering_registry.json";
pub const CARGO_MANIFEST_ENV_VAR: &str = "CARGO_MANIFEST_DIR";

pub struct AnnotatedField {
    pub field_name: String,
    pub attribute_name: String,
    pub mutator_name: String,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MeteringRegistry {
    attributes: HashMap<String, String>, // attribute_name -> attribute_type_tokens
    events: HashMap<String, HashMap<String, (String, String)>>, // event_name -> (field_name -> (attribute_name, mutator_name))
}

fn registry_path() -> Result<PathBuf, MeteringRegistryError> {
    let mut registry_path = PathBuf::from(
        std::env::var(CARGO_MANIFEST_ENV_VAR)
            .map_err(|_| MeteringRegistryError::CargoManifestError)?,
    );
    registry_path.push("target");
    registry_path.push(REGISTRY_FILE_NAME);
    return Ok(registry_path);
}

fn read_registry() -> Result<MeteringRegistry, MeteringRegistryError> {
    let registry_path = registry_path()?;
    if !registry_path.exists() {
        return Ok(MeteringRegistry::default());
    }
    let raw_registry = fs::read_to_string(&registry_path)?;
    let registry = match serde_json::from_str(&raw_registry) {
        Ok(result) => result,
        Err(_) => return Ok(MeteringRegistry::default()),
    };
    return Ok(registry);
}

fn write_registry(registry: &MeteringRegistry) -> Result<(), MeteringRegistryError> {
    let registry_path = registry_path()?;
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(registry_path, serde_json::to_string_pretty(&registry)?)?;
    return Ok(());
}

pub fn register_attribute(
    attribute_name: &str,
    attribute_type_tokens: &str,
) -> Result<(), MeteringRegistryError> {
    let mut registry = read_registry()?;

    // if registry.events.contains_key(attribute_name) {
    //     return Err(MeteringRegistryError::DuplicateAttributeError);
    // }

    registry.attributes.insert(
        attribute_name.to_string(),
        attribute_type_tokens.to_string(),
    );

    write_registry(&registry)?;

    Ok(())
}

pub fn list_registered_attributes() -> Result<HashMap<String, String>, MeteringRegistryError> {
    let registry = read_registry()?;
    let registered_attributes = registry.attributes;
    // TODO: don't return owned
    return Ok(registered_attributes.clone());
}

pub fn register_event(
    event_name: &str,
    annotated_fields: Vec<AnnotatedField>,
) -> Result<(), MeteringRegistryError> {
    let mut registry = read_registry()?;

    // if registry.events.contains_key(event_name) {
    //     return Err(MeteringRegistryError::DuplicateEventError);
    // }

    let mut annotated_fields_map = HashMap::new();

    for annotated_field in &annotated_fields {
        annotated_fields_map.insert(
            annotated_field.field_name.clone(),
            (
                annotated_field.attribute_name.clone(),
                annotated_field.mutator_name.clone(),
            ),
        );
    }

    registry
        .events
        .insert(event_name.to_string(), annotated_fields_map);

    write_registry(&registry)?;

    Ok(())
}
