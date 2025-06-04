use crate::errors::MeteringMacrosError;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf};

pub const REGISTRY_FILE_NAME: &str = "chroma_metering_registry.json";
pub const CARGO_MANIFEST_ENV_VAR: &str = "CARGO_MANIFEST_DIR";

pub struct AnnotatedField {
    pub field_name_string: String,
    pub attribute_name_string: String,
    pub mutator_name_string: String,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MeteringRegistry {
    attributes: HashMap<String, String>, // attribute_name_string -> attribute_type_string
    events: HashMap<String, HashMap<String, (String, String)>>, // event_name -> (field_name_string -> (attribute_name_string, mutator_name_string))
}

fn registry_path() -> Result<PathBuf, MeteringMacrosError> {
    let mut registry_path = PathBuf::from(
        std::env::var(CARGO_MANIFEST_ENV_VAR)
            .map_err(|_| MeteringMacrosError::CargoManifestError)?,
    );
    registry_path.push("target");
    registry_path.push(REGISTRY_FILE_NAME);
    return Ok(registry_path);
}

fn read_registry() -> Result<MeteringRegistry, MeteringMacrosError> {
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

fn write_registry(registry: &MeteringRegistry) -> Result<(), MeteringMacrosError> {
    let registry_path = registry_path()?;
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(registry_path, serde_json::to_string_pretty(&registry)?)?;
    return Ok(());
}

pub fn register_attribute(
    attribute_name_string: &str,
    attribute_type_string: &str,
) -> Result<(), MeteringMacrosError> {
    let mut registry = read_registry()?;

    // if registry.events.contains_key(attribute_name_string) {
    //     return Err(MeteringMacrosError::DuplicateAttributeError);
    // }

    registry.attributes.insert(
        attribute_name_string.to_string(),
        attribute_type_string.to_string(),
    );

    write_registry(&registry)?;

    Ok(())
}

pub fn get_registered_attributes() -> Result<HashMap<String, String>, MeteringMacrosError> {
    let registry = read_registry()?;
    let registered_attributes = registry.attributes;
    // TODO: don't return owned
    return Ok(registered_attributes.clone());
}

pub fn get_registered_attribute(
    attribute_name_string: &str,
) -> Result<(String, String), MeteringMacrosError> {
    let registry = read_registry()?;

    match registry.attributes.get(attribute_name_string) {
        Some(attribute_type_string) => Ok((
            attribute_name_string.to_string(),
            attribute_type_string.clone(),
        )),
        None => Err(MeteringMacrosError::AttributeNotFoundError(
            attribute_name_string.to_string(),
        )),
    }
}

pub fn register_event(
    event_name: &str,
    annotated_fields: Vec<AnnotatedField>,
) -> Result<(), MeteringMacrosError> {
    let mut registry = read_registry()?;

    // if registry.events.contains_key(event_name) {
    //     return Err(MeteringMacrosError::DuplicateEventError);
    // }

    let mut annotated_fields_map = HashMap::new();

    for annotated_field in &annotated_fields {
        annotated_fields_map.insert(
            annotated_field.field_name_string.clone(),
            (
                annotated_field.attribute_name_string.clone(),
                annotated_field.mutator_name_string.clone(),
            ),
        );
    }

    registry
        .events
        .insert(event_name.to_string(), annotated_fields_map);

    write_registry(&registry)?;

    Ok(())
}
