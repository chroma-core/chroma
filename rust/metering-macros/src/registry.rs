use crate::errors::MeteringMacrosError;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, f32::EPSILON, fs, path::PathBuf};

pub const REGISTRY_FILE_NAME: &str = "chroma_metering_registry.json";
pub const CARGO_MANIFEST_ENV_VAR: &str = "CARGO_MANIFEST_DIR";

/// Stores information for an annotated field within an event definition.
pub struct AnnotatedField {
    pub field_name_string: String,
    pub attribute_name_string: String,
    pub mutator_name_string: String,
}

/// Represents the structure of the registry stored on disk.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MeteringRegistry {
    attributes: HashMap<String, String>, // attribute_name -> attribute_type
    events: HashMap<String, HashMap<String, (String, String)>>, // event_name -> (field_name -> (attribute_name, mutator_name))
}

/// Returns the absolute path to the registry.
fn registry_path() -> Result<PathBuf, MeteringMacrosError> {
    let mut registry_path = PathBuf::from(
        std::env::var(CARGO_MANIFEST_ENV_VAR)
            .map_err(|_| MeteringMacrosError::CargoManifestError)?,
    );
    registry_path.push("target");
    registry_path.push(REGISTRY_FILE_NAME);
    return Ok(registry_path);
}

/// Reads the registry from disk.
fn read_registry() -> Result<MeteringRegistry, MeteringMacrosError> {
    let registry_path = registry_path()?;
    eprintln!("Registry path: {:?}", registry_path);
    eprintln!("Workspace path: {:?}", std::env::var("CARGO_WORKSPACE_DIR"));
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

/// Writes the registry to disk.
fn write_registry(registry: &MeteringRegistry) -> Result<(), MeteringMacrosError> {
    let registry_path = registry_path()?;
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(registry_path, serde_json::to_string_pretty(&registry)?)?;
    return Ok(());
}

/// # Overview
/// Registers an attribute in the global registry, overriding the existing entry if one exists.
///
/// # Arguments
/// * `attribute_name_string` - The attribute's name as a string.
/// * `attribute_type_string` - The type definition of the attribute as a string.
pub fn register_attribute(
    attribute_name_string: &str,
    attribute_type_string: &str,
) -> Result<(), MeteringMacrosError> {
    let mut registry = read_registry()?;

    registry.attributes.insert(
        attribute_name_string.to_string(),
        attribute_type_string.to_string(),
    );

    write_registry(&registry)?;

    Ok(())
}

/// # Overview
/// Gets a clone of the current mapping of attributes stored in the registry.
///
/// # Returns
/// * `Ok(registered_attributes)` if reading the registry succeeds.
/// * `Err(MeteringMacrosError)` if reading the registry fails.
pub fn get_registered_attributes() -> Result<HashMap<String, String>, MeteringMacrosError> {
    let registry = read_registry()?;
    let registered_attributes = registry.attributes;
    return Ok(registered_attributes.clone());
}

/// # Overview
/// Gets a registered attribute KV pair (name, type) given an attribute's name.
///
/// # Arguments
/// * `attribute_name_string` - A string containing the name of the attribute.
///
/// # Returns
/// * `Ok((attribute_name_string, attribute_type_string))` if the attribute is found in the registry.
/// * `Err(MeteringMacrosError)` if reading the registry fails or the attribute does not exist.
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

/// # Overview
/// This method registers an event in the registry. It overrides the existing entry for the event name
/// if one exists.
///
/// # Arguments
/// * `event_name_string` - The name of the event as a string.
/// * `annotated_fields` - A vector of [`AnnotatedField`] objects representing the event's annotated fields.
///
/// # Returns
/// * `Ok(())` if the event is registered.
/// * `Err(MeteringMacrosError)` if reading or writing the registry fails.
pub fn register_event(
    event_name_string: &str,
    annotated_fields: Vec<AnnotatedField>,
) -> Result<(), MeteringMacrosError> {
    // Open the registry.
    let mut registry = read_registry()?;

    // Create a map in which to store the annotated fields' names, mapped to tuples of their attributes and mutators.
    let mut annotated_fields_map = HashMap::new();

    // Populate the map.
    for annotated_field in &annotated_fields {
        annotated_fields_map.insert(
            annotated_field.field_name_string.clone(),
            (
                annotated_field.attribute_name_string.clone(),
                annotated_field.mutator_name_string.clone(),
            ),
        );
    }

    // Insert the event into the registry, overriding the existing entry for the event name if it exists.
    registry
        .events
        .insert(event_name_string.to_string(), annotated_fields_map);

    // Write the registry back to disk.
    write_registry(&registry)?;

    Ok(())
}
