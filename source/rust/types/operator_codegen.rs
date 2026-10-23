/// Module for generating Rust operator constants from Go source code.
///
/// This module is used by the build script to automatically generate operator constants
/// by parsing the Go constants file at build time.
use std::fs;
use std::path::Path;

pub fn generate_operator_constants() -> Result<(), Box<dyn std::error::Error>> {
    // Get the workspace root - try multiple strategies for local vs Docker builds
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;

    // Strategy 1: Two levels up from rust/types (works for local builds)
    let workspace_root_local = Path::new(&manifest_dir).parent().and_then(|p| p.parent());

    // Strategy 2: Check if we're in a Docker container at /chroma
    let workspace_root_docker = Path::new("/chroma");

    // Try local path first, fall back to Docker path
    let go_constants_path = if let Some(root) = workspace_root_local {
        let local_path = root.join("go/pkg/sysdb/metastore/db/dbmodel/constants.go");
        if local_path.exists() {
            local_path
        } else {
            workspace_root_docker.join("go/pkg/sysdb/metastore/db/dbmodel/constants.go")
        }
    } else {
        workspace_root_docker.join("go/pkg/sysdb/metastore/db/dbmodel/constants.go")
    };

    let out_dir = std::env::var("OUT_DIR")?;
    let dest_path = Path::new(&out_dir).join("operators_generated.rs");

    // Tell Cargo to rerun if the Go file changes (use relative path for portability)
    println!("cargo:rerun-if-changed=go/pkg/sysdb/metastore/db/dbmodel/constants.go");

    // Read the Go constants file
    let go_content = fs::read_to_string(&go_constants_path)
        .map_err(|e| format!("Failed to read {}: {}", go_constants_path.display(), e))?;

    // Parse operator UUIDs and names
    let mut operators = Vec::new();

    // Parse UUID constants like:
    // FunctionRecordCounter = uuid.MustParse("ccf2e3ba-633e-43ba-9394-46b0c54c61e3")
    for line in go_content.lines() {
        let trimmed = line.trim();
        // Only match lines that contain uuid.MustParse to avoid parsing other function constants
        if trimmed.starts_with("Function") && trimmed.contains("uuid.MustParse") {
            if let Some(uuid_line) = trimmed.strip_prefix("Function") {
                if let Some(uuid_str) = extract_uuid_from_line(uuid_line) {
                    // Extract the function name from the Go constant name
                    // FunctionRecordCounter -> RecordCounter -> record_counter
                    if let Some(name_part) = uuid_line.split('=').next() {
                        let const_name = name_part.trim();
                        let operator_name = camel_to_snake_case(const_name);
                        operators.push((const_name.to_string(), operator_name.clone(), uuid_str));
                    }
                }
            }
        }
    }

    // Also parse name constants like:
    // FunctionNameRecordCounter = "record_counter"
    let mut name_map = std::collections::HashMap::new();
    for line in go_content.lines() {
        if let Some(name_line) = line.trim().strip_prefix("FunctionName") {
            if let Some((const_name, name_str)) = extract_name_from_line(name_line) {
                name_map.insert(const_name, name_str);
            }
        }
    }

    // Generate Rust code
    let mut rust_code = String::from(
        "/// Well-known function IDs and names that are pre-populated in the database\n",
    );
    rust_code.push_str("/// \n");
    rust_code.push_str("/// GENERATED CODE - DO NOT EDIT MANUALLY\n");
    rust_code.push_str(
        "/// This file is auto-generated from go/pkg/sysdb/metastore/db/dbmodel/constants.go\n",
    );
    rust_code.push_str("/// by the build script in rust/types/build.rs\n");
    rust_code.push_str("use uuid::Uuid;\n\n");

    for (go_const_name, rust_name_base, uuid_str) in &operators {
        // Parse UUID to get byte array
        let uuid_bytes = parse_uuid_to_bytes(uuid_str)?;

        // Get the name constant from the name map if available
        let name_value = name_map
            .get(&format!("Name{}", go_const_name))
            .map(|s| s.as_str())
            .unwrap_or(rust_name_base.as_str());

        rust_code.push_str(&format!(
            "/// UUID for the built-in {} function\n",
            name_value
        ));
        rust_code.push_str(&format!(
            "pub const FUNCTION_{}_ID: Uuid = Uuid::from_bytes([\n",
            rust_name_base.to_uppercase()
        ));
        rust_code.push_str(&format!("    {}\n", format_uuid_bytes(&uuid_bytes)));
        rust_code.push_str("]);\n");

        rust_code.push_str(&format!(
            "/// Name of the built-in {} function\n",
            name_value
        ));
        rust_code.push_str(&format!(
            "pub const FUNCTION_{}_NAME: &str = \"{}\";\n\n",
            rust_name_base.to_uppercase(),
            name_value
        ));
    }

    // Write the generated file
    fs::write(&dest_path, rust_code)
        .map_err(|e| format!("Failed to write generated file: {}", e))?;

    Ok(())
}

fn extract_uuid_from_line(line: &str) -> Option<String> {
    // Extract UUID from: RecordCounter = uuid.MustParse("ccf2e3ba-633e-43ba-9394-46b0c54c61e3")
    let parts: Vec<&str> = line.split('"').collect();
    if parts.len() >= 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}

fn extract_name_from_line(line: &str) -> Option<(String, String)> {
    // Extract from: RecordCounter = "record_counter"
    let parts: Vec<&str> = line.split('=').collect();
    if parts.len() == 2 {
        let const_name = parts[0].trim().to_string();
        let name_parts: Vec<&str> = parts[1].split('"').collect();
        if name_parts.len() >= 2 {
            return Some((const_name, name_parts[1].to_string()));
        }
    }
    None
}

fn camel_to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

fn parse_uuid_to_bytes(uuid_str: &str) -> Result<[u8; 16], Box<dyn std::error::Error>> {
    // Parse UUID string like "ccf2e3ba-633e-43ba-9394-46b0c54c61e3" into bytes
    let hex_str = uuid_str.replace('-', "");
    if hex_str.len() != 32 {
        return Err(format!("Invalid UUID length: {}", uuid_str).into());
    }

    let mut bytes = [0u8; 16];
    for i in 0..16 {
        let byte_str = &hex_str[i * 2..i * 2 + 2];
        bytes[i] = u8::from_str_radix(byte_str, 16)
            .map_err(|e| format!("Failed to parse hex byte {}: {}", byte_str, e))?;
    }

    Ok(bytes)
}

fn format_uuid_bytes(bytes: &[u8; 16]) -> String {
    bytes
        .iter()
        .map(|b| format!("0x{:02x}", b))
        .collect::<Vec<_>>()
        .join(", ")
}
