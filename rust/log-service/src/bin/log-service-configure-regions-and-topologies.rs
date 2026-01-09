//! Interactive CLI tool for configuring regions and topologies.
//!
//! This tool guides the user through creating a `MultiCloudMultiRegionConfiguration`
//! by interactively querying for regions and topologies.

use std::io::BufRead;
use std::io::Write;

use chroma_types::MultiCloudMultiRegionConfiguration;
use chroma_types::ProviderRegion;
use chroma_types::RegionName;
use chroma_types::Topology;
use chroma_types::TopologyName;

/// Reads a line from stdin, trimming whitespace.
fn read_line(reader: &mut impl BufRead) -> std::io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line.trim().to_string())
}

/// Prompts the user and reads a response.
fn prompt(writer: &mut impl Write, reader: &mut impl BufRead, message: &str) -> String {
    write!(writer, "{}", message).expect("failed to write prompt");
    writer.flush().expect("failed to flush");
    read_line(reader).expect("failed to read line")
}

/// Prompts the user to enter region data.
fn read_region(writer: &mut impl Write, reader: &mut impl BufRead) -> Option<ProviderRegion<()>> {
    writeln!(writer).expect("failed to write newline");
    writeln!(writer, "=== Add a new region ===").expect("failed to write");
    writeln!(
        writer,
        "Enter the region name (e.g., 'aws-us-east-1'), or empty to finish adding regions:"
    )
    .expect("failed to write");

    let name = prompt(writer, reader, "Region name: ");
    if name.is_empty() {
        return None;
    }

    let region_name = match RegionName::new(&name) {
        Ok(rn) => rn,
        Err(e) => {
            writeln!(writer, "Invalid region name: {}", e).expect("failed to write");
            return read_region(writer, reader);
        }
    };

    let provider = prompt(writer, reader, "Provider (e.g., 'aws', 'gcp'): ");
    if provider.is_empty() {
        writeln!(writer, "Provider cannot be empty.").expect("failed to write");
        return read_region(writer, reader);
    }

    let region = prompt(writer, reader, "Region identifier (e.g., 'us-east-1'): ");
    if region.is_empty() {
        writeln!(writer, "Region identifier cannot be empty.").expect("failed to write");
        return read_region(writer, reader);
    }

    Some(ProviderRegion::new(region_name, provider, region, ()))
}

/// Collects all regions from the user.
fn collect_regions(writer: &mut impl Write, reader: &mut impl BufRead) -> Vec<ProviderRegion<()>> {
    let mut regions = Vec::new();

    writeln!(writer, "\n========================================").expect("failed to write");
    writeln!(writer, "Step 1: Define Regions").expect("failed to write");
    writeln!(writer, "========================================").expect("failed to write");
    writeln!(
        writer,
        "Enter regions one at a time. Press Enter with an empty name to finish."
    )
    .expect("failed to write");

    loop {
        match read_region(writer, reader) {
            Some(region) => {
                writeln!(
                    writer,
                    "Added region: {} (provider: {}, region: {})",
                    region.name(),
                    region.provider(),
                    region.region()
                )
                .expect("failed to write");
                regions.push(region);
            }
            None => {
                if regions.is_empty() {
                    writeln!(writer, "At least one region is required.").expect("failed to write");
                    continue;
                }
                break;
            }
        }
    }

    writeln!(writer, "\nDefined {} region(s).", regions.len()).expect("failed to write");
    regions
}

/// Displays the available regions for selection.
fn display_regions(writer: &mut impl Write, regions: &[ProviderRegion<()>]) {
    writeln!(writer, "\nAvailable regions:").expect("failed to write");
    for (i, region) in regions.iter().enumerate() {
        writeln!(
            writer,
            "  [{}] {} (provider: {}, region: {})",
            i + 1,
            region.name(),
            region.provider(),
            region.region()
        )
        .expect("failed to write");
    }
}

/// Prompts user to select regions for a topology.
fn select_regions_for_topology(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    regions: &[ProviderRegion<()>],
) -> Vec<RegionName> {
    let mut selected = Vec::new();

    writeln!(
        writer,
        "\nSelect regions for this topology (enter numbers one at a time, empty to finish):"
    )
    .expect("failed to write");

    loop {
        display_regions(writer, regions);
        let input = prompt(
            writer,
            reader,
            "Select region number (or empty to finish): ",
        );

        if input.is_empty() {
            break;
        }

        match input.parse::<usize>() {
            Ok(n) if n >= 1 && n <= regions.len() => {
                let region_name = regions[n - 1].name().clone();
                if selected.contains(&region_name) {
                    writeln!(writer, "Region already selected.").expect("failed to write");
                } else {
                    writeln!(writer, "Selected: {}", region_name).expect("failed to write");
                    selected.push(region_name);
                }
            }
            _ => {
                writeln!(
                    writer,
                    "Invalid selection. Please enter a number between 1 and {}.",
                    regions.len()
                )
                .expect("failed to write");
            }
        }
    }

    selected
}

/// Reads a single topology from the user.
fn read_topology(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    regions: &[ProviderRegion<()>],
) -> Option<Topology<()>> {
    writeln!(writer).expect("failed to write newline");
    writeln!(writer, "=== Add a new topology ===").expect("failed to write");
    writeln!(
        writer,
        "Enter the topology name (e.g., 'global'), or empty to finish adding topologies:"
    )
    .expect("failed to write");

    let name = prompt(writer, reader, "Topology name: ");
    if name.is_empty() {
        return None;
    }

    let topology_name = match TopologyName::new(&name) {
        Ok(tn) => tn,
        Err(e) => {
            writeln!(writer, "Invalid topology name: {}", e).expect("failed to write");
            return read_topology(writer, reader, regions);
        }
    };

    let selected_regions = select_regions_for_topology(writer, reader, regions);

    Some(Topology::new(topology_name, selected_regions, ()))
}

/// Collects all topologies from the user.
fn collect_topologies(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    regions: &[ProviderRegion<()>],
) -> Vec<Topology<()>> {
    let mut topologies = Vec::new();

    writeln!(writer, "\n========================================").expect("failed to write");
    writeln!(writer, "Step 2: Define Topologies").expect("failed to write");
    writeln!(writer, "========================================").expect("failed to write");
    writeln!(
        writer,
        "Enter topologies one at a time. Press Enter with an empty name to finish."
    )
    .expect("failed to write");

    while let Some(topology) = read_topology(writer, reader, regions) {
        writeln!(
            writer,
            "Added topology: {} with {} region(s)",
            topology.name(),
            topology.regions().len()
        )
        .expect("failed to write");
        topologies.push(topology);
    }

    writeln!(writer, "\nDefined {} topology(ies).", topologies.len()).expect("failed to write");
    topologies
}

/// Prompts user to select the preferred region.
fn select_preferred_region(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    regions: &[ProviderRegion<()>],
) -> RegionName {
    writeln!(writer, "\n========================================").expect("failed to write");
    writeln!(writer, "Step 3: Select Preferred Region").expect("failed to write");
    writeln!(writer, "========================================").expect("failed to write");

    loop {
        display_regions(writer, regions);
        let input = prompt(writer, reader, "Select the preferred region number: ");

        match input.parse::<usize>() {
            Ok(n) if n >= 1 && n <= regions.len() => {
                let selected = regions[n - 1].name().clone();
                writeln!(writer, "Preferred region: {}", selected).expect("failed to write");
                return selected;
            }
            _ => {
                writeln!(
                    writer,
                    "Invalid selection. Please enter a number between 1 and {}.",
                    regions.len()
                )
                .expect("failed to write");
            }
        }
    }
}

/// Runs the interactive configuration process.
fn run_interactive(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
) -> Result<MultiCloudMultiRegionConfiguration<(), ()>, String> {
    writeln!(
        writer,
        "\n========================================================"
    )
    .expect("failed to write");
    writeln!(writer, "  Multi-Cloud Multi-Region Configuration Tool").expect("failed to write");
    writeln!(
        writer,
        "========================================================"
    )
    .expect("failed to write");

    let regions = collect_regions(writer, reader);
    let topologies = collect_topologies(writer, reader, &regions);
    let preferred = select_preferred_region(writer, reader, &regions);

    MultiCloudMultiRegionConfiguration::new(preferred, regions, topologies)
        .map_err(|e| format!("Configuration validation failed: {}", e))
}

fn main() {
    let stdout = std::io::stdout();
    let stdin = std::io::stdin();
    let mut writer = stdout.lock();
    let mut reader = stdin.lock();

    match run_interactive(&mut writer, &mut reader) {
        Ok(config) => {
            writeln!(writer, "\n========================================")
                .expect("failed to write");
            writeln!(writer, "Configuration Complete!").expect("failed to write");
            writeln!(writer, "========================================").expect("failed to write");
            writeln!(writer, "\nJSON output:").expect("failed to write");
            let yaml = serde_yml::to_string(&config).expect("failed to serialize");
            writeln!(writer, "{}", yaml).expect("failed to write");
        }
        Err(e) => {
            writeln!(writer, "\nError: {}", e).expect("failed to write");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a mock reader from a string of inputs (one per line).
    fn mock_input(inputs: &[&str]) -> std::io::Cursor<Vec<u8>> {
        let data = inputs.join("\n") + "\n";
        std::io::Cursor::new(data.into_bytes())
    }

    #[test]
    fn single_region_no_topologies() {
        let inputs = vec![
            "aws-us-east-1", // Region name
            "aws",           // Provider
            "us-east-1",     // Region
            "",              // End regions
            "",              // End topologies
            "1",             // Preferred region
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        let output = String::from_utf8(writer).expect("output should be valid utf8");
        println!("single_region_no_topologies output:\n{}", output);

        assert!(result.is_ok(), "Expected success, got: {:?}", result);
        let config = result.unwrap();
        assert_eq!(config.regions().len(), 1);
        assert_eq!(config.topologies().len(), 0);
        assert_eq!(config.preferred().as_str(), "aws-us-east-1");
    }

    #[test]
    fn multiple_regions_with_topology() {
        let inputs = vec![
            "aws-us-east-1",    // First region name
            "aws",              // Provider
            "us-east-1",        // Region
            "gcp-europe-west1", // Second region name
            "gcp",              // Provider
            "europe-west1",     // Region
            "",                 // End regions
            "global",           // Topology name
            "1",                // Select first region
            "2",                // Select second region
            "",                 // End selection
            "",                 // End topologies
            "1",                // Preferred region
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        let output = String::from_utf8(writer).expect("output should be valid utf8");
        println!("multiple_regions_with_topology output:\n{}", output);

        assert!(result.is_ok(), "Expected success, got: {:?}", result);
        let config = result.unwrap();
        assert_eq!(config.regions().len(), 2);
        assert_eq!(config.topologies().len(), 1);
        assert_eq!(config.topologies()[0].name().as_str(), "global");
        assert_eq!(config.topologies()[0].regions().len(), 2);
    }

    #[test]
    fn invalid_region_name_retry() {
        let inputs = vec![
            "",              // Empty name (triggers retry, but this expects at least one region)
            "aws-us-east-1", // Valid region name after retry
            "aws",           // Provider
            "us-east-1",     // Region
            "",              // End regions
            "",              // End topologies
            "1",             // Preferred region
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        let output = String::from_utf8(writer).expect("output should be valid utf8");
        println!("invalid_region_name_retry output:\n{}", output);

        assert!(
            result.is_ok(),
            "Expected success after retry, got: {:?}",
            result
        );
    }

    #[test]
    fn multiple_topologies() {
        let inputs = vec![
            "aws-us-east-1", // First region
            "aws",
            "us-east-1",
            "aws-us-west-2", // Second region
            "aws",
            "us-west-2",
            "",          // End regions
            "east-only", // First topology
            "1",         // Select first region only
            "",          // End selection
            "west-only", // Second topology
            "2",         // Select second region only
            "",          // End selection
            "both",      // Third topology
            "1",         // Select first
            "2",         // Select second
            "",          // End selection
            "",          // End topologies
            "1",         // Preferred region
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        let output = String::from_utf8(writer).expect("output should be valid utf8");
        println!("multiple_topologies output:\n{}", output);

        assert!(result.is_ok(), "Expected success, got: {:?}", result);
        let config = result.unwrap();
        assert_eq!(config.regions().len(), 2);
        assert_eq!(config.topologies().len(), 3);
    }

    #[test]
    fn duplicate_region_selection_ignored() {
        let inputs = vec![
            "aws-us-east-1",
            "aws",
            "us-east-1",
            "",     // End regions
            "test", // Topology name
            "1",    // Select region
            "1",    // Try to select same region again
            "",     // End selection
            "",     // End topologies
            "1",    // Preferred
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        let output = String::from_utf8(writer).expect("output should be valid utf8");
        println!("duplicate_region_selection_ignored output:\n{}", output);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.topologies()[0].regions().len(), 1);
    }

    #[test]
    fn json_output_valid() {
        let inputs = vec![
            "aws-us-east-1",
            "aws",
            "us-east-1",
            "",
            "global",
            "1",
            "",
            "",
            "1",
        ];
        let mut reader = mock_input(&inputs);
        let mut writer = Vec::new();

        let result = run_interactive(&mut writer, &mut reader);
        assert!(result.is_ok());

        let config = result.unwrap();
        let json = serde_json::to_string_pretty(&config);
        assert!(json.is_ok(), "JSON serialization should succeed");
        println!("json_output_valid JSON:\n{}", json.unwrap());
    }
}
