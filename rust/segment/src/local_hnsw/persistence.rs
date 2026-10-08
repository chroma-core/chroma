//! Validate persisted bytes before passing them to the native HNSW loader.
use std::{
    collections::HashSet,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    mem::size_of,
    path::Path,
};

use super::{IdMap, HNSW_HEADER_FILE, HNSW_PERSISTENCE_VERSION, METADATA_FILE};

/// Structural information shared by startup validation and the offline checker.
#[derive(Debug)]
pub struct PersistedHnswIndex {
    pub dimensionality: usize,
    pub elements: usize,
    /// The checkpoint needs log replay, independent of construction validation.
    pub recovery_required: bool,
    /// Highest allocated label, including labels from an interrupted save.
    pub max_label: u32,
    pub(super) unmapped_active_labels: Vec<u32>,
    pub(super) mapped_deleted_labels: Vec<u32>,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "inconsistent or truncated persisted HNSW index",
    )
}

fn usize_from(reader: &mut impl Read) -> io::Result<usize> {
    let mut bytes = [0; size_of::<usize>()];
    reader.read_exact(&mut bytes)?;
    Ok(usize::from_ne_bytes(bytes))
}

fn u32_from(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_ne_bytes(bytes))
}

/// Inspect the complete header, payload lengths, native labels and pickle maps.
/// Inspection never repairs files. A missing pickle is reported as requiring recovery.
pub fn inspect_persisted_hnsw_index(path: &Path) -> io::Result<PersistedHnswIndex> {
    inspect_files(path, ConstructionValidation::Strict)
}

/// Inspect a stopped index before repairing its persisted construction setting.
///
/// Allows an out-of-range `ef_construction`, but retains all other header,
/// payload, graph and ID-map checks. Does not modify files or load native code.
/// Success does not mean the index is safe to load: repair the setting and call
/// [`inspect_persisted_hnsw_index`] before loading or publishing the repaired copy.
pub fn inspect_persisted_hnsw_index_for_config_repair(
    path: &Path,
) -> io::Result<PersistedHnswIndex> {
    inspect_files(path, ConstructionValidation::AllowRepair)
}

#[derive(Clone, Copy)]
enum ConstructionValidation {
    Strict,
    AllowRepair,
}

fn inspect_files(
    path: &Path,
    construction: ConstructionValidation,
) -> io::Result<PersistedHnswIndex> {
    let metadata = match File::open(path.join(METADATA_FILE)) {
        Ok(file) => Some(
            serde_pickle::from_reader::<_, IdMap>(file, serde_pickle::DeOptions::new())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
        ),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => return Err(err),
    };
    validate_files_with_construction(path, metadata.as_ref(), construction)
}

pub(super) fn validate_files(
    path: &Path,
    metadata: Option<&IdMap>,
) -> io::Result<PersistedHnswIndex> {
    validate_files_with_construction(path, metadata, ConstructionValidation::Strict)
}

fn validate_files_with_construction(
    path: &Path,
    metadata: Option<&IdMap>,
    construction: ConstructionValidation,
) -> io::Result<PersistedHnswIndex> {
    let mut header = File::open(path.join(HNSW_HEADER_FILE))?;
    if u32_from(&mut header)? != HNSW_PERSISTENCE_VERSION as u32 {
        return Err(invalid());
    }
    let offset_level0 = usize_from(&mut header)?;
    let capacity = usize_from(&mut header)?;
    let elements = usize_from(&mut header)?;
    let stride = usize_from(&mut header)?;
    let label_offset = usize_from(&mut header)?;
    let data_offset = usize_from(&mut header)?;
    let max_level = u32_from(&mut header)? as i32;
    let entrypoint = u32_from(&mut header)?;
    let max_m = usize_from(&mut header)?;
    let max_m0 = usize_from(&mut header)?;
    let m = usize_from(&mut header)?;
    let mut mult = [0; 8];
    header.read_exact(&mut mult)?;
    let mult = f64::from_ne_bytes(mult);
    let ef_construction = usize_from(&mut header)?;
    let vector_bytes = label_offset.checked_sub(data_offset).ok_or_else(invalid)?;
    let level0_bytes = max_m0
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(invalid)?;
    if (matches!(construction, ConstructionValidation::Strict)
        && !(1..=4096).contains(&ef_construction))
        || offset_level0 != 0
        || capacity < elements
        || max_m == 0
        || !(2..=10_000).contains(&m)
        || m != max_m
        || max_m.checked_mul(2) != Some(max_m0)
        || !mult.is_finite()
        || (mult - 1.0 / (m as f64).ln()).abs() > 1e-12
        // Even the smallest positive f64 sample cannot produce a level above
        // 1074 with M >= 2. Leave headroom without accepting unbounded levels.
        || max_level > 2048
        || (elements == 0 && (max_level != -1 || entrypoint != u32::MAX))
        || max_m0 < max_m
        || data_offset != level0_bytes
        || vector_bytes == 0
        || vector_bytes % size_of::<f32>() != 0
        || label_offset.checked_add(size_of::<usize>()) != Some(stride)
        || (elements > 0 && (entrypoint as usize >= elements || max_level < 0))
    {
        return Err(invalid());
    }
    // Check every native allocation product, including capacity-sized buffers.
    for width in [
        stride,
        size_of::<f32>(),
        size_of::<usize>(),
        size_of::<i32>(),
    ] {
        if capacity
            .checked_mul(width)
            .is_none_or(|n| n > isize::MAX as usize)
        {
            return Err(invalid());
        }
    }
    if elements > u32::MAX as usize {
        return Err(invalid());
    }
    let dimensionality = vector_bytes / size_of::<f32>();
    let mut data = File::open(path.join("data_level0.bin"))?;
    let lengths = File::open(path.join("length.bin"))?;
    let mut links = File::open(path.join("link_lists.bin"))?;
    let data_bytes = elements.checked_mul(stride).ok_or_else(invalid)?;
    let length_bytes = elements.checked_mul(size_of::<f32>()).ok_or_else(invalid)?;
    if data.metadata()?.len() < data_bytes as u64 || lengths.metadata()?.len() < length_bytes as u64
    {
        return Err(invalid());
    }
    if let Some(map) = metadata {
        if map.dimensionality.is_some_and(|dim| dim != dimensionality)
            || map.id_to_label.len() != map.label_to_id.len()
            || map.id_to_label.iter().any(|(id, label)| {
                *label == 0
                    || *label > map.total_elements_added
                    || map.label_to_id.get(label) != Some(id)
            })
        {
            return Err(invalid());
        }
    }
    let upper_level_bytes = max_m
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(invalid)?;
    let links_length = links.metadata()?.len();
    let mut labels = HashSet::new();
    let mut recovery_required = metadata.is_none() && elements != 0;
    let mut max_label = 0;
    let mut unmapped_active_labels = Vec::new();
    let mut mapped_deleted_labels = Vec::new();
    let mut levels = Vec::new();
    let mut link_offsets = Vec::new();
    for node in 0..elements {
        data.seek(SeekFrom::Start((node * stride) as u64))?;
        let flags = u32_from(&mut data)?;
        let degree = flags & 0xffff;
        if degree as usize > max_m0 {
            return Err(invalid());
        }
        for _ in 0..degree {
            if u32_from(&mut data)? as usize >= elements {
                return Err(invalid());
            }
        }
        data.seek(SeekFrom::Start((node * stride + label_offset) as u64))?;
        let label = u32::try_from(usize_from(&mut data)?).map_err(|_| invalid())?;
        if !labels.insert(label) {
            return Err(invalid());
        }
        if label == 0 {
            return Err(invalid());
        }
        max_label = max_label.max(label);
        let mapped = metadata.is_some_and(|map| map.label_to_id.contains_key(&label));
        let deleted = flags & 0x10000 != 0;
        if mapped && deleted {
            mapped_deleted_labels.push(label);
        }
        if !mapped && !deleted {
            unmapped_active_labels.push(label);
        }
        recovery_required |=
            mapped == deleted || metadata.is_some_and(|map| label > map.total_elements_added);
        let link_bytes = u32_from(&mut links)? as usize;
        if !link_bytes.is_multiple_of(upper_level_bytes)
            || link_bytes / upper_level_bytes > max_level.max(0) as usize
            || links
                .stream_position()?
                .checked_add(link_bytes as u64)
                .ok_or_else(invalid)?
                > links_length
        {
            return Err(invalid());
        }
        levels.push(link_bytes / upper_level_bytes);
        link_offsets.push(links.stream_position()?);
        for _ in 0..link_bytes / upper_level_bytes {
            let start = links.stream_position()?;
            let degree = u32_from(&mut links)?;
            if degree as usize > max_m {
                return Err(invalid());
            }
            for _ in 0..degree {
                if u32_from(&mut links)? as usize >= elements {
                    return Err(invalid());
                }
            }
            links.seek(SeekFrom::Start(start + upper_level_bytes as u64))?;
        }
    }
    // A map referring to a missing native label is not an ahead-of-pickle save.
    if metadata.is_some_and(|map| map.label_to_id.keys().any(|label| !labels.contains(label))) {
        return Err(invalid());
    }
    if elements > 0 && levels[entrypoint as usize] != max_level as usize {
        return Err(invalid());
    }
    // Neighbor IDs alone are insufficient: upper-level traversal dereferences
    // the neighbor's allocation at the same level.
    for (node, &offset) in link_offsets.iter().enumerate() {
        for level in 1..=levels[node] {
            links.seek(SeekFrom::Start(
                offset + ((level - 1) * upper_level_bytes) as u64,
            ))?;
            let degree = u32_from(&mut links)?;
            for _ in 0..degree {
                let neighbor = u32_from(&mut links)? as usize;
                if levels[neighbor] < level {
                    return Err(invalid());
                }
            }
        }
    }
    Ok(PersistedHnswIndex {
        dimensionality,
        elements,
        recovery_required,
        max_label,
        unmapped_active_labels,
        mapped_deleted_labels,
    })
}
