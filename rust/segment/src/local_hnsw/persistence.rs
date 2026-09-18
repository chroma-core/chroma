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
/// A newly initialized index may have no pickle only while its header is empty.
pub fn inspect_persisted_hnsw_index(path: &Path) -> io::Result<PersistedHnswIndex> {
    let metadata = match File::open(path.join(METADATA_FILE)) {
        Ok(file) => Some(
            serde_pickle::from_reader::<_, IdMap>(file, serde_pickle::DeOptions::new())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
        ),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => return Err(err),
    };
    validate_files(path, metadata.as_ref())
}

pub(super) fn validate_files(
    path: &Path,
    metadata: Option<&IdMap>,
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
    let _ef_construction = usize_from(&mut header)?;
    let vector_bytes = label_offset.checked_sub(data_offset).ok_or_else(invalid)?;
    let level0_bytes = max_m0
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(invalid)?;
    if offset_level0 != 0
        || capacity < elements
        || max_m == 0
        || m == 0
        || max_m0 < max_m
        || data_offset != level0_bytes
        || vector_bytes == 0
        || vector_bytes % size_of::<f32>() != 0
        || label_offset.checked_add(size_of::<usize>()) != Some(stride)
        || (elements > 0 && (entrypoint as usize >= elements || max_level < 0))
    {
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
    if metadata.is_none() && elements != 0 {
        return Err(invalid());
    }
    if let Some(map) = metadata {
        if map.dimensionality.is_some_and(|dim| dim != dimensionality)
            || map.id_to_label.len() != map.label_to_id.len()
            || map
                .id_to_label
                .iter()
                .any(|(id, label)| map.label_to_id.get(label) != Some(id))
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
    let mut active_count = 0;
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
        if let Some(map) = metadata {
            if label == 0 || label > map.total_elements_added {
                return Err(invalid());
            }
            if flags & 0x10000 == 0 {
                active_count += 1;
                if !map.label_to_id.contains_key(&label) {
                    return Err(invalid());
                }
            }
        }
        let link_bytes = u32_from(&mut links)? as usize;
        if link_bytes % upper_level_bytes != 0
            || link_bytes / upper_level_bytes > max_level.max(0) as usize
            || links
                .stream_position()?
                .checked_add(link_bytes as u64)
                .ok_or_else(invalid)?
                > links_length
        {
            return Err(invalid());
        }
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
    if metadata.is_some_and(|map| active_count != map.id_to_label.len()) {
        return Err(invalid());
    }
    Ok(PersistedHnswIndex {
        dimensionality,
        elements,
    })
}
