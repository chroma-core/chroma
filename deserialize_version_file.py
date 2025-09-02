import chromadb.proto.coordinator_pb2 as coordinator_pb2

# Read the serialized file
with open('/Users/sanketkedia/Downloads/000038_5415a1ef-4efc-4503-8a24-ef144a9d1911_flush', 'rb') as f:
    serialized_data = f.read()

# Deserialize the data
collection_version_file = coordinator_pb2.CollectionVersionFile()
collection_version_file.ParseFromString(serialized_data)

if not collection_version_file.HasField('version_history'):
    print("No version history found in the file.")
    exit(0)

versions = collection_version_file.version_history.versions
print(f"Found {len(versions)} versions in the file.")
print(collection_version_file)

exit(0)

output_file_path = "/Users/sanketkedia/Documents/000310_166f7650-8cd8-452d-904b-6f92ca5fd62c_flush"

 # Track the last non-empty segment_info we've seen
last_non_empty_segment_info = None
last_non_empty_version = None

def has_non_empty_segment_info(version_info):
    """Check if a version has non-empty segment_info."""
    if not version_info.HasField('segment_info'):
        return False
    
    segment_info = version_info.segment_info
    return len(segment_info.segment_compaction_info) > 0

# Process each version
for i, version in enumerate(versions):
    version_num = version.version
    
    if has_non_empty_segment_info(version):
        # This version has non-empty segment_info, update our tracker
        last_non_empty_segment_info = version.segment_info
        last_non_empty_version = version_num
        print(f"Version {version_num}: Has non-empty segment_info with {len(version.segment_info.segment_compaction_info)} segments")
    else:
        # This version has empty segment_info
        if last_non_empty_segment_info is not None:
            # We have a previous non-empty segment_info to use
            print(f"Version {version_num}: Patching with segment_info from version {last_non_empty_version}")
            
            # Clear any existing segment_info and copy from the last non-empty one
            version.segment_info.CopyFrom(last_non_empty_segment_info)
        else:
            print(f"Version {version_num}: No previous non-empty segment_info found, keeping empty")

print(f"\nWriting patched file to: {output_file_path}")
serialized_output = collection_version_file.SerializeToString()

with open(output_file_path, 'wb') as f:
    f.write(serialized_output)

print("Patching complete!")