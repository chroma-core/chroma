use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeListArray, FixedSizeListBuilder, Float32Array, Float32Builder,
        ListArray, ListBuilder, StructArray, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Fields},
};
use chroma_types::SpannPostingList;

use crate::{
    arrow::{
        block::delta::{
            spann_posting_list_delta::SpannPostingListDelta,
            spann_posting_list_size_tracker::SpannPostingListSizeTracker, BlockStorage,
            UnorderedBlockDelta,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};

type SpannPostingListDeltaEntry = (Vec<u32>, Vec<u32>, Vec<f32>);

pub struct SpannPostingListBuilderWrapper {
    doc_offset_ids_builder: ListBuilder<UInt32Builder>,
    doc_versions_builder: ListBuilder<UInt32Builder>,
    doc_embeddings_builder: ListBuilder<FixedSizeListBuilder<Float32Builder>>,
}

impl ArrowWriteableValue for &SpannPostingList<'_> {
    type ReadableValue<'referred_data> = SpannPostingList<'referred_data>;
    type PreparedValue = SpannPostingListDeltaEntry;
    type SizeTracker = SpannPostingListSizeTracker;
    type ArrowBuilder = SpannPostingListBuilderWrapper;
    type OwnedReadableValue = SpannPostingListDeltaEntry;

    // This method is only called for SingleColumnStorage.
    fn offset_size(_: usize) -> usize {
        unimplemented!()
    }

    // This method is only called for SingleColumnStorage.
    fn validity_size(_: usize) -> usize {
        unimplemented!()
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match &delta {
            BlockStorage::SpannPostingListDelta(builder) => {
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::SpannPostingListDelta(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder(_: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::SpannPostingListDelta(SpannPostingListDelta::new())
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        let num_rows = size_tracker.get_num_items();
        let num_offset_ids = size_tracker.get_doc_offset_ids_size() / std::mem::size_of::<u32>();
        let num_versions = size_tracker.get_doc_versions_size() / std::mem::size_of::<u32>();
        let num_embeddings = size_tracker.get_doc_embeddings_size() / std::mem::size_of::<f32>();
        SpannPostingListBuilderWrapper {
            doc_offset_ids_builder: ListBuilder::with_capacity(
                UInt32Builder::with_capacity(num_offset_ids),
                num_rows,
            ),
            doc_versions_builder: ListBuilder::with_capacity(
                UInt32Builder::with_capacity(num_versions),
                num_rows,
            ),
            doc_embeddings_builder: ListBuilder::with_capacity(
                FixedSizeListBuilder::with_capacity(
                    Float32Builder::with_capacity(num_embeddings),
                    size_tracker.get_embedding_dimension().unwrap_or(0) as i32,
                    num_offset_ids,
                ),
                num_rows,
            ),
        }
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        (
            value.doc_offset_ids.to_vec(),
            value.doc_versions.to_vec(),
            value.doc_embeddings.to_vec(),
        )
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        let doc_offset_ids = value.0;
        let doc_versions = value.1;
        let doc_embeddings = value.2;
        let embedding_dim = doc_embeddings.len() / doc_offset_ids.len();

        let inner_offset_id_ref = builder.doc_offset_ids_builder.values();
        let inner_version_ref = builder.doc_versions_builder.values();
        for (doc_offset_id, doc_version) in doc_offset_ids.into_iter().zip(doc_versions.into_iter())
        {
            inner_offset_id_ref.append_value(doc_offset_id);
            inner_version_ref.append_value(doc_version);
        }
        let inner_embeddings_ref = builder.doc_embeddings_builder.values();
        let mut f32_count = 0;
        for embedding in doc_embeddings.into_iter() {
            inner_embeddings_ref.values().append_value(embedding);
            f32_count += 1;
            if f32_count == embedding_dim {
                inner_embeddings_ref.append(true);
                f32_count = 0;
            }
        }
        builder.doc_offset_ids_builder.append(true);
        builder.doc_versions_builder.append(true);
        builder.doc_embeddings_builder.append(true);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>) {
        // Struct fields.
        let offset_field = Field::new(
            "offset_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        );
        let version_field = Field::new(
            "version",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        );
        let embeddings_field = Field::new(
            "embeddings",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    size_tracker.get_embedding_dimension().unwrap_or(0) as i32,
                ),
                true,
            ))),
            true,
        );
        // Construct struct array from these 3 child arrays.
        let offset_child_array = builder.doc_offset_ids_builder.finish();
        let version_child_array = builder.doc_versions_builder.finish();
        let embeddings_child_array = builder.doc_embeddings_builder.finish();
        let value_arr = StructArray::from(vec![
            (
                Arc::new(offset_field.clone()),
                Arc::new(offset_child_array) as ArrayRef,
            ),
            (
                Arc::new(version_field.clone()),
                Arc::new(version_child_array) as ArrayRef,
            ),
            (
                Arc::new(embeddings_field.clone()),
                Arc::new(embeddings_child_array) as ArrayRef,
            ),
        ]);
        let struct_fields = Fields::from(vec![offset_field, version_field, embeddings_field]);
        let value_field = Field::new("value", DataType::Struct(struct_fields), true);
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());

        (value_field, value_arr)
    }

    fn get_owned_value_from_delta(
        prefix: &str,
        key: KeyWrapper,
        delta: &BlockDelta,
    ) -> Option<Self::OwnedReadableValue> {
        match &delta.builder {
            BlockStorage::SpannPostingListDelta(builder) => builder.get_owned_value(prefix, key),
            _ => panic!("Invalid builder type"),
        }
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for SpannPostingList<'referred_data> {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self {
        let as_struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        let doc_offset_ids_arr = as_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let doc_id_start_idx = doc_offset_ids_arr.value_offsets()[index] as usize;
        let doc_id_end_idx = doc_offset_ids_arr.value_offsets()[index + 1] as usize;

        let doc_offset_slice_at_idx = &doc_offset_ids_arr
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()[doc_id_start_idx..doc_id_end_idx];

        let doc_versions_arr = as_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let doc_version_start_idx = doc_versions_arr.value_offsets()[index] as usize;
        let doc_version_end_idx = doc_versions_arr.value_offsets()[index + 1] as usize;
        let doc_versions_slice_at_idx = &doc_versions_arr
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()[doc_version_start_idx..doc_version_end_idx];

        let doc_embeddings_arr = as_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let top_level_start_idx = doc_embeddings_arr.value_offsets()[index] as usize;
        let top_level_end_idx = doc_embeddings_arr.value_offsets()[index + 1] as usize;
        let doc_embeddings_fixed_size_list = doc_embeddings_arr
            .values()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let doc_embeddings_start_idx =
            doc_embeddings_fixed_size_list.value_offset(top_level_start_idx) as usize;
        let doc_embeddings_end_idx =
            doc_embeddings_fixed_size_list.value_offset(top_level_end_idx) as usize;
        let doc_embeddings_slice_at_idx = &doc_embeddings_fixed_size_list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()[doc_embeddings_start_idx..doc_embeddings_end_idx];

        SpannPostingList {
            doc_offset_ids: doc_offset_slice_at_idx,
            doc_versions: doc_versions_slice_at_idx,
            doc_embeddings: doc_embeddings_slice_at_idx,
        }
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    ) {
        <&SpannPostingList>::add(prefix, key.into(), &value, storage);
    }
}
