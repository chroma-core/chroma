use sea_query::Iden;

pub trait MetadataTable {
    fn table_name() -> Self;
    fn id_column() -> Self;
    fn key_column() -> Self;
    fn str_value_column() -> Self;
    fn int_value_column() -> Self;
    fn float_value_column() -> Self;
    fn bool_value_column() -> Self;
}

/// The table definitions here should match the table schema in sqlite
#[derive(Iden)]
pub enum Embeddings {
    Table,
    Id,
    SegmentId,
    EmbeddingId,
    SeqId,
    CreatedAt,
}

#[derive(Iden)]
pub enum EmbeddingFulltextSearch {
    Table,
    Rowid,
    StringValue,
}

#[derive(Iden)]
pub enum MaxSeqId {
    Table,
    SegmentId,
    SeqId,
}

#[derive(Iden)]
pub enum Collections {
    Table,
    Id,
    Name,
    Dimension,
    DatabaseId,
    ConfigJsonStr,
    SchemaStr,
}

#[derive(Iden)]
pub enum Segments {
    Table,
    Id,
    Type,
    Scope,
    Collection,
}

#[derive(Iden)]
pub enum Databases {
    Table,
    Id,
    Name,
    TenantId,
}

#[derive(Iden)]
pub enum CollectionMetadata {
    Table,
    CollectionId,
    Key,
    StrValue,
    IntValue,
    FloatValue,
    BoolValue,
}

impl MetadataTable for CollectionMetadata {
    fn table_name() -> Self {
        CollectionMetadata::Table
    }

    fn id_column() -> Self {
        CollectionMetadata::CollectionId
    }

    fn key_column() -> Self {
        CollectionMetadata::Key
    }

    fn str_value_column() -> Self {
        CollectionMetadata::StrValue
    }

    fn int_value_column() -> Self {
        CollectionMetadata::IntValue
    }

    fn float_value_column() -> Self {
        CollectionMetadata::FloatValue
    }

    fn bool_value_column() -> Self {
        CollectionMetadata::BoolValue
    }
}

#[derive(Iden)]
pub enum EmbeddingMetadata {
    Table,
    Id,
    Key,
    StringValue,
    IntValue,
    FloatValue,
    BoolValue,
}

impl MetadataTable for EmbeddingMetadata {
    fn table_name() -> Self {
        EmbeddingMetadata::Table
    }

    fn id_column() -> Self {
        EmbeddingMetadata::Id
    }

    fn key_column() -> Self {
        EmbeddingMetadata::Key
    }

    fn str_value_column() -> Self {
        EmbeddingMetadata::StringValue
    }

    fn int_value_column() -> Self {
        EmbeddingMetadata::IntValue
    }

    fn float_value_column() -> Self {
        EmbeddingMetadata::FloatValue
    }

    fn bool_value_column() -> Self {
        EmbeddingMetadata::BoolValue
    }
}

#[derive(Iden)]
pub enum SegmentMetadata {
    Table,
    SegmentId,
    Key,
    StrValue,
    IntValue,
    FloatValue,
    BoolValue,
}

impl MetadataTable for SegmentMetadata {
    fn table_name() -> Self {
        SegmentMetadata::Table
    }

    fn id_column() -> Self {
        SegmentMetadata::SegmentId
    }

    fn key_column() -> Self {
        SegmentMetadata::Key
    }

    fn str_value_column() -> Self {
        SegmentMetadata::StrValue
    }

    fn int_value_column() -> Self {
        SegmentMetadata::IntValue
    }

    fn float_value_column() -> Self {
        SegmentMetadata::FloatValue
    }

    fn bool_value_column() -> Self {
        SegmentMetadata::BoolValue
    }
}
