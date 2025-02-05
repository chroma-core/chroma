use sea_query::Iden;

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
pub enum EmbeddingMetadata {
    Table,
    Id,
    Key,
    StringValue,
    IntValue,
    FloatValue,
    BoolValue,
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
