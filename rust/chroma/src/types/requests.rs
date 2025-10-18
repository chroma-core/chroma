use bon::Builder;

#[derive(Builder)]
pub struct ListCollectionsRequest {
    #[builder(default = 100)]
    pub limit: usize,
    pub offset: Option<usize>,
    pub database_id: Option<String>,
}
