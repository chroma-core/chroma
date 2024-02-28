use super::types::{HashMapBlockfile, KeyType, ValueType};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::types::Blockfile;

pub(crate) trait BlockfileProvider {
    fn new() -> Self;
    fn open(self, path: &str) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>>;
    fn create(
        &mut self,
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>>;
}

pub(super) struct HashMapBlockfileProvider {}

impl HashMapBlockfileProvider {
    fn new() -> Self {
        Self {}
    }

    fn open(self, path: &str) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>> {
        Ok(Box::new(HashMapBlockfile::new())) // TODO: This should not just create a new blockfile every time
    }

    fn create(
        &mut self,
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>> {
        Ok(Box::new(HashMapBlockfile::new())) // TODO: This should error if it already exists
    }
}
