//! In-memory provisioning operations used by service lifecycle tests.

use super::TestSysDb;
use crate::{AttachFunctionError, GetAttachedFunctionError};
use chroma_types::{
    AttachedFunction, AttachedFunctionUuid, Collection, CollectionUuid, CreateCollectionError,
    DatabaseUuid, FinishCreateAttachedFunctionError, Schema, Segment,
};

impl TestSysDb {
    /// Interrupts one finish call before it changes storage. This models a
    /// process stopping after attachment creation and before completion.
    pub fn fail_finish_attached_function_on_call(&mut self, call: usize) {
        let mut inner = self.inner.lock();
        inner.finish_attached_function_calls = 0;
        inner.fail_finish_attached_function_on_call = Some(call);
    }

    pub fn finish_attached_function_calls(&self) -> usize {
        self.inner.lock().finish_attached_function_calls
    }

    pub(crate) fn create_collection_record(
        &mut self,
        mut collection: Collection,
        segments: Vec<Segment>,
        get_or_create: bool,
    ) -> Result<Collection, CreateCollectionError> {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.collections.values().find(|existing| {
            !inner
                .soft_deleted_collections
                .contains(&existing.collection_id)
                && existing.tenant == collection.tenant
                && existing.database == collection.database
                && existing.name == collection.name
        }) {
            return if get_or_create {
                Ok(existing.clone())
            } else {
                Err(CreateCollectionError::AlreadyExists(collection.name))
            };
        }
        if let Some(database) = inner
            .databases
            .get(&(collection.tenant.clone(), collection.database.clone()))
        {
            collection.database_id = DatabaseUuid(database.id);
        }
        inner
            .collections
            .insert(collection.collection_id, collection.clone());
        for segment in segments {
            inner.segments.insert(segment.id, segment);
        }
        Ok(collection)
    }

    pub(crate) fn create_attached_function_record(
        &mut self,
        function: AttachedFunction,
    ) -> Result<(AttachedFunctionUuid, bool), AttachFunctionError> {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.tasks.values().find(|existing| {
            existing.input_collection_id == function.input_collection_id
                && existing.name == function.name
                && !existing.is_deleted
        }) {
            if existing.tenant_id != function.tenant_id
                || existing.database_id != function.database_id
                || existing.output_collection_name != function.output_collection_name
                || existing.params != function.params
            {
                return Err(AttachFunctionError::AlreadyExists(function.name));
            }
            return Ok((existing.id, false));
        }
        let id = function.id;
        inner
            .tasks
            .insert((id, function.input_collection_id), function);
        Ok((id, true))
    }

    pub(crate) fn finish_create_attached_function(
        &mut self,
        id: AttachedFunctionUuid,
        output_schema: String,
    ) -> Result<bool, FinishCreateAttachedFunctionError> {
        let _: Schema = serde_json::from_str(&output_schema)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let mut inner = self.inner.lock();
        inner.finish_attached_function_calls += 1;
        if inner.fail_finish_attached_function_on_call == Some(inner.finish_attached_function_calls)
        {
            inner.fail_finish_attached_function_on_call = None;
            return Err(
                tonic::Status::internal("injected interruption before attachment finish").into(),
            );
        }

        let function = inner
            .tasks
            .values()
            .find(|function| function.id == id && !function.is_deleted)
            .cloned()
            .ok_or(FinishCreateAttachedFunctionError::AttachedFunctionNotFound)?;
        // Foundation provisions output collections before attaching functions.
        // Refuse missing outputs rather than claiming a ready attachment.
        let output = inner
            .collections
            .values()
            .find(|collection| {
                collection.tenant == function.tenant_id
                    && collection.database == function.database_id
                    && collection.name == function.output_collection_name
            })
            .map(|collection| collection.collection_id)
            .ok_or_else(|| {
                tonic::Status::not_found("attached function output collection does not exist")
            })?;
        let mut changed = false;
        for function in inner
            .tasks
            .values_mut()
            .filter(|function| function.id == id)
        {
            changed |= function.output_collection_id.is_none();
            function.output_collection_id = Some(output);
        }
        Ok(changed)
    }

    pub(crate) fn add_attached_function_input(
        &mut self,
        id: AttachedFunctionUuid,
        input: CollectionUuid,
    ) -> Result<(AttachedFunctionUuid, bool), AttachFunctionError> {
        let mut inner = self.inner.lock();
        if inner.tasks.contains_key(&(id, input)) {
            return Ok((id, false));
        }
        let mut function = inner
            .tasks
            .values()
            .find(|function| function.id == id && !function.is_deleted)
            .cloned()
            .ok_or_else(|| AttachFunctionError::FunctionNotFound(id.to_string()))?;
        let collection = inner.collections.get(&input).ok_or_else(|| {
            AttachFunctionError::InvalidArgument("input collection does not exist".into())
        })?;
        if collection.tenant != function.tenant_id || collection.database != function.database_id {
            return Err(AttachFunctionError::InvalidArgument(
                "input collection belongs to another database".into(),
            ));
        }
        function.input_collection_id = input;
        function.output_collection_id = None;
        inner.tasks.insert((id, input), function);
        Ok((id, true))
    }

    pub(crate) fn get_attached_functions(
        &mut self,
        name: Option<String>,
        input: Option<CollectionUuid>,
        ids: Vec<AttachedFunctionUuid>,
        only_ready: bool,
    ) -> Result<Vec<AttachedFunction>, GetAttachedFunctionError> {
        Ok(self
            .inner
            .lock()
            .tasks
            .values()
            .filter(|function| {
                !function.is_deleted
                    && name.as_ref().is_none_or(|name| name == &function.name)
                    && input.is_none_or(|input| input == function.input_collection_id)
                    && (ids.is_empty() || ids.contains(&function.id))
                    && (!only_ready || function.output_collection_id.is_some())
            })
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_preserves_existing_schema_configuration_and_identity() {
        let mut sysdb = TestSysDb::new();
        let first = Collection {
            collection_id: CollectionUuid::new(),
            tenant: "team".into(),
            database: "foundation".into(),
            name: "wiki".into(),
            schema: Some(Schema::new_record_only()),
            dimension: Some(1),
            ..Default::default()
        };
        sysdb
            .create_collection_record(first.clone(), vec![], false)
            .unwrap();
        let attempted = Collection {
            collection_id: CollectionUuid::new(),
            schema: None,
            dimension: Some(1024),
            ..first.clone()
        };
        // The production Go catalog and SQLite backend return an existing
        // collection unchanged for get-or-create, without comparing config.
        let returned = sysdb
            .create_collection_record(attempted.clone(), vec![], true)
            .unwrap();
        assert_eq!(
            serde_json::to_value(returned).unwrap(),
            serde_json::to_value(first).unwrap()
        );
        assert!(matches!(
            sysdb.create_collection_record(attempted, vec![], false),
            Err(CreateCollectionError::AlreadyExists(_))
        ));
    }

    #[test]
    fn collection_identity_is_scoped_to_tenant_and_database() {
        let mut sysdb = TestSysDb::new();
        let mut ids = std::collections::HashSet::new();
        for (tenant, database) in [
            ("alice", "foundation"),
            ("bob", "foundation"),
            ("alice", "another"),
        ] {
            let collection = Collection {
                collection_id: CollectionUuid::new(),
                tenant: tenant.into(),
                database: database.into(),
                name: "wiki".into(),
                ..Default::default()
            };
            let stored = sysdb
                .create_collection_record(collection, vec![], true)
                .unwrap();
            assert!(ids.insert(stored.collection_id));
        }
    }
}
