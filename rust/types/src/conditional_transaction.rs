use std::collections::BTreeSet;

use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::{GetRequest, GetResponse, OccReadMode, OccReadToken, Operation};

/// One buffered write operation in transaction call order.
///
/// The per-operation `ids` preserve the ids and order that belong to this
/// specific write call. `ConditionalTransactionState::buffered_write_ids` is a
/// separate membership index used for fast validation such as read-after-write
/// and duplicate-write checks; it does not replace this ordered payload state.
#[derive(Clone, Debug, PartialEq)]
pub struct ConditionalBufferedWrite {
    operation: Operation,
    ids: Vec<String>,
}

impl ConditionalBufferedWrite {
    pub fn new(operation: Operation, ids: Vec<String>) -> Self {
        Self { operation, ids }
    }

    pub fn operation(&self) -> Operation {
        self.operation
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConditionalTransactionState {
    /// Ids whose values have contributed to the transaction's read snapshot.
    ///
    /// Point reads add their requested ids, while filter reads add only ids
    /// returned by the query. This set becomes the conditional read set checked
    /// against writes appended after the captured read token.
    read_ids: BTreeSet<String>,
    /// First OCC read token observed by the transaction.
    ///
    /// Later reads must reuse this token so every read observes one stable log
    /// snapshot. Its log upper-bound offset is the observed log position for
    /// conditional write validation.
    read_token: Option<OccReadToken>,
    /// Ids known to exist in the captured read snapshot.
    ///
    /// Returned ids are marked present for both point and filter reads. A later
    /// unfiltered point read can remove an id from this set by proving absence.
    known_present: BTreeSet<String>,
    /// Ids known not to exist in the captured read snapshot.
    ///
    /// Only unfiltered point reads can prove absence, because filtered reads can
    /// omit ids for reasons other than nonexistence.
    known_absent: BTreeSet<String>,
    /// Buffered write calls waiting to be emitted at commit time.
    ///
    /// The vector preserves transaction call order, and each entry preserves
    /// the id order from the write call that produced it.
    buffered_writes: Vec<ConditionalBufferedWrite>,
    /// Membership index for ids affected by buffered writes.
    ///
    /// This mirrors the union of ids in `buffered_writes` so validation can
    /// cheaply reject read-after-write and duplicate buffered-write ids.
    buffered_write_ids: BTreeSet<String>,
    /// Whether this state has been closed against further transactional work.
    ///
    /// Once set, new reads are rejected and the state is not reopened.
    closed: bool,
}

impl ConditionalTransactionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_ids(&self) -> &BTreeSet<String> {
        &self.read_ids
    }

    pub fn read_token(&self) -> Option<OccReadToken> {
        self.read_token
    }

    pub fn known_present(&self) -> &BTreeSet<String> {
        &self.known_present
    }

    pub fn known_absent(&self) -> &BTreeSet<String> {
        &self.known_absent
    }

    pub fn buffered_writes(&self) -> &[ConditionalBufferedWrite] {
        &self.buffered_writes
    }

    pub fn buffered_write_ids(&self) -> &BTreeSet<String> {
        &self.buffered_write_ids
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn prepare_get_request(
        &self,
        request: GetRequest,
    ) -> Result<GetRequest, ConditionalTransactionError> {
        self.validate_get_request(&request)?;
        Ok(match self.read_token {
            Some(read_token) => request.with_occ_read_token(read_token),
            None => request.with_occ_read_token_generation(),
        })
    }

    pub fn finish_get(
        &mut self,
        request: &GetRequest,
        response: GetResponse,
    ) -> Result<GetResponse, ConditionalTransactionError> {
        self.record_get_response(request, &response)?;
        Ok(response)
    }

    pub fn record_get_response(
        &mut self,
        request: &GetRequest,
        response: &GetResponse,
    ) -> Result<(), ConditionalTransactionError> {
        self.validate_get_request(request)?;
        let read_token = match request.occ_read_mode() {
            OccReadMode::Capture => response
                .occ_read_token()
                .ok_or(ConditionalTransactionError::MissingReadToken)?,
            OccReadMode::AtToken(read_token) => read_token,
            OccReadMode::None => return Err(ConditionalTransactionError::MissingReadToken),
        };
        observed_log_offset(read_token)?;
        if let Some(existing_read_token) = self.read_token {
            if existing_read_token != read_token {
                return Err(ConditionalTransactionError::ReadTokenMismatch {
                    expected_log_upper_bound_offset: existing_read_token.log_upper_bound_offset(),
                    actual_log_upper_bound_offset: read_token.log_upper_bound_offset(),
                });
            }
        }

        let returned_ids: BTreeSet<&str> = response.ids.iter().map(String::as_str).collect();
        if let Some(id) = response
            .ids
            .iter()
            .find(|id| self.buffered_write_ids.contains(id.as_str()))
        {
            return Err(ConditionalTransactionError::ReadAfterBufferedWrite { id: id.clone() });
        }

        let mut next_read_ids = self.read_ids.clone();
        let mut next_known_present = self.known_present.clone();
        let mut next_known_absent = self.known_absent.clone();

        if let Some(ids) = &request.ids {
            for id in ids {
                next_read_ids.insert(id.clone());
            }
            for id in &response.ids {
                next_read_ids.insert(id.clone());
                next_known_present.insert(id.clone());
                next_known_absent.remove(id);
            }
            if request.r#where.is_none() {
                for id in ids {
                    if !returned_ids.contains(id.as_str()) {
                        next_known_absent.insert(id.clone());
                        next_known_present.remove(id);
                    }
                }
            }
        } else {
            for id in &response.ids {
                next_read_ids.insert(id.clone());
                next_known_present.insert(id.clone());
                next_known_absent.remove(id);
            }
        }

        self.read_ids = next_read_ids;
        self.known_present = next_known_present;
        self.known_absent = next_known_absent;
        self.read_token.get_or_insert(read_token);

        Ok(())
    }

    fn validate_get_request(
        &self,
        request: &GetRequest,
    ) -> Result<(), ConditionalTransactionError> {
        if self.closed {
            return Err(ConditionalTransactionError::Closed);
        }

        match &request.ids {
            Some(ids) => {
                if let Some(id) = ids
                    .iter()
                    .find(|id| self.buffered_write_ids.contains(id.as_str()))
                {
                    return Err(ConditionalTransactionError::ReadAfterBufferedWrite {
                        id: id.clone(),
                    });
                }
            }
            None if !matches!(request.limit, Some(limit) if limit > 0) => {
                return Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit);
            }
            None => {}
        }

        Ok(())
    }
}

fn observed_log_offset(read_token: OccReadToken) -> Result<i64, ConditionalTransactionError> {
    i64::try_from(read_token.log_upper_bound_offset()).map_err(|_| {
        ConditionalTransactionError::ReadTokenOutOfRange {
            log_upper_bound_offset: read_token.log_upper_bound_offset(),
        }
    })
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConditionalTransactionError {
    #[error("conditional transaction is closed")]
    Closed,
    #[error("transactional filter reads require a positive limit")]
    FilterReadRequiresPositiveLimit,
    #[error("cannot transactionally read id {id:?} after buffering a write for it")]
    ReadAfterBufferedWrite { id: String },
    #[error("transactional get response did not include an OCC read token")]
    MissingReadToken,
    #[error(
        "transactional read token changed from log upper bound offset {expected_log_upper_bound_offset} to {actual_log_upper_bound_offset}"
    )]
    ReadTokenMismatch {
        expected_log_upper_bound_offset: u64,
        actual_log_upper_bound_offset: u64,
    },
    #[error("transactional read token offset {log_upper_bound_offset} exceeds i64 range")]
    ReadTokenOutOfRange { log_upper_bound_offset: u64 },
}

impl ChromaError for ConditionalTransactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ConditionalTransactionError::Closed
            | ConditionalTransactionError::FilterReadRequiresPositiveLimit
            | ConditionalTransactionError::ReadAfterBufferedWrite { .. } => {
                ErrorCodes::InvalidArgument
            }
            ConditionalTransactionError::MissingReadToken
            | ConditionalTransactionError::ReadTokenMismatch { .. }
            | ConditionalTransactionError::ReadTokenOutOfRange { .. } => {
                ErrorCodes::FailedPrecondition
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::{
        CollectionUuid, Include, IncludeList, MetadataComparison, MetadataExpression,
        MetadataValue, OccReadMode, PrimitiveOperator, Where,
    };

    use super::*;

    fn string_set(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    fn request(
        ids: Option<Vec<&str>>,
        where_clause: Option<Where>,
        limit: Option<u32>,
    ) -> GetRequest {
        GetRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            ids.map(|ids| ids.into_iter().map(String::from).collect()),
            where_clause,
            limit,
            0,
            IncludeList(vec![Include::Document, Include::Metadata]),
        )
        .unwrap()
    }

    fn response(ids: &[&str], token_offset: u64) -> GetResponse {
        GetResponse {
            ids: ids.iter().map(|id| (*id).to_string()).collect(),
            include: vec![Include::Document, Include::Metadata],
            occ_read_token: Some(OccReadToken::try_new(token_offset).unwrap()),
            ..Default::default()
        }
    }

    fn response_without_token(ids: &[&str]) -> GetResponse {
        GetResponse {
            ids: ids.iter().map(|id| (*id).to_string()).collect(),
            include: vec![Include::Document, Include::Metadata],
            ..Default::default()
        }
    }

    fn metadata_where() -> Where {
        Where::Metadata(MetadataExpression {
            key: "status".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("ready".to_string()),
            ),
        })
    }

    #[test]
    fn transactional_get_prepares_capture_mode_and_preserves_response() {
        let mut state = ConditionalTransactionState::new();
        let request = request(Some(vec!["doc-1"]), None, None);
        let prepared = state.prepare_get_request(request.clone()).unwrap();

        assert_eq!(prepared.occ_read_mode(), OccReadMode::Capture);
        assert_eq!(prepared.include, request.include);

        let response = response(&["doc-1"], 42);
        let finished = state.finish_get(&prepared, response.clone()).unwrap();

        assert_eq!(finished.ids, response.ids);
        assert_eq!(finished.include, response.include);
        assert_eq!(state.read_token(), Some(OccReadToken::try_new(42).unwrap()));
    }

    #[test]
    fn point_get_includes_absent_ids_in_read_set() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["present", "absent"]), None, None))
            .unwrap();

        state
            .record_get_response(&request, &response(&["present"], 50))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["present", "absent"]));
        assert_eq!(state.known_present(), &string_set(&["present"]));
        assert_eq!(state.known_absent(), &string_set(&["absent"]));
    }

    #[test]
    fn later_reads_reuse_first_read_token() {
        let mut state = ConditionalTransactionState::new();
        let first = state
            .prepare_get_request(request(Some(vec!["first"]), None, None))
            .unwrap();
        state
            .record_get_response(&first, &response(&["first"], 100))
            .unwrap();
        let second = state
            .prepare_get_request(request(Some(vec!["second"]), None, None))
            .unwrap();
        assert_eq!(
            second.occ_read_mode(),
            OccReadMode::AtToken(OccReadToken::try_new(100).unwrap())
        );
        state
            .record_get_response(&second, &response_without_token(&["second"]))
            .unwrap();

        assert_eq!(
            state.read_token(),
            Some(OccReadToken::try_new(100).unwrap())
        );
        assert_eq!(state.read_ids(), &string_set(&["first", "second"]));
    }

    #[test]
    fn later_capture_with_different_token_is_rejected() {
        let mut state = ConditionalTransactionState::new();
        let first = state
            .prepare_get_request(request(Some(vec!["first"]), None, None))
            .unwrap();
        state
            .record_get_response(&first, &response(&["first"], 100))
            .unwrap();
        let stale_capture =
            request(Some(vec!["second"]), None, None).with_occ_read_token_generation();

        assert!(matches!(
            state.record_get_response(&stale_capture, &response(&["second"], 101)),
            Err(ConditionalTransactionError::ReadTokenMismatch {
                expected_log_upper_bound_offset: 100,
                actual_log_upper_bound_offset: 101,
            })
        ));
    }

    #[test]
    fn filter_only_get_requires_positive_limit() {
        let state = ConditionalTransactionState::new();

        assert!(matches!(
            state.prepare_get_request(request(None, Some(metadata_where()), None)),
            Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit)
        ));
        assert!(matches!(
            state.prepare_get_request(request(None, Some(metadata_where()), Some(0))),
            Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit)
        ));

        let prepared = state
            .prepare_get_request(request(None, Some(metadata_where()), Some(1)))
            .unwrap();
        assert_eq!(prepared.occ_read_mode(), OccReadMode::Capture);
    }

    #[test]
    fn filter_only_get_uses_returned_ids_as_read_set() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(None, Some(metadata_where()), Some(10)))
            .unwrap();

        state
            .record_get_response(&request, &response(&["doc-1", "doc-2"], 60))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["doc-1", "doc-2"]));
        assert_eq!(state.known_present(), &string_set(&["doc-1", "doc-2"]));
        assert!(state.known_absent().is_empty());
    }

    #[test]
    fn ids_with_filter_marks_only_returned_ids_present() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(
                Some(vec!["returned", "unknown"]),
                Some(metadata_where()),
                None,
            ))
            .unwrap();

        state
            .record_get_response(&request, &response(&["returned"], 70))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["returned", "unknown"]));
        assert_eq!(state.known_present(), &string_set(&["returned"]));
        assert!(state.known_absent().is_empty());
    }

    #[test]
    fn absent_point_reads_are_tracked() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["missing"]), None, None))
            .unwrap();

        state
            .record_get_response(&request, &response(&[], 80))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["missing"]));
        assert!(state.known_present().is_empty());
        assert_eq!(state.known_absent(), &string_set(&["missing"]));
    }

    #[test]
    fn reading_buffered_write_id_fails_but_other_ids_can_be_read() {
        let mut state = ConditionalTransactionState::new();
        state.buffered_write_ids.insert("written".to_string());

        assert!(matches!(
            state.prepare_get_request(request(Some(vec!["written"]), None, None)),
            Err(ConditionalTransactionError::ReadAfterBufferedWrite { id })
                if id == "written"
        ));

        let prepared = state
            .prepare_get_request(request(Some(vec!["other"]), None, None))
            .unwrap();
        state
            .record_get_response(&prepared, &response(&["other"], 90))
            .unwrap();
        assert_eq!(state.read_ids(), &string_set(&["other"]));
    }

    #[test]
    fn missing_occ_read_token_fails_transactional_get() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["doc"]), None, None))
            .unwrap();
        let response = GetResponse {
            ids: vec!["doc".to_string()],
            include: vec![Include::Document],
            ..Default::default()
        };

        assert!(matches!(
            state.record_get_response(&request, &response),
            Err(ConditionalTransactionError::MissingReadToken)
        ));
    }

    #[test]
    fn out_of_range_read_token_fails_transactional_get() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["doc"]), None, None))
            .unwrap();
        let response = response(&["doc"], i64::MAX as u64 + 1);

        assert_eq!(
            state.record_get_response(&request, &response),
            Err(ConditionalTransactionError::ReadTokenOutOfRange {
                log_upper_bound_offset: i64::MAX as u64 + 1
            })
        );
    }

    #[test]
    fn transaction_state_tracks_buffers_and_closed_state() {
        let mut state = ConditionalTransactionState::new();

        assert!(state.buffered_writes().is_empty());
        assert!(state.buffered_write_ids().is_empty());
        assert!(!state.is_closed());

        state.close();

        assert!(state.is_closed());
        assert!(matches!(
            state.prepare_get_request(request(Some(vec!["doc"]), None, None)),
            Err(ConditionalTransactionError::Closed)
        ));
    }
}
