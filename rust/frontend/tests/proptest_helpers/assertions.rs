use chroma_types::{are_metadatas_close_to_equal, GetResponse, QueryResponse};

pub fn check_get_responses_are_close_to_equal(reference: &GetResponse, received: &GetResponse) {
    assert_eq!(
        reference.ids, received.ids,
        "Expected {:?} to be equal to {:?}",
        reference.ids, received.ids
    );
    assert_eq!(
        reference.embeddings, received.embeddings,
        "Expected {:?} to be equal to {:?}",
        reference.embeddings, received.embeddings
    );
    assert_eq!(
        reference.documents, received.documents,
        "Expected {:?} to be equal to {:?}",
        reference.documents, received.documents
    );
    assert_eq!(
        reference.metadatas.is_none(),
        received.metadatas.is_none(),
        "Expected {:?} to be equal to {:?}",
        reference.metadatas,
        received.metadatas
    );

    if let Some(reference_metadatas) = reference.metadatas.as_ref() {
        if let Some(received_metadatas) = received.metadatas.as_ref() {
            assert_eq!(
                reference_metadatas.len(),
                received_metadatas.len(),
                "Expected {:?} to be equal to {:?}",
                reference,
                received
            );
            for i in 0..reference_metadatas.len() {
                let reference = &reference_metadatas[i];
                let received = &received_metadatas[i];

                assert_eq!(
                    reference.is_none(),
                    received.is_none(),
                    "Expected {:?} to be equal to {:?}",
                    reference,
                    received
                );

                if let Some(reference) = reference {
                    if let Some(received) = received {
                        assert!(
                            are_metadatas_close_to_equal(reference, received),
                            "Expected {:?} to be equal to {:?}",
                            reference,
                            received
                        );
                    }
                }
            }
        }
    }
}

// todo: check distances
pub fn check_query_responses_are_close_to_equal(
    reference: &QueryResponse,
    received: &QueryResponse,
) {
    assert_eq!(
        reference.ids, received.ids,
        "Expected {:?} to be equal to {:?}",
        reference.ids, received.ids
    );
    assert_eq!(
        reference.embeddings, received.embeddings,
        "Expected {:?} to be equal to {:?}",
        reference.embeddings, received.embeddings
    );
    assert_eq!(
        reference.documents, received.documents,
        "Expected {:?} to be equal to {:?}",
        reference.documents, received.documents
    );
    assert_eq!(
        reference.metadatas.is_none(),
        received.metadatas.is_none(),
        "Expected {:?} to be equal to {:?}",
        reference.metadatas,
        received.metadatas
    );

    if let Some(reference_metadatas_list) = reference.metadatas.as_ref() {
        if let Some(received_metadatas_list) = received.metadatas.as_ref() {
            assert_eq!(
                reference_metadatas_list.len(),
                received_metadatas_list.len(),
                "Expected {:?} to be equal to {:?}",
                reference_metadatas_list.len(),
                received_metadatas_list.len()
            );
            for i in 0..reference_metadatas_list.len() {
                let reference_metadatas = &reference_metadatas_list[i];
                let received_metadatas = &received_metadatas_list[i];

                assert_eq!(
                    reference_metadatas.len(),
                    received_metadatas.len(),
                    "Expected {:?} to be equal to {:?}",
                    reference_metadatas,
                    received_metadatas
                );

                for i in 0..reference_metadatas.len() {
                    let reference = &reference_metadatas[i];
                    let received = &received_metadatas[i];

                    assert_eq!(
                        reference.is_none(),
                        received.is_none(),
                        "Expected {:?} to be equal to {:?}",
                        reference,
                        received
                    );

                    if let Some(reference) = reference {
                        if let Some(received) = received {
                            assert!(
                                are_metadatas_close_to_equal(reference, received),
                                "Expected {:?} to be equal to {:?}",
                                reference,
                                received
                            );
                        }
                    }
                }
            }
        }
    }
}
