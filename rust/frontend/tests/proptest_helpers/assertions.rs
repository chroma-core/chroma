use chroma_types::{are_metadatas_close_to_equal, GetResponse};
use ndarray::{Array, Array2};

fn embeddings_list_to_ndarray(embeddings: &[Vec<f32>]) -> Array2<f32> {
    if embeddings.is_empty() {
        return Array::from_shape_vec((0, 0), vec![]).unwrap();
    }

    let n = embeddings.len();
    let d = embeddings[0].len();
    let flattened = embeddings.iter().flatten().cloned().collect::<Vec<f32>>();
    Array::from_shape_vec((n, d), flattened).unwrap()
}

pub fn check_get_responses_are_close_to_equal(
    mut reference: GetResponse,
    mut received: GetResponse,
) {
    reference.sort_by_ids();
    received.sort_by_ids();

    // Check IDs
    assert_eq!(
        received.ids, reference.ids,
        "Received IDs {:?}, expected IDs {:?}",
        received.ids, reference.ids
    );

    // Check embeddings
    match (
        reference.embeddings.is_some(),
        received.embeddings.is_some(),
    ) {
        (true, false) => {
            panic!(
                "Expected that embeddings are Some(..). Expected {:?}",
                reference.embeddings
            );
        }
        (false, true) => {
            panic!(
                "Expected that embeddings are None, but got {:?}",
                received.embeddings
            );
        }
        _ => {}
    }
    if let Some(reference_embeddings) = reference.embeddings.as_ref() {
        if let Some(received_embeddings) = received.embeddings.as_ref() {
            let reference_embeddings = embeddings_list_to_ndarray(reference_embeddings);
            let received_embeddings = embeddings_list_to_ndarray(received_embeddings);

            assert!(
                reference_embeddings.abs_diff_eq(&received_embeddings, 1.0e-6),
                "Received embeddings {:?}, expected embeddings {:?}",
                received_embeddings,
                reference_embeddings,
            );
        }
    }

    // Check documents
    assert_eq!(
        reference.documents, received.documents,
        "Received documents {:?}, expected documents {:?}",
        received.documents, reference.documents
    );

    // Check metadata
    match (reference.metadatas.is_some(), received.metadatas.is_some()) {
        (true, false) => {
            panic!(
                "Expected that metadatas are Some(..). Expected {:?}",
                reference.metadatas
            );
        }
        (false, true) => {
            panic!(
                "Expected that metadatas are None, but got {:?}",
                received.metadatas
            );
        }
        _ => {}
    }

    if let Some(reference_metadatas) = reference.metadatas.as_ref() {
        if let Some(received_metadatas) = received.metadatas.as_ref() {
            assert_eq!(
                reference_metadatas.len(),
                received_metadatas.len(),
                "Expected {} metatadas, but got {}",
                reference_metadatas.len(),
                received_metadatas.len()
            );

            for i in 0..reference_metadatas.len() {
                let reference = &reference_metadatas[i];
                let received = &received_metadatas[i];

                match (reference.is_some(), received.is_some()) {
                    (true, false) => {
                        panic!(
                            "Expected that metadata at index {} is Some(..). Expected {:?}",
                            i, reference
                        );
                    }
                    (false, true) => {
                        panic!(
                            "Expected that metadata at index {} is None, but got {:?}",
                            i, received
                        );
                    }
                    _ => {}
                }

                if let Some(reference) = reference {
                    if let Some(received) = received {
                        assert!(
                            are_metadatas_close_to_equal(reference, received),
                            "Received metadata {:#?}, expected metadata {:#?}",
                            reference,
                            received,
                        );
                    }
                }
            }
        }
    }
}
