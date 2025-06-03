use std::any::TypeId;

pub fn type_id_to_string(id: TypeId) -> String {
    let repr = format!("{:?}", id);
    repr.trim_start_matches("TypeId { t: ")
        .trim_end_matches(" }")
        .to_string()
}
