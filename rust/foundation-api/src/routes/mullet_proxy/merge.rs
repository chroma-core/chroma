use axum::body::Bytes;
use serde_json::Value;

use super::error::MulletProxyError;

/// Inject the auth-resolved `user` into the caller's JSON body. Overwrites
/// any caller-supplied `user` so a client can't impersonate. An empty
/// body is treated as `{}` so a body-less POST still reaches mullet with
/// just `{"user": "<id>"}` (mullet's own zod schema rejects missing
/// required fields with 400, which the proxy relays verbatim).
pub(super) fn merge_user(body: &Bytes, user_id: String) -> Result<Value, MulletProxyError> {
    let mut value: Value = if body.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(body).map_err(|e| MulletProxyError::InvalidBody(e.to_string()))?
    };
    let Value::Object(map) = &mut value else {
        return Err(MulletProxyError::InvalidBody(
            "request body must be a JSON object".to_string(),
        ));
    };
    map.insert("user".to_string(), Value::String(user_id));
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_user_inserts_into_empty_object() {
        let body = Bytes::from(r#"{}"#);
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"user": "42"}));
    }

    #[test]
    fn merge_user_overrides_caller_supplied_user() {
        let body = Bytes::from(r#"{"query":"hi","user":"attacker"}"#);
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"query": "hi", "user": "42"}));
    }

    #[test]
    fn merge_user_preserves_other_keys_without_email_or_tenant() {
        let body = Bytes::from(r#"{"query":"q","session_id":"s","repo":"r","source":"cli"}"#);
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(
            merged,
            serde_json::json!({
                "query": "q",
                "session_id": "s",
                "repo": "r",
                "source": "cli",
                "user": "42",
            })
        );
        let obj = merged.as_object().unwrap();
        assert!(!obj.contains_key("email"));
        assert!(!obj.contains_key("tenant"));
    }

    #[test]
    fn merge_user_treats_empty_body_as_empty_object() {
        let body = Bytes::new();
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"user": "42"}));
    }

    #[test]
    fn merge_user_rejects_non_object_body() {
        let body = Bytes::from(r#"["not","an","object"]"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, MulletProxyError::InvalidBody(_)));
    }

    #[test]
    fn merge_user_rejects_invalid_json() {
        let body = Bytes::from(r#"not json"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, MulletProxyError::InvalidBody(_)));
    }
}
