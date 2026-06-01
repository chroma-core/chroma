use axum::body::Bytes;
use serde_json::Value;

use super::error::AskProxyError;

/// Inject the auth-resolved `user` into the caller's JSON body. Overwrites
/// any caller-supplied `user` so a client can't impersonate. An empty
/// body is treated as `{}` so a body-less POST still reaches Modal with
/// just `{"user": "<id>"}` (Modal's schema rejects missing required
/// fields, which the proxy relays verbatim).
pub(super) fn merge_user(body: &Bytes, user_id: String) -> Result<Value, AskProxyError> {
    let mut value: Value = if body.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(body).map_err(|e| AskProxyError::InvalidBody(e.to_string()))?
    };
    let Value::Object(map) = &mut value else {
        return Err(AskProxyError::InvalidBody(
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
    fn overrides_caller_supplied_user() {
        let body = Bytes::from(r#"{"query":"hi","user":"attacker"}"#);
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"query": "hi", "user": "42"}));
    }

    #[test]
    fn treats_empty_body_as_empty_object() {
        let body = Bytes::new();
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"user": "42"}));
    }

    #[test]
    fn rejects_non_object_body() {
        let body = Bytes::from(r#"["not","an","object"]"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, AskProxyError::InvalidBody(_)));
    }

    #[test]
    fn rejects_invalid_json() {
        let body = Bytes::from(r#"not json"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, AskProxyError::InvalidBody(_)));
    }
}
