pub mod optional_u128_as_hex {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes `Option<u128>` as lowercase hex string if Some, skips if None.
    pub fn serialize<S>(val: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(v) => serializer.serialize_str(&format!("{:x}", v)),
            None => serializer.serialize_none(),
        }
    }

    /// Deserializes a lowercase hex string into `Option<u128>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                let val = u128::from_str_radix(&s, 16).map_err(D::Error::custom)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::optional_u128_as_hex;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        #[serde(
            with = "optional_u128_as_hex",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        value: Option<u128>,
    }

    #[test]
    fn test_serialize_some_u128() {
        let s = TestStruct { value: Some(255) };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"value":"ff"}"#);
    }

    #[test]
    fn test_serialize_none() {
        let s = TestStruct { value: None };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{}"#);
    }

    #[test]
    fn test_deserialize_some_u128() {
        let json = r#"{"value":"ff"}"#;
        let s: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(s, TestStruct { value: Some(255) });
    }

    #[test]
    fn test_deserialize_none() {
        let json = r#"{}"#;
        let s: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(s, TestStruct { value: None });
    }
}
