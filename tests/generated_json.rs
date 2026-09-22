use jev_datafusion::{JevQuestion, JevRequest};
use serde_json::{json, Value};

fn evidence() -> Value {
    json!({
        "nested": [
            {"$serde_json::private::Number": "1"},
            {"$serde_json::private::RawValue": "true"},
            {"$serde_json::private::Number": "not a number", "other": 2},
            {"$serde_json::private::RawValue": "not JSON", "other": null},
            null,
            "",
            {},
            []
        ],
        "precise": "12345678901234567890.123456789012345678901234567890".parse::<serde_json::Number>().unwrap()
    })
}

#[test]
fn generated_jev_types_preserve_generic_json_from_text_and_values() {
    let expected = evidence();
    let question = json!({"type":"noul", "instructions":expected, "criteria":expected});
    for decoded in [
        serde_json::from_str::<JevQuestion>(&question.to_string()).unwrap(),
        serde_json::from_value::<JevQuestion>(question.clone()).unwrap(),
    ] {
        assert_eq!(decoded.instructions, expected);
        assert_eq!(decoded.criteria, Some(expected.clone()));
    }
    let request =
        json!({"state":expected, "questions":{"q":question}, "model":"controlled-fixture"});
    for decoded in [
        serde_json::from_str::<JevRequest>(&request.to_string()).unwrap(),
        serde_json::from_value::<JevRequest>(request).unwrap(),
    ] {
        assert_eq!(decoded.state, expected);
        assert_eq!(decoded.questions["q"].instructions, expected);
        assert_eq!(decoded.questions["q"].criteria, Some(expected.clone()));
    }
}

#[test]
fn generated_jev_json_presence_and_null_semantics_are_unchanged() {
    for input in [
        r#"{"type":"noul","instructions":null}"#,
        r#"{"type":"noul","instructions":null,"criteria":null}"#,
    ] {
        let value: JevQuestion = serde_json::from_str(input).unwrap();
        assert_eq!(value.instructions, Value::Null);
        assert!(value.criteria.is_none());
    }
    assert!(serde_json::from_str::<JevQuestion>(r#"{"type":"noul"}"#).is_err());
    assert!(serde_json::from_str::<JevRequest>(r#"{"questions":{},"model":"test"}"#).is_err());
    let request: JevRequest =
        serde_json::from_str(r#"{"state":null,"questions":{},"model":"test"}"#).unwrap();
    assert!(request.state.is_null());
    assert!(request.questions.is_empty());
}
