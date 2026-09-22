#[path = "../../src/state.rs"]
#[allow(dead_code)] // Includes the shared converter; instructions are tested separately.
mod state;

use std::sync::{Arc, Mutex};

use arrow::{
    array::{Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema},
};
use axum::{routing::post, Router};
use datafusion::{
    common::ScalarValue,
    prelude::{ParquetReadOptions, SessionContext},
};
use jev_datafusion::{register_jev, sql, JevProvider};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::LogicalType,
    variant::{VariantArrayBuilder, VariantBuilderExt},
};
use serde_json::{json, Value};

// Reuse the parent issue's reviewable synthetic work episodes, including the
// underspecified and prompt-injection records. No live model expectations.
fn fixture() -> RecordBatch {
    let document: Value =
        serde_json::from_str(include_str!("../fixtures/work-episodes.json")).unwrap();
    let episodes = document["episodes"].as_array().unwrap();
    let states = episodes
        .iter()
        .map(|episode| &episode["state"])
        .collect::<Vec<_>>();
    let text = Arc::new(StringArray::from_iter_values(
        states
            .iter()
            .map(|state| json!({"episode":state}).to_string()),
    )) as ArrayRef;
    let activity = Arc::new(StringArray::from_iter_values(
        states
            .iter()
            .map(|state| state["activity"].as_str().unwrap()),
    )) as ArrayRef;
    let constraints = Arc::new(StringArray::from_iter_values(
        states
            .iter()
            .map(|state| state["constraints"].as_str().unwrap()),
    )) as ArrayRef;
    let episode = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("constraints", DataType::Utf8, false)),
            constraints,
        ),
        (
            Arc::new(Field::new("activity", DataType::Utf8, false)),
            activity,
        ),
    ])) as ArrayRef;
    let structured = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("episode", episode.data_type().clone(), false)),
        episode.clone(),
    )])) as ArrayRef;
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["episode"; states.len()])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("values", episode.data_type().clone(), true)),
            episode,
        ),
    ]);
    let map = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, states.len())),
            entries,
            None,
            false,
        )
        .unwrap(),
    ) as ArrayRef;
    let mut builder = VariantArrayBuilder::new(states.len());
    for (index, state) in states.iter().enumerate() {
        let mut object = builder.new_object();
        let mut fields = state.as_object().unwrap().iter().collect::<Vec<_>>();
        if index % 2 == 0 {
            fields.reverse();
        }
        for (name, value) in fields {
            object.insert(name, value.as_str().unwrap());
        }
        object.finish();
    }
    let encoded = builder.build();
    let encoded_field = encoded.field("encoded");
    let encoded = ArrayRef::from(encoded);
    let variant = Arc::new(StructArray::from(vec![(
        Arc::new(encoded_field.clone().with_name("episode")),
        encoded.clone(),
    )])) as ArrayRef;
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("text_state", text.data_type().clone(), false),
            Field::new("struct_state", structured.data_type().clone(), false),
            Field::new("map_state", map.data_type().clone(), false),
            Field::new("variant_state", variant.data_type().clone(), false),
            encoded_field,
        ])),
        vec![text, structured, map, variant, encoded],
    )
    .unwrap()
}

#[tokio::test]
async fn parquet_source_encodings_produce_identical_real_udf_request_bytes() {
    let input = fixture();
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), input.schema(), None).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    assert_eq!(
        reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema()
            .get_fields()[4]
            .get_basic_info()
            .logical_type_ref(),
        Some(&LogicalType::Variant {
            specification_version: None
        })
    );
    let decoded = reader.build().unwrap().next().unwrap().unwrap();
    assert_eq!(
        decoded.schema().field(4).extension_type_name(),
        Some("arrow.parquet.variant")
    );
    for row in 0..decoded.num_rows() {
        let expected =
            state::state_json(ScalarValue::try_from_array(decoded.column(0), row).unwrap())
                .unwrap();
        for column in 1..4 {
            let actual = state::state_json_with_field(
                ScalarValue::try_from_array(decoded.column(column), row).unwrap(),
                Some(decoded.schema().field(column)),
            )
            .unwrap();
            assert_eq!(
                state::canonical_bytes(&actual).unwrap(),
                state::canonical_bytes(&expected).unwrap()
            );
        }
        let root_variant = state::state_json_with_field(
            ScalarValue::try_from_array(decoded.column(4), row).unwrap(),
            Some(decoded.schema().field(4)),
        )
        .unwrap();
        assert_eq!(root_variant, expected["episode"]);
    }

    let bodies = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let captured = bodies.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move |body: String| {
            captured.lock().unwrap().push(body.into_bytes());
            async { r#"{"answers":{"q0":{"noul":0.5}}}"# }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("controlled-fixture".into()), &endpoint).unwrap()),
    )
    .unwrap();
    context
        .register_parquet(
            "work_episodes",
            file.path().to_str().unwrap(),
            // Variant is an Arrow extension: retaining its metadata is part of
            // reading its storage contract, not a Jev-specific representation.
            ParquetReadOptions::default().skip_metadata(false),
        )
        .await
        .unwrap();
    for column in ["text_state", "struct_state", "map_state", "variant_state"] {
        let output = sql(
            &context,
            &format!("SELECT noul({column}, 'Is recurring work evidenced?') FROM work_episodes"),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            input.num_rows()
        );
    }
    // A root Variant needs the Field extension metadata carried by real SQL.
    let root = sql(
        &context,
        "SELECT noul(encoded, 'Is recurring work evidenced?') FROM work_episodes",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(
        root.iter().map(RecordBatch::num_rows).sum::<usize>(),
        input.num_rows()
    );
    let bodies = bodies.lock().unwrap();
    assert_eq!(
        bodies.len(),
        5 * input.num_rows(),
        "separate queries do not share cache entries"
    );
    for representation in bodies.chunks_exact(input.num_rows()).take(4).skip(1) {
        assert_eq!(representation, &bodies[..input.num_rows()]);
    }
    for (root, wrapped) in bodies[4 * input.num_rows()..]
        .iter()
        .zip(&bodies[..input.num_rows()])
    {
        let root = state::parse_json_value(std::str::from_utf8(root).unwrap()).unwrap();
        let wrapped = state::parse_json_value(std::str::from_utf8(wrapped).unwrap()).unwrap();
        assert_eq!(root["state"], wrapped["state"]["episode"]);
    }
    server.abort();
}

#[tokio::test]
async fn invalid_state_and_sql_null_send_zero_http_requests() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async { r#"{"answers":{"q0":{"noul":0.5}}}"# }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("controlled-fixture".into()), &endpoint).unwrap()),
    )
    .unwrap();
    let null = sql(&context, "SELECT noul(CAST(NULL AS VARCHAR), 'question')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(null[0].column(0).null_count(), 1);
    for state in [
        "'{\"n\":1,\"n\":2}'".to_string(),
        "named_struct('n', CAST('NaN' AS DOUBLE))".to_string(),
        "named_struct('n', CAST('2026-09-18T00:00:00.123456789' AS TIMESTAMP))".to_string(),
        "map(['a','a'], [1,2])".to_string(),
        "map([1,2], ['a','b'])".to_string(),
        format!("'{}{}{}'", "[".repeat(65), "null", "]".repeat(65)),
        format!("'{}'", "x".repeat(1_048_576)),
    ] {
        let result = match sql(&context, &format!("SELECT noul({state}, 'question')")).await {
            Ok(dataframe) => dataframe.collect().await.map(|_| ()),
            Err(error) => Err(error),
        };
        assert!(result.is_err(), "invalid state succeeded");
        assert_eq!(requests.load(Ordering::SeqCst), 0);
    }
    // Plain text that happens to spell a JSON primitive remains text and is sent.
    sql(&context, "SELECT noul('null', 'question')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    server.abort();
}
