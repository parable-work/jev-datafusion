mod support;

use datafusion::common::Result;
use jev_datafusion::sql;
use support::{rows, Mock};

#[tokio::test]
async fn inner_join_semantic_filter_runs_asynchronously_after_cheap_candidate_matching(
) -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![
        (42, "keep".into()),
        (42, "other".into()),
        (99, "outside".into()),
    ]]])?;
    let result = sql(&mock.context, "SELECT l.body AS source, r.body AS candidate, noul(named_struct('source',l.body,'candidate',r.body),'Do these match?') AS p FROM episodes l JOIN episodes r ON l.tenant_id = r.tenant_id WHERE l.body = 'keep' AND noul(named_struct('source',l.body,'candidate',r.body),'Do these match?') > 0.8").await?.collect().await?;
    assert_eq!(rows(&result), 2);
    assert_eq!(
        mock.count(),
        2,
        "Only the two cheap candidate pairs are classified, with SELECT/WHERE reuse"
    );
    Ok(())
}

#[tokio::test]
async fn inner_join_keeps_cheap_non_equi_matching_before_paid_on_predicate() -> Result<()> {
    let mock = Mock::new(4, 32).await?;
    mock.table(vec![vec![vec![
        (42, "keep".into()),
        (42, "other".into()),
        (99, "outside".into()),
    ]]])?;
    let result = sql(&mock.context, "SELECT l.body AS source, r.body AS candidate FROM episodes l JOIN episodes r ON l.tenant_id = r.tenant_id AND l.body <> r.body AND noul(named_struct('source',l.body,'candidate',r.body),'Do these match?') > 0.8 WHERE l.body = 'keep'").await?.collect().await?;
    assert_eq!(rows(&result), 1);
    assert_eq!(mock.count(), 1);
    Ok(())
}

#[tokio::test]
async fn outer_join_row_preservation_is_unchanged_for_paid_projection() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![
        (42, "keep".into()),
        (42, "other".into()),
        (99, "outside".into()),
    ]]])?;
    let result = sql(&mock.context, "SELECT l.body, r.body AS missing, noul(l.body,'Q?') FROM episodes l LEFT JOIN episodes r ON l.tenant_id = r.tenant_id AND r.body = 'missing'").await?.collect().await?;
    assert_eq!(rows(&result), 3);
    assert_eq!(
        result
            .iter()
            .map(|batch| batch.column(1).null_count())
            .sum::<usize>(),
        3
    );
    assert_eq!(mock.count(), 3);
    Ok(())
}
