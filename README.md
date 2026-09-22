# jev-datafusion

Add four SQL functions to a DataFusion session: `noul`, `choice`, `score`, and `ask`. Each one sends the row's state to a judgment server and returns a typed answer.

This is a library. It does not include a copy of DataFusion. Your project depends on DataFusion 53.1.0 from crates.io, and on this crate, which registers the functions on a `SessionContext`.

TypeSafe is one server. OpenRouter is another. Any endpoint that accepts the same JSON body can be a third.

## Copy this to your agent

```text
Add jev-datafusion to this Rust project. Do not vendor or fork DataFusion.

1. Add these dependencies. Match an existing DataFusion dependency to 53.1.0 if the project already has one:

[dependencies]
jev-datafusion = { git = "https://github.com/parable-work/jev-datafusion" }
datafusion = "=53.1.0"

2. Register exactly one server before any judgment query. Registering twice fails because the function names are already taken.

use std::sync::Arc;
use datafusion::prelude::SessionContext;
use jev_datafusion::{register, sql, OpenRouter, TypeSafe};

let ctx = SessionContext::new();

// TypeSafe. Reads TYPESAFE_API_KEY. Default model: jev-1.13.0
// POST https://api.typesafe.ai/v1/systemone
register(&ctx, Arc::new(TypeSafe::from_env()?))?;

// OpenRouter instead. Reads OPENROUTER_API_KEY. Default model: typesafe/jev-1.13
// POST https://openrouter.ai/api/alpha/decisions
// register(&ctx, Arc::new(OpenRouter::from_env()?))?;

3. Run SQL through jev_datafusion::sql, not SessionContext::sql. The wrapper fills omitted optional arguments. These four queries are the whole surface:

SELECT noul(message, instructions => 'Is the customer asking for money back?') AS p_refund
FROM tickets;

SELECT choice(
  message,
  instructions => 'Which queue should handle this?',
  criteria => '{"billing":"Charges, invoices, refunds","technical":"Bugs and outages","other":"Anything else"}'
) AS queue
FROM tickets;

SELECT score(
  message,
  instructions => 'How soon does this need a reply?',
  criteria => '["Can wait a week","This week","Today"]'
) AS urgency
FROM tickets;

SELECT ask(
  message,
  questions => '{"refund":{"type":"noul","instructions":"Is the customer asking for money back?"},"queue":{"type":"choice","instructions":"Which queue should handle this?","criteria":{"billing":"Charges and refunds","other":"Anything else"}}}'
) AS raw
FROM tickets;

4. Keep API keys in the environment. Do not commit them and do not put them in CI. noul returns a float. choice returns label, confidence, and probabilities. score returns score, confidence, and probabilities. ask returns the response text unchanged. An explicit model => '...' argument overrides the server default. SET jev.on_error = 'null' turns a bad row into NULL instead of failing the query.
```

## Quick start

```toml
[dependencies]
jev-datafusion = { git = "https://github.com/parable-work/jev-datafusion" }
datafusion = "=53.1.0"
```

```rust
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use jev_datafusion::{register, sql, TypeSafe};

let ctx = SessionContext::new();
register(&ctx, Arc::new(TypeSafe::from_env()?))?;

let batches = sql(
    &ctx,
    "SELECT noul(message, instructions => 'Is the customer asking for money back?') FROM tickets",
)
.await?
.collect()
.await?;
```

Use `OpenRouter::from_env()` instead of `TypeSafe::from_env()` if the key you have is an OpenRouter key. Register one of them.

To point at any other System One URL, including a model that is not Jev:

```rust
use jev_datafusion::Compatible;

register(
    &ctx,
    Arc::new(Compatible::new(
        Some(api_key),
        "https://example.test/v1/systemone",
        "your-model-id",
    )?),
)?;
```

The model id is whatever that server expects. `jev-1.13.0` is only the TypeSafe default.

## Four queries

Start from a table with one text column:

```sql
-- message: 'I was charged twice for my annual plan this morning. Please refund one of the charges today.'
```

Yes/no. One float from 0 to 1. Near 0.5 means the call is ambiguous, not "medium intensity."

```sql
SELECT noul(
  message,
  instructions => 'Is the customer asking for money back?'
) AS p_refund
FROM tickets;
```

Pick one label and keep the distribution.

```sql
SELECT choice(
  message,
  instructions => 'Which queue should handle this?',
  criteria => '{"billing":"Charges, invoices, refunds","technical":"Bugs and outages","other":"Anything else"}'
) AS queue
FROM tickets;
```

`queue` is a struct: `label`, `confidence`, `probabilities`.

Place the row on an ordered rubric. The score is the expected position on that list, not a percentage of time saved.

```sql
SELECT score(
  message,
  instructions => 'How soon does this need a reply?',
  criteria => '["Can wait a week","This week","Today"]'
) AS urgency
FROM tickets;
```

`urgency` is a struct: `score`, `confidence`, `probabilities`.

Ask several questions in one request. The result is the server's response text, unchanged, so you can read it with DataFusion's JSON functions.

```sql
SELECT ask(
  message,
  questions => '{"refund":{"type":"noul","instructions":"Is the customer asking for money back?"},"queue":{"type":"choice","instructions":"Which queue should handle this?","criteria":{"billing":"Charges and refunds","other":"Anything else"}}}'
) AS raw
FROM tickets;
```

Pass `model => 'typesafe/jev-1.13'` or `model => 'jev-1.13.0'` when you want that call pinned. Otherwise the registered server's default is used.

```sql
SET jev.model = 'jev-1.13.0';
SET jev.on_error = 'null';
```

`fail` stops the query. `null` returns NULL for a bad row. Missing credentials still fail while planning.

## Check it against your own key

The default tests start DataFusion, register the functions, and run these SQL queries against a local HTTP server. GitHub Actions then sends one `noul` query to TypeSafe and one to OpenRouter, using repository secrets. The keys are not in the repository. A live answer only has to be a probability from 0 to 1.

To send the four queries above to your account:

```bash
TYPESAFE_API_KEY=... cargo run --example live
OPENROUTER_API_KEY=... cargo run --example live
```

OpenRouter wins if both variables are set. The example prints which server it used and the four result tables. It does not print the key.

## What the tests already prove

`cargo test` builds this crate against DataFusion 53.1.0 and runs the SQL. A passing test means DataFusion planned the function, sent the JSON body to the server URL for that adapter, and decoded the answer into Arrow. TypeSafe, OpenRouter, and a caller-supplied URL are covered by those local servers.

```bash
cargo test
cargo test --test suite bounded_mock_matrix -- --ignored --test-threads=1
cargo test --test suite benchmark_ -- --ignored --test-threads=1
```

The ignored tests are larger mock-server benchmarks. They still do not call a hosted model.

## Behavior worth knowing

- At most eight HTTP requests are in flight per process. 429 is retried. TypeSafe and compatible endpoints also retry 529. Four attempts, 60 second deadline.
- Identical requests in one batch are sent once.
- A missing key fails at planning time. The error names `TYPESAFE_API_KEY`, `OPENROUTER_API_KEY`, or `JEV_API_KEY`.
- TypeSafe prices `jev-1.13.0` at $0.042 per million input tokens. OpenRouter and compatible servers use `usage.cost` when the response includes it.
- State can be text, JSON, an Arrow struct or map, or a Parquet Variant.

## License

Apache-2.0. See [LICENSE](LICENSE).
