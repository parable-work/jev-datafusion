# jev-datafusion

DataFusion SQL functions for typed judgments:

| Function | Returns |
| --- | --- |
| `noul(state, instructions, criteria?, model?)` | `Float64` probability of yes |
| `choice(state, instructions, criteria, model?)` | `label`, `confidence`, and the option distribution |
| `score(state, instructions, criteria, model?)` | `score`, `confidence`, and the level distribution |
| `ask(state, questions, model?)` | the response text, unchanged |

`model` is the id your server expects. `jev-1.13.0` is TypeSafe's default. It is not the only model these functions can call.

## Install

```toml
[dependencies]
jev-datafusion = { git = "https://github.com/parable-work/jev-datafusion" }
datafusion = "=53.1.0"
```

The crate is pinned to DataFusion 53.1.0 and Arrow 58.3.0.

## Servers

TypeSafe is one server.

```rust
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use jev_datafusion::{register, Compatible, OpenRouter, TypeSafe};

let context = SessionContext::new();

// POST https://api.typesafe.ai/v1/systemone
// TYPESAFE_API_KEY, default model jev-1.13.0
register(&context, Arc::new(TypeSafe::from_env()?))?;

// POST https://openrouter.ai/api/alpha/decisions
// OPENROUTER_API_KEY, default model typesafe/jev-1.13
register(&context, Arc::new(OpenRouter::from_env()?))?;

// Any endpoint that accepts the same JSON body, including a non-Jev model.
register(
    &context,
    Arc::new(Compatible::new(
        Some(api_key),
        "https://ai-gateway.vercel.sh/typesafe/v1/systemone",
        "typesafe-ai/jev",
    )?),
)?;
```

```sql
SELECT noul(message, instructions => 'Is the customer asking for a refund?')
FROM tickets;

SELECT choice(
  state,
  instructions => 'Which queue should handle this?',
  criteria => '{"billing":"Charges and refunds","technical":"Bugs and outages"}',
  model => 'jev-1.13.0'
)
FROM tickets;
```

`SET jev.model` and `SET jev.on_error` (`fail` or `null`) apply to the session. An explicit `model =>` argument wins over the session default. The session default comes from the server you registered.

## Behavior worth knowing

- At most eight HTTP requests are in flight per process. 429 is retried. TypeSafe and compatible System One endpoints also retry 529. Four attempts, 60 second deadline.
- Identical requests in one batch are sent once. The cache is charged to the query memory pool and is not shared across queries.
- Missing credentials fail while planning. The error names that server's key (`TYPESAFE_API_KEY`, `OPENROUTER_API_KEY`, or `JEV_API_KEY`).
- TypeSafe prices `jev-1.13.0` at $0.042 per million input tokens. OpenRouter and compatible servers use `usage.cost` when the response includes it. Anything else is counted as unpriced.
- State can be text, JSON, an Arrow struct or map, or a Parquet Variant. Duplicate JSON keys are rejected.

## Test

```bash
cargo test
```

Live calls are not part of `cargo test`.

## License

Apache-2.0. See [LICENSE](LICENSE).
