# Data Lake Engineering: Decisions & Trade-offs

A personal project documenting decisions and trade-offs from building a layered data platform on **Google BigQuery** and **Dataform** — the kind of reasoning that applies to any multi-source data lake feeding both BI and AI agents, not tool-specific trivia. Illustrated with a synthetic SaaS product-analytics scenario (product usage, subscription/billing, support tickets) and a synthetic dataset — not any employer's or client's real data, pipelines, or incidents.

## Key Decisions & Problems Encountered

**Choosing between a managed connector, a managed ELT tool, and a custom connector isn't a style preference — it's forced by failure mode**
For a high-volume support/ticketing source, I tried two managed paths first. A hosted ELT connector kept failing mid-run on the load step into BigQuery once the table crossed a certain row-count threshold — the extraction side was fine, the batch insert wasn't. A native BigQuery connector for the same source didn't yet expose all the tables the model needed — some entities simply weren't in its supported set. Neither failure was a configuration mistake to fix; both were structural limits of the tool for that specific source at that specific volume. The fix was writing a small Python connector directly against the source's REST API, running serverless on a schedule (Cloud Run + Cloud Scheduler) — more code to own, but no black box between the API and BigQuery to debug when it silently stops. Lesson generalized: the more laborious path is sometimes the only reliable one, and that's a decision made per source, not a blanket policy.

**Why the raw layer exists even though it "does nothing"**
Bronze holds unmodified data, partitioned by ingestion date, with no transformations. It looks redundant until a downstream bug needs a full reprocess — replaying from Bronze is a BigQuery-internal operation; replaying from the source API means fighting whatever rate limits and history-retention window that API has. Bronze is insurance against the case where the source can't or won't hand you the same data twice.

**Partition by ingestion date, not event date**
Billing and support systems commonly amend historical records — a subscription charge gets prorated after the fact, a ticket gets recategorized days after it closed. If Bronze partitioned on event date, every one of those amendments would touch an already-closed partition. Partitioning on ingestion date instead keeps Bronze strictly append-only regardless of how much the source revises its own history; reconciling "what changed" becomes Silver's problem, not Bronze's.

**An incremental lookback window is a bet, not a default**
Silver and Gold models use a lookback window to reprocess recent history on every run, sized to cover the kind of delay that shows up in operational systems: processing lag, plus amendments that can backfill or correct a record days after it was first written. A window that's too short silently drops those late corrections. One that's too long makes every incremental run cost close to a full rebuild, for no accuracy gain past the point where amendments stop arriving. There's no universally correct window — it has to be sized against how late the specific source's corrections actually land, and revisited if that behavior changes.

**A failed data-quality check blocks deployment; it doesn't just log a warning**
Assertions that fail stop the Dataform run outright — nothing downstream of a failed table gets published. The alternative (publish with a warning) optimizes for uptime over correctness, which is backwards once a layer feeds an agent instead of just a dashboard: a person looking at a chart with a footnote can apply judgment, but an agent citing a stale or duplicated number states it as fact. A visibly missing answer is a smaller failure than a confidently wrong one.

**Governance as a label on the table, not a separate permissions service**
Every table carries a small set of BigQuery labels (`layer`, `source_system`, `agent_readable`, `domain`) instead of routing access decisions through a separate governance service. The trade-off is real: a label is declarative and lives right next to the schema it describes, cheap to audit by scanning table metadata — but it's enforced by convention (whatever reads the label has to respect it), not by the database engine the way row-level security would be. That's the right trade-off at the scale and trust level of a single internal data platform; it stops being the right trade-off once multiple untrusted consumers need the same data with different access rules.

## Architecture

```
Raw Sources
    │
    ├── Product usage events    (Native BigQuery export)
    ├── Subscription & billing  (Native connector)
    └── Support tickets         (Custom Python connector — see decisions above)
         │
         ▼
    ── BRONZE ──────────────────────────────────────────
    Raw ingestion layer. No transformations.
    Partitioned by ingestion date.
    agent_readable: false
         │
         ▼
    ── SILVER ──────────────────────────────────────────
    Cleaned, deduplicated, typed.
    One model per source/entity.
    Incremental with a lookback window.
    agent_readable: true
         │
         ▼
    ── GOLD ────────────────────────────────────────────
    Business-ready aggregations.
    Unified per-account activity model.
    Optimized for dashboards and AI agents.
    agent_readable: true
```

## Project Structure

```
definitions/
  02_silver/
    product/      # Product usage Silver
    billing/       # Subscription & billing Silver
    support/       # Support tickets Silver
  03_gold/
    account/       # Unified per-account activity
  assertions/
    02_silver/    # Data quality checks per source
    03_gold/      # Cross-source consistency checks
_templates/
  silver_source_template.sqlx
  gold_account_template.sqlx
  assertion_no_duplicates_template.sqlx
```

## Unified Account Activity Model

The `gld_account_activity_daily` table is the core output — a single model consolidating product usage, billing, and support signals per account:

| Column | Description |
|---|---|
| `date` | Event date |
| `account_id` | Account identifier |
| `plan_tier` | Subscription tier (Free / Pro / Enterprise) |
| `mrr_brl` | Monthly recurring revenue in BRL |
| `active_users` | Daily active users for the account |
| `product_events` | Count of product usage events |
| `support_tickets_opened` | Tickets opened |
| `support_tickets_resolved` | Tickets resolved |

## Examples

Illustrative snippets, written for this project with synthetic/generic names — not copied from any employer's or client's codebase.

**Silver model — typed, deduplicated, incremental**

```sqlx
config {
  type: "incremental",
  schema: "silver",
  name: "slv_account_billing_daily",
  bigquery: {
    partitionBy: "date",
    labels: { layer: "silver", source_system: "billing", agent_readable: "true", domain: "account" }
  },
  uniqueKey: ["date", "account_id"]
}

SELECT
  DATE(event_date) AS date,
  account_id,
  plan_tier,
  mrr_brl
FROM ${ref("bronze_billing_raw")}
WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- lookback window, see Key Decisions
${when(incremental(), `AND date >= (SELECT MAX(date) FROM ${self()})`)}
```

**Assertion — no duplicates on primary key**

```sqlx
config {
  type: "assertion",
  name: "assert_no_duplicate_account_days"
}

SELECT date, account_id, COUNT(*) AS row_count
FROM ${ref("slv_account_billing_daily")}
GROUP BY date, account_id
HAVING COUNT(*) > 1
```

An assertion that returns zero rows passes silently. Any row returned fails the Dataform run and blocks deployment — the failure mode is "nothing ships," not "bad data ships with a warning."

**Grounding contract — the label that gates agent access**

```javascript
// definitions/03_gold/account/gld_account_activity_daily.sqlx (config block)
config {
  type: "table",
  bigquery: {
    labels: {
      layer: "gold",
      agent_readable: "true",
      pipeline: "account_activity",
      domain: "account"
    }
  }
}
```

`agent_readable` is just a BigQuery label, but it's load-bearing: a semantic layer consuming this data would only list tables carrying `agent_readable: true`. Flip the label to `false` and a table disappears from what any agent can query — no code change in the agent layer required.

## Data Quality

Assertions are mapped to the five data quality dimensions that determine whether a layer is safe to expose to an LLM:

| Dimension | Assertion |
|---|---|
| **Accuracy** | Type checks, range validation on numeric fields |
| **Completeness** | Non-null on required columns |
| **Consistency** | No duplicates on primary key, standardized formats across sources |
| **Relevance** | `agent_readable` label scoped to tables that are actually useful for agent queries |
| **Representativeness** | Date range checks — no future dates, no data older than 2 years, no gaps vs. expected ingestion cadence |

Assertions run automatically on every Dataform execution and block deployment on failure — bad data never reaches a layer an agent can query.

## Stack

- **Transformation:** Dataform (SQLX)
- **Warehouse:** Google BigQuery
- **Ingestion:** BigQuery native connectors · a managed ELT connector (for sources with native support) · a custom Python connector on Cloud Run + Cloud Scheduler (for sources neither managed path covered — see Key Decisions)
- **Orchestration:** Dataform Schedules · Cloud Workflows
- **Language:** SQL · JavaScript (Dataform config) · Python (custom connector)

## Related Projects

Independent projects exploring adjacent patterns, not components of one deployed system:

- [multi-agent-analytics](https://github.com/fabricioespel-bit/multi-agent-analytics) — AI agents conversing over a data lake shaped like this one
- [marketing-dashboard](https://github.com/fabricioespel-bit/marketing-dashboard) — a performance dashboard built on a similar Gold-layer model
