# GTM Knowledge Base — Deal Intelligence System
### Case Study: Ingestion & Transformation Engine

---

## Overview

This submission implements the **Ingestion & Transformation Engine** for a GTM Knowledge Base that indexes Gong call transcripts, HubSpot notes and meetings, and Slack messages into a unified vector store to power an internal Deal Intelligence AI agent.

**Stack:** Python · PostgreSQL · pgvector · dbt · OpenAI Embeddings

---

## Repository Structure

```
├── transform.py              # Main ETL script (core deliverable)
├── canonical_output.json     # Clean output — 8 canonical documents
├── gtm_report.md             # Full technical report (approach + design)
└── fixtures/
    ├── gong_calls.json       # Gong call records (mock)
    ├── hubspot_data.json     # HubSpot notes & meetings (mock)
    ├── slack_messages.json   # Slack messages (mock)
    └── identity_map.json     # Slack user → HubSpot account map (mock)
```

---

## Quickstart

```bash
# Install dependencies (no external packages needed beyond stdlib)
python --version  # requires Python 3.9+

# Run the transformation pipeline
python transform.py
```

**Expected output:**
```
GTM Knowledge Base — Ingestion Pipeline
========================================
  Gong:     2 documents
  HubSpot:  4 documents
  Slack:    2 documents

✓ 8 canonical documents → canonical_output.json
```

---

## Architecture

```
[ Gong PostgreSQL ]   [ HubSpot API ]   [ Slack API ]
         │                   │                │
         └───────────────────┼────────────────┘
                             ↓
             ┌─────────────────────────────────┐
             │   Ingestion & Transformation    │
             │   Python ETL · dbt · resolver   │
             └─────────────────────────────────┘
                             ↓
                    [ Canonical JSON ]
                             ↓
               ┌──────────────────────────┐
               │   pgvector (PostgreSQL)  │
               │   embeddings + metadata  │
               └──────────────────────────┘
                             ↓
               [ Deal Intelligence Agent ]
                             ↓
             Sales · Customer Success · Marketing
```

**Why pgvector over Pinecone or Weaviate:**
The Gong source already lives in PostgreSQL. pgvector adds vector similarity search to an existing database at zero incremental cost, supports native SQL filters (`WHERE account_id = '...'`), and requires no new infrastructure. At the expected volume (~150K vectors/month), it comfortably stays within pgvector's performance envelope. See `gtm_report.md` §2.2 for the full trade-off comparison.

---

## Canonical Schema

Every document — regardless of source — is normalized to this schema before storage:

```json
{
  "doc_id":       "uuid-v5 (deterministic from source + source_id)",
  "source":       "gong | hubspot_note | hubspot_meeting | slack",
  "account_id":   "HubSpot Account ID — master key for cross-source linking",
  "deal_id":      "HubSpot Deal ID | null",
  "company_name": "string",
  "timestamp":    "ISO 8601",
  "content_type": "transcript | note | meeting | message",
  "content":      "clean text — this field gets embedded",
  "metadata": {
    "participants":     ["email@company.com"],
    "next_steps":       "string | null",
    "outcome":          "string | null",
    "sentiment_signal": "positive | neutral | negative | null",
    "source_url":       "string | null"
  }
}
```

The `content` field is assembled from the most signal-rich fields per source:
- **Gong:** `spotlight_brief` + `spotlight_key_points` + `spotlight_next_steps` + `spotlight_outcome`
- **HubSpot notes:** `hs_note_body` (HTML stripped)
- **HubSpot meetings:** `hs_meeting_title` + `hs_meeting_body` + `hs_meeting_outcome`
- **Slack:** thread messages joined in chronological order

---

## Identity Resolution

The hardest problem in the pipeline is linking a Slack message to a HubSpot Account. There is no shared key between the two systems. The resolution strategy is **channel-based**:

```
Slack channel_id
  → identity_map.json (channel_id → account_id)
     populated by sales ops when a deal channel is created
```

In a B2B environment a single seller works on dozens of accounts simultaneously, so mapping by `user_id` would ambiguously link all of a seller's messages to one account. A dedicated deal channel (e.g. `#deal-acme-corp`) resolves deterministically to a single HubSpot Account.

Three supported strategies are documented in `identity_map.json`:
1. **Dedicated deal channel** — one channel per account, mapped at deal creation (recommended)
2. **Slack Connect** — prospect's corporate email resolves via HubSpot Contacts API
3. **NER fallback** — company name extraction via NER/LLM for shared internal channels

The map is refreshed nightly and on new channel creation.

---

## Script Architecture (`transform.py`)

| Class | Responsibility |
|---|---|
| `CanonicalDocument` | Dataclass defining the output schema |
| `IdentityResolver` | Loads and queries the Slack → HubSpot identity map |
| `GongTransformer` | Transforms Gong call records → CanonicalDocument |
| `HubSpotTransformer` | Transforms HubSpot notes and meetings → CanonicalDocument |
| `SlackTransformer` | Groups Slack threads and transforms → CanonicalDocument |
| `IngestionPipeline` | Orchestrates all transformers, writes output |

**Key design decisions in the code:**
- `doc_id` uses `uuid.uuid5` (deterministic) — same source record always produces the same ID, enabling safe upserts
- Gong spotlight fields are preferred over raw transcript for embedding — they are more information-dense and have less noise
- Slack messages are grouped by `thread_ts` — a thread has more semantic coherence than an individual message
- `content` assembly is separated from schema construction — each transformer has a dedicated content assembly function that can be modified independently

---

## Reliability & Scale

**Handling 1,000+ Gong calls/day:**

```
Scheduler (every 15 min)
  → Query Gong table WHERE call_datetime > last_run_ts
  → Enqueue new calls in job_queue table
  → Workers consume queue → transform → embed → upsert to pgvector
  → Rate limit hit → job returns to queue with backoff (max 5 retries)
  → Exhausted retries → dead-letter queue for manual review
```

No data is dropped under rate limiting. The queue acts as a durable buffer between ingestion rate and API rate limits.

**Reducing hallucinations:**
- Hybrid search: vector similarity + `ts_vector` keyword match
- Metadata pre-filtering: `account_id` + date range scopes retrieval before ranking
- Relevance threshold: low-similarity results are excluded, not passed to the LLM
- System prompt instructs the agent to answer only from provided context and cite sources explicitly
- If no relevant context is found, the agent responds with "I don't have enough data" — no fabrication

---

## Production Extension Points

The script is annotated with `# TODO` comments marking the three main extension points:

```python
# TODO: Replace fixture reads with live PostgreSQL queries
# TODO: Add embedding generation (OpenAI text-embedding-3-small)
# TODO: Add pgvector upsert (INSERT ... ON CONFLICT (doc_id) DO UPDATE)
```

The pgvector target table schema:

```sql
CREATE TABLE knowledge_base (
    doc_id        UUID PRIMARY KEY,
    source        TEXT NOT NULL,
    account_id    TEXT NOT NULL,
    deal_id       TEXT,
    company_name  TEXT,
    timestamp     TIMESTAMPTZ,
    content_type  TEXT,
    content       TEXT,
    metadata      JSONB,
    embedding     vector(1536),
    -- Generated column for hybrid search (vector + full-text keyword match)
    fts_document  tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- HNSW index: preferred over legacy ivfflat since pgvector v0.5.0.
-- Advantages: no pre-training step required, grows dynamically,
-- better recall and lower latency under concurrent load.
CREATE INDEX ON knowledge_base USING hnsw (embedding vector_cosine_ops);

-- GIN index for full-text search (hybrid retrieval)
CREATE INDEX ON knowledge_base USING gin (fts_document);

-- Composite index for pre-filtering by account + recency
CREATE INDEX ON knowledge_base (account_id, timestamp DESC);
```

---

## Full Technical Report

See [`gtm_report.md`](./gtm_report.md) for:
- Approach and design principles
- Architecture trade-off analysis (pgvector vs Pinecone vs Weaviate)
- Hourly refresh strategy
- User experience design (Slack bot + HubSpot sidebar)
- Reliability and scale details
- Anti-hallucination strategy
