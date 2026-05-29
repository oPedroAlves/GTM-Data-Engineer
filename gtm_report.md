# GTM Knowledge Base — Deal Intelligence System
## Technical Report

---

## 1. Approach

### 1.1 Problem framing

The GTM team lacks a unified view of customer sentiment across platforms. Gong transcripts, HubSpot notes, and Slack messages each tell part of the story, but no system connects them to the same account. The result is that sellers waste time switching between tools and still miss context.

The solution proposed here is a **Canonical Knowledge Base**: a pipeline that ingests data from all three sources, resolves their identities to a common HubSpot Account ID, and produces structured documents optimized for retrieval by an AI agent.

### 1.2 Design principles

Three principles guided every decision:

**Simplicity over completeness.** At 1,000+ Gong calls per day, the system needs to be reliable and maintainable by a small team. Every additional service (a dedicated vector database, a streaming queue, a separate embedding microservice) adds operational overhead that must be justified by a concrete need.

**SQL-native wherever possible.** The GTM team's primary data already lives in PostgreSQL. Keeping the knowledge base in the same system means no new credentials, no new monitoring, and no new cost center. Queries that combine vector similarity with structured filters (e.g., "find relevant calls for this account in Q2") are natively supported.

**LLM consumption as the output contract.** Every transformation decision — chunking strategy, metadata structure, field selection — is evaluated against one question: does this make the AI agent's responses more accurate and grounded?

---

### 1.3 Canonical Representation

All three sources are normalized into a single JSON schema before storage. This is the system's most important design decision: by decoupling source format from storage format, we can add or replace sources without changing the retrieval logic.

```json
{
  "doc_id": "uuid-v5 (deterministic from source + source_id)",
  "source": "gong | hubspot_note | hubspot_meeting | slack",
  "account_id": "hs_account_id",
  "deal_id": "string | null",
  "company_name": "string",
  "timestamp": "ISO 8601 datetime",
  "content_type": "transcript | note | meeting | message",
  "content": "clean text block — what gets embedded",
  "metadata": {
    "participants": ["email@company.com"],
    "next_steps": "string | null",
    "outcome": "string | null",
    "call_url": "string | null",
    "sentiment_signal": "positive | neutral | negative | null"
  },
  "embedding": "vector(1536)"
}
```

The `content` field is what gets embedded — it is a clean, deduplicated text block assembled from the most signal-rich fields of each source (e.g., for Gong: `spotlight_brief` + `spotlight_key_points` + the parsed transcript body). Raw JSON blobs are never embedded directly.

---

### 1.4 Identity Resolution

The most ambiguous part of the ingestion pipeline is linking a Slack message to a HubSpot Account. There is no shared key between the two systems.

The resolution strategy is **channel-based**:

```
Slack channel_id
        ↓
identity_map (channel_id → account_id)
        populated by sales ops when a deal channel is created
```

In a B2B sales environment, a single seller works on dozens of accounts simultaneously. Mapping by `user_id` would link every message a seller sends to the same account — useless in production. Mapping by `channel_id` is deterministic: a dedicated deal channel (e.g. `#deal-acme-corp`) resolves unambiguously to one HubSpot Account.

Three supported strategies (see `identity_map.json`):
1. **Dedicated deal channel** — one Slack channel per account, mapped by sales ops at deal creation (recommended)
2. **Slack Connect** — external prospects invited to a shared channel; their corporate email resolves via HubSpot Contacts API to an Account
3. **NER fallback** — for shared internal channels, company name mentions are extracted via NER/LLM and fuzzy-matched against HubSpot company records

The `channel_id → account_id` map is refreshed nightly and triggered when a new channel is created for a deal.

For Gong, the `company_id` field is used directly as the HubSpot Account ID anchor, since Gong is already configured to pull account data from HubSpot.

---

## 2. System Design

### 2.1 Architecture overview

```
[ Gong PostgreSQL ]   [ HubSpot API ]   [ Slack API ]
         │                   │                │
         └───────────────────┼────────────────┘
                             ↓
             ┌─────────────────────────────┐
             │  Ingestion & Transformation  │
             │  Python ETL · dbt · resolver │
             └─────────────────────────────┘
                             ↓
                    [ Canonical JSON ]
                             ↓
                ┌────────────────────────┐
                │  pgvector (PostgreSQL) │
                │  embeddings + metadata │
                └────────────────────────┘
                             ↓
                [ Deal Intelligence Agent ]
                             ↓
              Sales ·  Customer Success · Marketing
```

### 2.2 Storage choice: pgvector

The system uses **pgvector**, a PostgreSQL extension that adds vector similarity search to an existing database.

| Criterion | pgvector | Pinecone | Weaviate |
|---|---|---|---|
| Incremental cost | $0 (existing DB) | Per query + storage | Self-hosted infra |
| Operational complexity | None (already managed) | Low (SaaS) | High (new service) |
| SQL + filter queries | Native | Limited | Partial |
| Scale ceiling | ~10M vectors at <100ms | Unlimited | Unlimited |
| Team familiarity | High | Low | Low |
| Fit with dbt | Native | None | None |

At 1,000 calls/day with average 5 chunks per call, the system accumulates ~150,000 vectors/month. This is well within pgvector's performance envelope, and the ability to write queries like:

```sql
SELECT content, metadata
FROM knowledge_base
WHERE account_id = 'acme-corp'
  AND timestamp > NOW() - INTERVAL '30 days'
ORDER BY embedding <=> $query_embedding
LIMIT 10;
```

...without any additional infrastructure is a meaningful advantage over a standalone vector store.

**When to reconsider:** If vector count exceeds ~5M or if query latency SLAs become strict (<50ms at p99), Pinecone or Qdrant would be the next evaluation step.

### 2.3 Freshness strategy

GTM data changes hourly. A full re-index on every cycle would be cost-prohibitive and unnecessary.

The refresh strategy is **incremental by source**:

- **Gong:** Poll `call_datetime > last_run_timestamp` from the PostgreSQL source table. Only new or updated calls are re-processed. Estimated: ~50–100 calls/hour during business hours.
- **HubSpot:** Use the Associations and Batch Read APIs with a `last_modified` filter. Notes and meetings updated since the last run are fetched and re-embedded.
- **Slack:** Maintain a `last_cursor` per channel. `conversations.history` supports cursor-based pagination, so only messages since the last cursor are fetched.

Each source runs on an independent schedule via a lightweight task scheduler (Airflow or even `pg_cron` for simplicity). Failed runs are retried with exponential backoff. A `processing_log` table tracks run state and allows safe idempotent reprocessing.

### 2.4 User experience

The Deal Intelligence Agent is designed as a **conversational interface**, accessible where sellers already work:

- **Slack bot:** sellers type `/deal [company name]` or ask natural questions ("what's the latest from Acme?") and get a structured summary with source links.
- **HubSpot sidebar widget (optional):** embedded in the deal view, the agent surfaces relevant Gong clips, open questions, and sentiment signals without leaving the CRM.
- **Response contract:** every answer includes source attribution (call date, meeting title, Slack thread) to allow the seller to verify before relying on it.

This design prioritizes **trust and speed**: sellers should get a confident, cited answer in under 3 seconds, or know exactly where to look if they want to dig deeper.

---

## 3. Reliability & Scale

### 3.1 Handling 1,000+ Gong calls per day

The ingestion pipeline processes calls asynchronously. Each call is treated as an independent unit of work:

1. A scheduler queries the Gong source table for new calls every 15 minutes.
2. Each call is placed on a processing queue (implemented with a `job_queue` table in PostgreSQL or a lightweight queue like Redis).
3. Worker processes consume the queue, run the transformation, generate embeddings via the OpenAI API (or a self-hosted model), and upsert into pgvector.
4. If the embedding API rate-limits a request, the job is returned to the queue with an incremented retry counter and a backoff delay. Jobs are not dropped — they are retried up to 5 times before being flagged for manual review.
5. A dead-letter queue holds failed jobs for inspection.

This design ensures **no data is dropped** under rate limiting: the queue acts as a durable buffer between the ingestion rate and the API rate limit.

### 3.2 Reducing hallucinations

Hallucinations in RAG systems have two root causes: the retrieval step returns irrelevant context, or the LLM fabricates details not present in the context. Both are addressed:

**Retrieval quality:**
- Hybrid search (vector similarity + keyword match via `ts_vector`) ensures both semantic and exact matches are considered.
- Metadata filters (`account_id`, `date range`) scope retrieval to relevant documents before ranking.
- A relevance threshold filters out low-similarity results rather than passing noise to the LLM.

**Generation quality:**
- The system prompt instructs the agent to answer only from provided context and to cite sources explicitly.
- If no relevant context is found, the agent responds with "I don't have enough data on this account" rather than generating a plausible-sounding but fabricated answer.
- Answer length is kept short and structured (bullet points with sources) to reduce the surface area for hallucination.

### 3.3 Agent interface for sellers and sales leaders

**Sellers** interact primarily through the Slack bot and HubSpot sidebar. They ask account-specific questions and get synthesized answers with source links. The agent is designed to feel like asking a knowledgeable colleague, not running a database query.

**Sales leaders** interact through a dashboard view that aggregates sentiment signals and deal risk indicators across the portfolio. Queries like "which deals have had no customer contact in 30 days?" or "what objections are coming up most in enterprise calls this quarter?" are served by the same retrieval layer but with broader scope.

Both interfaces share the same underlying agent and knowledge base — the difference is the query scope and the output format, not the infrastructure.

---

*Sections 3 (Transformation Script) and 4 (Clean JSON output) follow as separate files.*
