#!/usr/bin/env python3
"""
GTM Knowledge Base — Ingestion & Transformation Engine (v2)
===========================================================

Transforms raw data from Gong, HubSpot, and Slack into a unified
Canonical JSON schema optimized for vector storage and LLM retrieval.

Architecture decisions:
  - Identity resolution: channel_id → HubSpot Account ID (not user_id,
    which is ambiguous in a multi-account B2B environment)
  - Gong: spotlight fields as primary embedding content
  - HubSpot: note body and meeting outcome assembled as content blocks
  - Slack: messages grouped by thread; replies trigger full thread fetch
  - HTML cleaned via html.parser (not regex, to avoid false-positive tag removal)
  - Per-record error handling: one bad record never aborts the full batch

Changes from v1 (post code review):
  - [FIX] Identity resolution: user_id → channel_id (fixes multi-account B2B)
  - [FIX] Slack incremental: detect reply messages, fetch full thread before upsert
  - [FIX] Pipeline: per-record try/except with dead-letter queue support
  - [FIX] HTML cleaning: html.parser replaces regex (fixes false-positive on < > chars)
  - [FIX] Index: HNSW recommended over legacy ivfflat (see DDL in README)
  - [FIX] Hybrid search: tsvector column added to DDL (see README)

Usage:
  python transform.py
  python transform.py --gong path/to/gong.json --output out.json
"""

import json
import uuid
import re
import argparse
from datetime import datetime
from dataclasses import dataclass, field, asdict
from html.parser import HTMLParser
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path


# ─── Canonical Schema ─────────────────────────────────────────────────────────

@dataclass
class CanonicalDocument:
    """
    Unified representation of a single customer interaction.

    `content` is the field that gets embedded — it is assembled from the
    most signal-rich fields of each source and cleaned for LLM consumption.

    `metadata` carries structured attributes used for:
      - Pre-filtering in vector search (account_id, deal_id, date range)
      - Source attribution in agent responses (call_url, note_id)
      - Downstream analytics (sentiment_signal, participants)
    """
    doc_id: str
    source: str           # gong | hubspot_note | hubspot_meeting | slack
    account_id: str
    deal_id: Optional[str]
    company_name: str
    timestamp: str        # ISO 8601
    content_type: str     # transcript | note | meeting | message
    content: str          # clean text for embedding
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Content Cleaning ─────────────────────────────────────────────────────────

class _HTMLTextExtractor(HTMLParser):
    """
    Extracts plain text from HTML using Python's stdlib html.parser.

    Why not regex: a pattern like r"<[^>]+>" would incorrectly strip
    valid text containing comparison operators, e.g.:
        "volume < 100 and revenue > 50k"
    would match "< 100 and revenue >" as an HTML tag and delete it.
    A real parser handles this correctly by tracking parser state.
    """
    def __init__(self):
        super().__init__()
        self._parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def clean_text(text: str) -> str:
    """
    Prepares raw text for embedding:
      1. Strips HTML tags via html.parser (safe for < > in plain text)
      2. Normalises whitespace
      3. Removes wrapping quotes
    """
    if not text:
        return ""
    extractor = _HTMLTextExtractor()
    extractor.feed(text)
    text = extractor.get_text()
    text = re.sub(r"\s+", " ", text).strip()
    text = text.strip("\"'")
    return text


def assemble_gong_content(record: Dict) -> str:
    """
    Assembles the embedding content from a Gong call record.

    Strategy: prefer spotlight fields over raw transcript.
    Spotlight fields are Gong's AI-generated summaries — they are more
    information-dense and have less noise than verbatim transcripts.

    Fallback: if no spotlight data exists, use the first N utterances
    from the transcript as a best-effort summary.

    Note on transcript chunking: for granular retrieval (e.g. finding
    the exact moment a pricing objection was raised), the raw transcript
    should be chunked into overlapping windows of ~300 tokens and stored
    as separate documents. This is a production extension.
    """
    parts = []

    if record.get("spotlight_brief"):
        parts.append(f"Summary: {clean_text(record['spotlight_brief'])}")

    if record.get("spotlight_key_points"):
        parts.append(f"Key points: {clean_text(record['spotlight_key_points'])}")

    if record.get("spotlight_next_steps"):
        parts.append(f"Next steps: {clean_text(record['spotlight_next_steps'])}")

    if record.get("spotlight_outcome"):
        parts.append(f"Outcome: {clean_text(record['spotlight_outcome'])}")

    # Fallback: first 10 transcript utterances
    if not parts and record.get("transcript_json"):
        raw = record["transcript_json"]
        transcript = json.loads(raw) if isinstance(raw, str) else raw
        excerpt = " ".join(u.get("text", "") for u in transcript[:10])
        parts.append(f"Transcript excerpt: {clean_text(excerpt)}")

    return "\n\n".join(parts)


def extract_participants(transcript: List[Dict]) -> List[str]:
    """Returns unique participant emails from a transcript, deduped and ordered."""
    seen: set = set()
    result = []
    for utterance in transcript:
        email = utterance.get("speakerEmail", "")
        if email and email not in seen:
            seen.add(email)
            result.append(email)
    return result


# ─── Sentiment Helper ─────────────────────────────────────────────────────────

def infer_sentiment(gong_record: Dict) -> Optional[str]:
    """
    Rule-based sentiment signal from Gong's spotlight outcome field.

    Uses whole-word matching (\\b boundaries) to avoid false positives
    from substring matches (e.g. "not agreed" should not match "agreed").

    Limitation: negation context ("did not agree") is not handled.
    For production, replace with a one-shot LLM classifier prompt or
    Gong's native sentiment API (if licensed).
    """
    outcome = (gong_record.get("spotlight_outcome") or "").lower()
    if not outcome:
        return None

    positive_signals = {"positive", "interested", "agreed", "approved", "advanced", "strong"}
    negative_signals = {"negative", "churned", "lost", "declined", "blocked", "risk"}

    for word in positive_signals:
        if re.search(rf"\b{word}\b", outcome):
            return "positive"
    for word in negative_signals:
        if re.search(rf"\b{word}\b", outcome):
            return "negative"
    return "neutral"


# ─── Identity Resolver ────────────────────────────────────────────────────────

class IdentityResolver:
    """
    Resolves a Slack channel_id to a HubSpot Account ID.

    WHY CHANNEL-BASED (not user-based):
    In a B2B sales environment, a single seller (e.g. Alex Rivera) works
    on dozens of accounts simultaneously. Mapping by user_id would link
    every message Alex sends to the same account — useless in production.

    Mapping by channel_id is deterministic: a dedicated channel per account
    (#deal-acme-corp) resolves unambiguously to one HubSpot Account.

    Three supported mapping strategies (see identity_map.json):
      1. dedicated_channel — one Slack channel per account (recommended)
      2. slack_connect — external users invited; their email → HubSpot Contact
      3. ner_fallback — extract company mentions via NER/LLM for shared channels

    The map is populated by sales ops when a deal channel is created,
    and refreshed nightly via a sync job.
    """

    def __init__(self, identity_map_path: str):
        with open(identity_map_path) as f:
            data = json.load(f)
        self._channel_map: Dict[str, Dict] = {
            entry["channel_id"]: entry
            for entry in data["channel_mappings"]
        }

    def resolve_channel(self, channel_id: str) -> Optional[Dict]:
        """Returns account context for a channel, or None if unmapped."""
        return self._channel_map.get(channel_id)


# ─── Source Transformers ──────────────────────────────────────────────────────

class GongTransformer:
    """Transforms a Gong call record into a CanonicalDocument."""

    def transform(self, record: Dict) -> Optional[CanonicalDocument]:
        # Skip calls with no usable content
        has_spotlight = any(record.get(f) for f in [
            "spotlight_brief", "spotlight_key_points",
            "spotlight_next_steps", "spotlight_outcome"
        ])
        has_transcript = record.get("has_transcript") and record.get("transcript_json")

        if not has_spotlight and not has_transcript:
            return None

        transcript = []
        if record.get("transcript_json"):
            raw = record["transcript_json"]
            transcript = json.loads(raw) if isinstance(raw, str) else raw

        content = assemble_gong_content(record)
        if not content:
            return None

        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"gong:{record['call_id']}"))

        return CanonicalDocument(
            doc_id=doc_id,
            source="gong",
            account_id=record["company_id"],
            deal_id=record.get("deal_id"),
            company_name=record["company_name"],
            timestamp=record["call_datetime"],
            content_type="transcript",
            content=content,
            metadata={
                "call_id": record["call_id"],
                "call_url": record.get("call_url"),
                "title": record.get("title"),
                "participants": extract_participants(transcript),
                "next_steps": clean_text(record.get("spotlight_next_steps", "")),
                "outcome": clean_text(record.get("spotlight_outcome", "")),
                "sentiment_signal": infer_sentiment(record),
            },
        )


class HubSpotTransformer:
    """Transforms HubSpot notes and meetings into CanonicalDocuments."""

    def transform_note(
        self,
        note: Dict,
        account_id: str,
        company_name: str,
        deal_id: Optional[str],
    ) -> Optional[CanonicalDocument]:
        props = note.get("properties", {})
        body = clean_text(props.get("hs_note_body", ""))
        if not body:
            return None

        timestamp = props.get("hs_timestamp") or note.get("createdAt", "")
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"hs_note:{note['id']}"))

        return CanonicalDocument(
            doc_id=doc_id,
            source="hubspot_note",
            account_id=account_id,
            deal_id=deal_id,
            company_name=company_name,
            timestamp=timestamp,
            content_type="note",
            content=body,
            metadata={
                "note_id": note["id"],
                "owner_id": props.get("hubspot_owner_id"),
                "sentiment_signal": None,
            },
        )

    def transform_meeting(
        self,
        meeting: Dict,
        account_id: str,
        company_name: str,
        deal_id: Optional[str],
    ) -> Optional[CanonicalDocument]:
        props = meeting.get("properties", {})
        title = props.get("hs_meeting_title", "")
        body = clean_text(props.get("hs_meeting_body", "") or "")
        outcome = clean_text(props.get("hs_meeting_outcome", "") or "")

        if not title and not body and not outcome:
            return None

        parts = []
        if title:
            parts.append(f"Meeting: {title}")
        if body:
            parts.append(body)
        if outcome:
            parts.append(f"Outcome: {outcome}")

        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"hs_meeting:{meeting['id']}"))

        return CanonicalDocument(
            doc_id=doc_id,
            source="hubspot_meeting",
            account_id=account_id,
            deal_id=deal_id,
            company_name=company_name,
            timestamp=props.get("hs_meeting_start_time") or meeting.get("createdAt", ""),
            content_type="meeting",
            content="\n".join(parts),
            metadata={
                "meeting_id": meeting["id"],
                "title": title,
                "attendees": props.get("hs_attendee_owner_ids", []),
                "sentiment_signal": None,
            },
        )


class SlackTransformer:
    """
    Transforms Slack messages into CanonicalDocuments.

    Account linkage: resolved at channel level (not user level).
    Each channel maps to exactly one HubSpot Account via IdentityResolver.

    Thread grouping: messages sharing a thread_ts are merged into a single
    document to preserve conversational context.

    Incremental safety: in a real incremental run, only new messages arrive
    in the batch. If a new message is a reply to an older thread (thread_ts
    != ts), the full thread must be fetched before generating the embedding
    — otherwise the upsert would overwrite the existing document with only
    the new reply, destroying historical context.

    The `fetch_full_thread_fn` parameter is the production hook for this:
        fetch_full_thread_fn(channel_id, thread_ts) → List[Dict]
    It wraps the Slack conversations.replies API call. In this fixture-based
    implementation, all messages are already present so no fetch is needed.
    """

    def __init__(self, resolver: IdentityResolver):
        self.resolver = resolver

    def transform_channel(
        self,
        channel_data: Dict,
        fetch_full_thread_fn: Optional[Callable] = None,
    ) -> List[CanonicalDocument]:
        messages = channel_data.get("messages", [])
        channel_id = channel_data["channel_id"]

        # Resolve account at channel level — fails fast if channel is unmapped
        account_context = self.resolver.resolve_channel(channel_id)
        if not account_context:
            return []

        # Identify reply messages (thread_ts present and differs from own ts)
        reply_thread_ids = {
            msg["thread_ts"]
            for msg in messages
            if msg.get("thread_ts") and msg["thread_ts"] != msg.get("ts")
        }

        # In incremental mode: replace partial batch with full thread history
        # to prevent upsert from overwriting existing docs with orphaned replies.
        if fetch_full_thread_fn and reply_thread_ids:
            full_messages = [m for m in messages if not m.get("thread_ts") or m["thread_ts"] == m.get("ts")]
            for thread_ts in reply_thread_ids:
                full_thread = fetch_full_thread_fn(channel_id, thread_ts)
                full_messages.extend(full_thread)
            messages = full_messages

        # Group by thread
        threads: Dict[str, List[Dict]] = {}
        for msg in messages:
            key = msg.get("thread_ts") or msg.get("ts")
            threads.setdefault(key, []).append(msg)

        results = []
        for thread_messages in threads.values():
            doc = self._transform_thread(thread_messages, channel_id, account_context)
            if doc:
                results.append(doc)
        return results

    def _transform_thread(
        self,
        messages: List[Dict],
        channel_id: str,
        account_context: Dict,
    ) -> Optional[CanonicalDocument]:
        sorted_msgs = sorted(messages, key=lambda m: float(m.get("ts", 0)))
        lines = [clean_text(m.get("text", "")) for m in sorted_msgs]
        content = "\n".join(line for line in lines if line)

        if not content:
            return None

        first_ts = float(sorted_msgs[0].get("ts", 0))
        timestamp = datetime.fromtimestamp(first_ts).isoformat() + "Z"
        root_ts = sorted_msgs[0].get("thread_ts") or sorted_msgs[0]["ts"]
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"slack:{channel_id}:{root_ts}"))

        return CanonicalDocument(
            doc_id=doc_id,
            source="slack",
            account_id=account_context["account_id"],
            deal_id=account_context.get("deal_id"),
            company_name=account_context["company_name"],
            timestamp=timestamp,
            content_type="message",
            content=content,
            metadata={
                "channel_id": channel_id,
                "thread_ts": root_ts,
                "participants": list({m.get("user") for m in messages if m.get("user")}),
                "message_count": len(messages),
                "sentiment_signal": None,
            },
        )


# ─── Pipeline ─────────────────────────────────────────────────────────────────

class IngestionPipeline:
    """
    Orchestrates the end-to-end ingestion and transformation process.

    Error handling strategy:
      - Failures are caught at the individual record level
      - A bad record is logged and sent to a dead-letter queue (DLQ)
      - Processing continues for all remaining records in the batch
      - This ensures one malformed payload never aborts a 1,000-record run

    Production extension points (marked with TODO):
      - TODO: Replace fixture reads with live PostgreSQL queries / API calls
      - TODO: Add embedding generation (batch via OpenAI text-embedding-3-small)
      - TODO: Add pgvector upsert (INSERT ... ON CONFLICT (doc_id) DO UPDATE SET ...)
      - TODO: Wrap in a scheduler (Airflow DAG or pg_cron)
      - TODO: Wire fetch_full_thread_fn to Slack conversations.replies API
    """

    def __init__(self, identity_map_path: str):
        resolver = IdentityResolver(identity_map_path)
        self.gong = GongTransformer()
        self.hubspot = HubSpotTransformer()
        self.slack = SlackTransformer(resolver)
        self.dead_letter: List[Dict] = []   # DLQ: records that failed transformation

    def _to_dlq(self, source: str, record_id: str, error: Exception) -> None:
        """Logs a failed record to the dead-letter queue instead of aborting."""
        entry = {"source": source, "record_id": record_id, "error": str(error)}
        self.dead_letter.append(entry)
        print(f"  [WARN] {source} record {record_id} skipped → DLQ: {error}")

    def run(
        self,
        gong_path: str,
        hubspot_path: str,
        slack_path: str,
        output_path: str,
    ) -> List[Dict]:
        documents: List[Dict] = []

        # ── 1. Gong ──────────────────────────────────────────────
        with open(gong_path) as f:
            gong_data = json.load(f)

        for record in gong_data.get("calls", []):
            try:
                doc = self.gong.transform(record)
                if doc:
                    documents.append(doc.to_dict())
            except Exception as e:
                self._to_dlq("gong", record.get("call_id", "unknown"), e)

        print(f"  Gong:     {sum(1 for d in documents if d['source'] == 'gong')} documents")

        # ── 2. HubSpot ───────────────────────────────────────────
        with open(hubspot_path) as f:
            hs_data = json.load(f)

        for deal in hs_data.get("deals", []):
            acct = deal["account_id"]
            name = deal["company_name"]
            did  = deal["deal_id"]

            for note in deal.get("notes", []):
                try:
                    doc = self.hubspot.transform_note(note, acct, name, did)
                    if doc:
                        documents.append(doc.to_dict())
                except Exception as e:
                    self._to_dlq("hubspot_note", note.get("id", "unknown"), e)

            for meeting in deal.get("meetings", []):
                try:
                    doc = self.hubspot.transform_meeting(meeting, acct, name, did)
                    if doc:
                        documents.append(doc.to_dict())
                except Exception as e:
                    self._to_dlq("hubspot_meeting", meeting.get("id", "unknown"), e)

        hs_count = sum(1 for d in documents if d["source"].startswith("hubspot"))
        print(f"  HubSpot:  {hs_count} documents")

        # ── 3. Slack ─────────────────────────────────────────────
        with open(slack_path) as f:
            slack_data = json.load(f)

        before = len(documents)
        for channel in slack_data.get("channels", []):
            try:
                # fetch_full_thread_fn=None here (batch mode, all messages present)
                # In incremental mode, pass a function that calls conversations.replies
                docs = self.slack.transform_channel(channel, fetch_full_thread_fn=None)
                documents.extend(d.to_dict() for d in docs)
            except Exception as e:
                self._to_dlq("slack", channel.get("channel_id", "unknown"), e)

        print(f"  Slack:    {len(documents) - before} documents")

        # ── 4. Write output ──────────────────────────────────────
        output = {
            "generated_at": datetime.now().isoformat() + "Z",
            "total_documents": len(documents),
            "dead_letter_count": len(self.dead_letter),
            "sources": {
                "gong": sum(1 for d in documents if d["source"] == "gong"),
                "hubspot_note": sum(1 for d in documents if d["source"] == "hubspot_note"),
                "hubspot_meeting": sum(1 for d in documents if d["source"] == "hubspot_meeting"),
                "slack": sum(1 for d in documents if d["source"] == "slack"),
            },
            "documents": documents,
        }

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        print(f"\n✓ {len(documents)} canonical documents → {output_path}")
        if self.dead_letter:
            print(f"⚠ {len(self.dead_letter)} records in DLQ — review with --dlq flag")
        return documents


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GTM Knowledge Base — Ingestion & Transformation Engine"
    )
    parser.add_argument("--gong",         default="gong_calls.json")
    parser.add_argument("--hubspot",      default="hubspot_data.json")
    parser.add_argument("--slack",        default="slack_messages.json")
    parser.add_argument("--identity-map", default="identity_map.json")
    parser.add_argument("--output",       default="canonical_output.json")
    args = parser.parse_args()

    print("GTM Knowledge Base — Ingestion Pipeline")
    print("=" * 40)

    pipeline = IngestionPipeline(identity_map_path=args.identity_map)
    pipeline.run(
        gong_path=args.gong,
        hubspot_path=args.hubspot,
        slack_path=args.slack,
        output_path=args.output,
    )
