# Architecture: Distributed LLM Inference Router

This document explains the design of this system — not just *what* was built, but *why* each decision was made. Every architectural choice started with a question.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Questions and Decisions](#2-design-questions-and-decisions)
3. [System Architecture](#3-system-architecture)
4. [Component Deep Dives](#4-component-deep-dives)
   - [Database Design](#41-database-design)
   - [Decision Engine](#42-decision-engine)
   - [Background Optimizer](#43-background-optimizer)
   - [Provider Layer](#44-provider-layer)
   - [Observability](#45-observability)
5. [Request Lifecycle](#5-request-lifecycle)
6. [Trade-offs and What I'd Change at Scale](#6-trade-offs-and-what-id-change-at-scale)
7. [Phase 2 Roadmap](#7-phase-2-roadmap)

---

## 1. Problem Statement

> "Build a system that routes LLM inference requests across multiple providers, optimizing for cost and latency."

That statement is deliberately vague. The first job of a systems engineer is to make it concrete before touching code. The section below documents every question asked, the answer chosen for this project, and why that answer shaped the design.

---

## 2. Design Questions and Decisions

### Scale

**Q: How many requests per second at peak?**

A: This system is designed for 15K+ requests in a load test scenario, with moderate steady-state traffic. At this scale, synchronous FastAPI request handling is sufficient — each request waits for the provider to respond before returning.

*Why it matters:* At 10 req/sec, synchronous is fine. At 10,000 req/sec, a synchronous design creates a bottleneck — the API server is blocked waiting on slow LLM providers. That's where a queue (Kafka) between ingestion and execution becomes necessary. The decision to start synchronous and add Kafka in Phase 2 is deliberate: build the simplest thing that works, then add infrastructure when the load genuinely demands it.

---

**Q: Are requests bursty or steady?**

A: Assumed bursty — traffic spikes during business hours, quiet at night.

*Why it matters:* Bursty traffic is the reason the optimizer looks at the last 500 requests rather than all-time averages. A slow provider at 2am shouldn't penalize its score at 2pm if it recovered. Recency matters more than history.

---

### Latency Requirements

**Q: Is this user-facing or background (batch)?**

A: Both. The `priority` field lets the caller declare their intent: `latency` for user-facing flows where someone is waiting, `cost` for background jobs where speed doesn't matter, `balanced` as the default.

*Why it matters:* If the answer were "always user-facing," the scoring formula would hard-code high latency weight. If it were "always batch," cost would dominate. Supporting both without changing the system is why the weights are parameterized by priority, not hardcoded.

---

**Q: What's the acceptable p95 latency?**

A: No hard SLA defined for this project, but the system tracks p95 latency per model so one could be enforced. In production, you'd add a `max_latency_ms` field to the request and filter out any model whose p95 exceeds it before scoring.

*Why it matters:* p95, not average, is the right latency metric. Average hides tail behavior — a model could have a 200ms average but 5,000ms p95, meaning 1 in 20 users waits 5 seconds. The optimizer stores and scores on p95 for exactly this reason.

---

### The Cost/Latency Tradeoff

**Q: Who decides the tradeoff — the system or the caller?**

A: The caller, via the `priority` field on every request.

*Why it matters:* Hardcoding the tradeoff in the system means every team using the router gets the same weights — which is never true in practice. A customer-facing chatbot needs low latency. A nightly report summarization job needs low cost. The `priority` field delegates that decision to whoever knows best: the calling service.

---

**Q: Is there a hard cost budget per request?**

A: Not enforced in Phase 1. The system tracks cost per request in `request_logs` and exposes running totals via Prometheus, which is the foundation for budget enforcement. A `max_cost_usd` field on the request would be a natural Phase 2 addition.

---

### Provider Requirements

**Q: Which providers must be supported, and can new ones be added without rewriting the router?**

A: OpenAI, Anthropic, and HuggingFace for Phase 1. New providers must be addable without touching the routing logic.

*Why it matters:* This is why the provider layer uses a dispatch table instead of if/else chains:

```python
# Bad — adding a 4th provider means editing routing logic
if provider == "openai":
    call_openai(...)
elif provider == "anthropic":
    call_anthropic(...)

# Good — adding a 4th provider is one dictionary entry, zero logic changes
PROVIDER_CALLERS = {
    "openai":      call_openai,
    "anthropic":   call_anthropic,
    "huggingface": call_huggingface,
}
caller = PROVIDER_CALLERS[provider]
```

The dispatch table is the Open/Closed Principle in practice: open for extension (new providers), closed for modification (routing logic doesn't change).

---

**Q: What happens if a provider goes down?**

A: Phase 1 returns a 502 error and logs `success=False` to `request_logs`. The optimizer will naturally detect the falling success rate and down-rank the provider within 60 seconds, reducing traffic to it.

*Why it matters:* This is the honest minimum. The "right" production answer is a circuit breaker — if a provider's error rate exceeds 5% in the last 2 minutes, stop routing to it immediately rather than waiting for the optimizer cycle. That's a Phase 2 addition. The groundwork is already laid: `success` and `error_message` columns in `request_logs` exist specifically to support this.

---

### Observability

**Q: Do we need to know which model handled each request after the fact?**

A: Yes. Every request is logged to `request_logs` with provider, model, latency, cost, and token counts.

*Why it matters:* Without this log, the system is a black box. You can't answer "why did latency spike at 3pm?" or "which provider is costing the most?" More importantly for this project: without `request_logs`, the background optimizer has nothing to learn from. The logging requirement is what makes adaptive routing possible.

---

**Q: How fresh does the routing data need to be?**

A: Minute-level. The optimizer runs every 60 seconds.

*Why it matters:* Real-time (per-request) updates would mean a database write and read on every single request — expensive and a potential bottleneck. Hourly updates are too stale to react to a provider degrading. 60 seconds is a practical middle ground: fast enough to respond to real incidents, cheap enough not to add overhead to every request.

---

### Statefulness

**Q: Should routing be stateless (same input → same model) or adaptive (learns from traffic)?**

A: Adaptive. The same model should not always be picked if it's been degrading in production.

*Why it matters:* Stateless routing is just a lookup table — fast but blind. It can't react to a provider getting slow, raising prices, or going partially down. Adaptive routing requires a feedback loop: requests happen → results are logged → optimizer re-ranks → future requests are routed better. That cycle is the core of this system.

---

**Q: What does failure look like, and is it acceptable?**

A: A failed provider call logs `success=False`, returns a 502 to the caller, and lets the optimizer naturally down-rank the model. This is acceptable for Phase 1 but not for production.

*Why it matters:* This is the question most people forget to ask. Production systems need automatic retry with a different provider, dead-letter queues for requests that failed all retries, and alerting when error rates cross a threshold. Each of those is a discrete engineering problem, and answering this question upfront prevents building a system that silently drops requests.

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT                               │
│              POST /route  {prompt, priority}                │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Router                           │
│                                                             │
│  1. Reads routing_policies from PostgreSQL                  │
│  2. Scores all models via Decision Engine                   │
│  3. Picks winner, calls provider                            │
│  4. Logs result to request_logs                             │
│  5. Updates Prometheus metrics                              │
│  6. Returns response with full telemetry                    │
└──────┬──────────────────────────────────────┬───────────────┘
       │                                      │
       ▼                                      ▼
┌─────────────┐                    ┌──────────────────────┐
│  Providers  │                    │     PostgreSQL        │
│             │                    │                      │
│  OpenAI     │                    │  request_logs        │
│  Anthropic  │                    │  routing_policies    │
│  HuggingFace│                    └──────────┬───────────┘
└─────────────┘                               │
                                              │ every 60s
                                              ▼
                                   ┌──────────────────────┐
                                   │  Background Optimizer │
                                   │                      │
                                   │  Reads request_logs  │
                                   │  Computes p95, avg   │
                                   │  Rewrites policies   │
                                   └──────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    Observability                            │
│                                                             │
│  GET /metrics  →  Prometheus  →  Grafana dashboards         │
│  GET /debug/scores  →  live model leaderboard               │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Component Deep Dives

### 4.1 Database Design

Two tables. The split is deliberate.

**`request_logs`** — append-only. One row per inference call. Never updated after insert.

```sql
id                UUID        primary key
provider          TEXT        "openai" | "anthropic" | "huggingface"
model             TEXT        "gpt-3.5-turbo" | "claude-haiku-4-5" | ...
prompt_tokens     INTEGER
completion_tokens INTEGER
latency_ms        FLOAT
cost_usd          FLOAT
success           BOOLEAN
error_message     TEXT        null on success
routed_at         TIMESTAMP
```

*Why append-only?* Logs are a record of what happened. Mutating them would destroy the audit trail the optimizer relies on. Every row is immutable history.

**`routing_policies`** — one row per model, rewritten every 60 seconds by the optimizer.

```sql
model               TEXT    primary key
provider            TEXT
avg_latency_ms      FLOAT   computed from request_logs
p95_latency_ms      FLOAT   computed from request_logs
cost_per_1k_tokens  FLOAT   from cost table (static)
success_rate        FLOAT   computed from request_logs
request_count       INTEGER
updated_at          TIMESTAMP
```

*Why separate tables?* The decision engine reads `routing_policies` on every single request — it needs to be fast. If the engine queried `request_logs` directly, every request would trigger a full aggregation query over potentially millions of rows. `routing_policies` is a pre-aggregated cache: the optimizer does the expensive computation on a schedule so the hot path stays cheap.

This is the read/write separation pattern: slow writes (optimizer, every 60s) feed fast reads (decision engine, every request).

---

### 4.2 Decision Engine

**The scoring formula:**

```
score = α × latency_score + β × cost_score + γ × reliability_score
```

Where:
```
latency_score     = 1 - (model_p95 / worst_p95_across_all_models)
cost_score        = 1 - (model_cost / most_expensive_model)
reliability_score = model_success_rate
```

**Why normalize to [0, 1]?** Latency is measured in milliseconds (200, 800, 2000...) and cost is measured in dollars per 1K tokens (0.0001, 0.005...). These are different units on completely different scales. Without normalization, whichever dimension has larger raw numbers would dominate the score regardless of the weights. Normalizing each dimension to [0, 1] makes the weights meaningful: α=0.6 genuinely means "I care 60% about latency."

**Why p95 latency instead of average?**

Average latency is a misleading metric for user-facing systems. A model with 200ms average could have 5,000ms p95 — meaning 1 in 20 users waits 5 seconds. p95 is what the slowest reasonable user experiences. Optimizing for p95 protects the tail, not just the median.

**Why those specific weights?**

```python
WEIGHTS = {
    "cost":     (α=0.2, β=0.6, γ=0.2),  # cost drives 60% of score
    "latency":  (α=0.6, β=0.2, γ=0.2),  # latency drives 60% of score
    "balanced": (α=0.33, β=0.33, γ=0.34) # equal weight, reliability as tiebreaker
}
```

These are intentionally simple starting points. In production, you'd A/B test different weight configurations and measure outcomes (user satisfaction, cost per session) to tune them. The architecture supports this — changing weights is a one-line config change, not a code change.

**Why include reliability at all?**

A model with great latency and great cost that fails 20% of the time is not a good model. Without a reliability term, the score would keep routing to it. The `success_rate` term ensures that a degrading provider gets down-ranked automatically as failures accumulate in `request_logs`.

---

### 4.3 Background Optimizer

**Why 60 seconds?**

Three competing concerns:
- *Freshness:* if a provider degrades, how quickly should routing react?
- *Cost:* running aggregation queries on `request_logs` isn't free
- *Stability:* updating routing policies too frequently causes oscillation (constantly switching providers)

60 seconds is a practical middle ground. Fast enough to react to real incidents within one minute. Cheap enough not to add meaningful load to PostgreSQL. Stable enough that the routing doesn't thrash between providers on every request.

**Why the last 500 requests?**

Two reasons. First, recency: a provider that was slow at 2am but fast at 2pm should be scored on the 2pm data. All-time averages would dilute recent signal with stale history. Second, a bounded window keeps the query cost predictable regardless of total request volume.

**Why require at least 5 samples before updating a model's policy?**

With fewer than 5 data points, a single outlier (one very slow request, one error) would wildly distort the score. The seed values (500ms avg, 800ms p95, 100% success) are conservative defaults that keep every model in contention while real data accumulates. Once 5 samples exist, real data takes over.

**The feedback loop:**

```
request arrives
     ↓
decision engine picks model from routing_policies
     ↓
provider call completes (or fails)
     ↓
result written to request_logs
     ↓
(every 60s) optimizer reads request_logs
     ↓
optimizer rewrites routing_policies
     ↓
next request uses updated scores
```

This is a closed-loop control system. The router's own traffic is the signal that improves future routing decisions.

---

### 4.4 Provider Layer

**Why a uniform return shape across all providers?**

```python
{
    "text":              str,
    "prompt_tokens":     int,
    "completion_tokens": int,
    "model":             str,
    "provider":          str,
}
```

Each provider's API returns completely different response shapes. OpenAI uses `response.choices[0].message.content`. Anthropic uses `response.content[0].text`. HuggingFace returns a list. If the router handled these differences inline, every provider change would require touching the routing logic.

The provider layer absorbs all that variation and hands the router a consistent dict. The router never needs to know which provider it's talking to — it just uses the dict.

**Why estimate token counts for HuggingFace?**

HuggingFace's Inference API doesn't return token usage in its response. The estimate (`len(text) / 4`) is a well-known approximation: English text averages roughly 4 characters per token. It's not exact, but it's close enough for cost estimation. In production, you'd use a tokenizer library (`tiktoken` for OpenAI models, `transformers` for others) to get exact counts.

---

### 4.5 Observability

**Four metrics and why each one:**

`llm_router_requests_total{provider, model, status}` — a counter. Answers: "how many requests went to each model?" and "what's the error rate per provider?" The `status` label (success/error) lets you compute error rate as a ratio without a separate metric.

`llm_router_latency_ms{provider, model}` — a histogram. Histograms automatically compute p50, p95, p99 from bucket counts. This is more efficient than storing raw latency values and computing percentiles at query time.

`llm_router_cost_usd_total{provider, model}` — a counter. Answers: "how much have we spent on each model?" This is the number that proves the "40% cost savings" claim in the load test.

`llm_router_routing_decisions_total{priority}` — a counter. Answers: "how often do callers use cost vs latency vs balanced priority?" Useful for understanding how the system is actually being used, which informs whether the weight defaults are right.

**Why Prometheus + Grafana over just logging?**

Logs are good for debugging individual requests. Metrics are good for understanding system behavior over time. "What was the p95 latency for Anthropic between 2pm and 3pm last Tuesday?" is a 10-second Grafana query. Answering the same question from logs requires parsing potentially millions of log lines. Both have their place — this system has both.

---

## 5. Request Lifecycle

Here is what happens, in order, when `POST /route {"prompt": "What is ML?", "priority": "cost"}` arrives:

**1. FastAPI receives the request**
Pydantic validates the body against `InferenceRequest`. If `priority` is missing, it defaults to `"balanced"`. If `prompt` is empty, it returns a 422 immediately without touching the DB.

**2. Decision engine scores models**
Queries `routing_policies` — one fast read, all models. Normalizes p95 latency and cost_per_1k across all models to [0, 1]. Applies `cost` weights (α=0.2, β=0.6, γ=0.2). The cheapest model with acceptable latency and high reliability wins. Returns `("anthropic", "claude-haiku-4-5")` — or whatever the current winner is.

**3. Provider call**
`call_provider("anthropic", "claude-haiku-4-5", "What is ML?")` dispatches to `call_anthropic`. The Anthropic SDK sends the request. The call blocks until the response arrives. `time.time()` before and after gives wall-clock latency.

**4. Cost calculation**
`estimate_cost("claude-haiku-4-5", prompt_tokens, completion_tokens)` looks up `$0.0004 / 1K tokens` from the cost table and multiplies by total tokens.

**5. Log to PostgreSQL**
One `INSERT` into `request_logs`: provider, model, tokens, latency, cost, success. This row will be read by the optimizer in the next 60-second window.

**6. Update Prometheus**
Increments `requests_total`, records latency in the histogram, increments `cost_usd_total`. No DB involved — Prometheus metrics live in memory.

**7. Response returned**
```json
{
  "request_id": "a3f2...",
  "provider": "anthropic",
  "model": "claude-haiku-4-5",
  "output": "Machine learning is...",
  "latency_ms": 312.4,
  "cost_usd": 0.0000024,
  "prompt_tokens": 12,
  "completion_tokens": 48,
  "routing_priority": "cost"
}
```

The caller can see exactly which model was chosen, why (priority), and what it cost. This transparency is intentional — it makes the system debuggable and the routing decisions auditable.

---

## 6. Trade-offs and What I'd Change at Scale

| Decision | What was built | What production needs |
|---|---|---|
| Synchronous request handling | FastAPI waits for provider response | Kafka queue + async worker pool (Phase 2) |
| 502 on provider failure | Return error to caller | Retry with next-best model automatically |
| 60s optimizer interval | Fixed schedule | Adaptive — faster when error rates spike |
| Token estimation for HuggingFace | `len(text) / 4` approximation | Exact tokenizer per model family |
| No cost budget enforcement | Track cost, don't limit it | `max_cost_usd` field on request, hard cutoff |
| No auth on the API | Open endpoints | API key middleware, per-key rate limiting |
| Single app instance | One FastAPI process | Horizontal scaling behind a load balancer |

These aren't oversights — they're deliberate Phase 1 simplifications. Every item in this table is a concrete engineering problem for Phase 2.

---

## 7. Phase 2 Roadmap

The Phase 1 system works end-to-end but handles requests synchronously. Phase 2 turns it into a genuinely distributed platform by decoupling ingestion from execution.

**What changes:**

- `POST /route` publishes to a **Kafka topic** and returns a `request_id` immediately (non-blocking)
- A **worker pool** of async consumers pulls from Kafka, calls providers, logs results
- A **Redis rate limiter** with a token bucket protects the API from burst overload
- A **circuit breaker** per provider stops routing to a failing provider within seconds, not minutes

**What stays the same:**

- The decision engine (still scores and picks models)
- The optimizer (still runs every 60s on request_logs)
- The provider layer (still the same uniform interface)
- The database schema (no changes needed)

The Phase 1 architecture was designed so Phase 2 is an extension, not a rewrite. The provider layer's uniform interface, the two-table DB split, and the optimizer's separation from the hot path were all made with Phase 2 in mind.