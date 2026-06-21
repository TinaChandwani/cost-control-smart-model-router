# Architecture: Distributed LLM Inference Router

This document explains the design of this system — not just *what* was built, but *why* each decision was made. Every architectural choice started with a question.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Questions and Decisions](#2-design-questions-and-decisions)
3. [System Architecture](#3-system-architecture)
4. [Request Lifecycle](#5-request-lifecycle)
5. [Trade-offs and What I'd Change at Scale](#6-trade-offs-and-what-id-change-at-scale)
6. [Phase 2 Roadmap](#7-phase-2-roadmap)

---

## 1. Problem Statement

> "Build a system that routes LLM inference requests across multiple providers, optimizing for cost and latency."

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
