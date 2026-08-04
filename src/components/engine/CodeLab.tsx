import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";

const SNIPPETS: { id: string; label: string; lang: string; code: string }[] = [
  {
    id: "schema",
    label: "Sharded ledger schema",
    lang: "sql",
    code: String.raw`-- Double-entry ledger. Shard key = account_id so a transfer touches
-- at most two shards; same-shard transfers stay single-node ACID.
CREATE TYPE txn_state AS ENUM ('pending','posted','failed','reversed');

CREATE TABLE accounts (
  id            uuid PRIMARY KEY,
  currency      char(3)  NOT NULL,
  balance_minor bigint   NOT NULL DEFAULT 0 CHECK (balance_minor >= 0),
  version       bigint   NOT NULL DEFAULT 0,   -- optimistic concurrency token
  frozen        boolean  NOT NULL DEFAULT false,
  updated_at    timestamptz NOT NULL DEFAULT now()
) PARTITION BY HASH (id);
-- 16 logical partitions per physical shard: resharding = move partitions.
DO $$ BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format(
      'CREATE TABLE accounts_p%s PARTITION OF accounts
         FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
  END LOOP;
END $$;

CREATE TABLE transactions (
  id              uuid PRIMARY KEY,
  idempotency_key text NOT NULL,
  debit_account   uuid NOT NULL,
  credit_account  uuid NOT NULL,
  amount_minor    bigint NOT NULL CHECK (amount_minor > 0),
  currency        char(3) NOT NULL,
  state           txn_state NOT NULL DEFAULT 'pending',
  created_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT no_self_transfer CHECK (debit_account <> credit_account)
);
-- Durable idempotency: the DB, not the cache, is the source of truth.
CREATE UNIQUE INDEX transactions_idem ON transactions (idempotency_key);

CREATE TABLE ledger_entries (
  id           bigserial PRIMARY KEY,
  txn_id       uuid NOT NULL REFERENCES transactions(id),
  account_id   uuid NOT NULL,
  direction    smallint NOT NULL CHECK (direction IN (-1, 1)),
  amount_minor bigint NOT NULL,
  balance_after bigint NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_by_account ON ledger_entries (account_id, id DESC);

-- Transactional outbox: events commit atomically with the ledger write,
-- a relay tails it into Kafka (exactly-once effect, at-least-once delivery).
CREATE TABLE outbox (
  id         bigserial PRIMARY KEY,
  topic      text NOT NULL,
  key        text NOT NULL,
  payload    jsonb NOT NULL,
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;`,
  },
  {
    id: "transfer",
    label: "ACID transfer + idempotency",
    lang: "typescript",
    code: String.raw`import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { withShard } from "@/lib/shard.server";

const TransferInput = z.object({
  idempotencyKey: z.string().min(16),
  fromAccount: z.string().uuid(),
  toAccount: z.string().uuid(),
  amountMinor: z.number().int().positive(),
  currency: z.string().length(3),
});

export const transfer = createServerFn({ method: "POST" })
  .inputValidator((d) => TransferInput.parse(d))
  .handler(async ({ data }) => {
    // 1. Fast idempotency probe: Redis SET NX guards the hot retry path.
    const claimed = await redis.set(
      "idem:" + data.idempotencyKey, "in-flight",
      { NX: true, EX: 86_400 },
    );
    if (!claimed) {
      const prior = await loadByIdempotencyKey(data.idempotencyKey);
      if (prior) return prior;                 // replay -> same response
      throw httpError(409, "duplicate_in_flight");
    }

    // 2. Single ACID transaction on the debit shard, REPEATABLE READ.
    return withShard(data.fromAccount, async (tx) => {
      await tx.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

      // Deterministic lock order kills deadlocks between crossing transfers.
      const [a, b] = [data.fromAccount, data.toAccount].sort();
      const rows = await tx.query(
        "SELECT id, balance_minor, version, frozen, currency FROM accounts \
         WHERE id IN ($1,$2) ORDER BY id FOR UPDATE", [a, b],
      );
      const from = rows.find((r) => r.id === data.fromAccount);
      const to = rows.find((r) => r.id === data.toAccount);
      if (!from || !to) throw httpError(404, "account_not_found");
      if (from.frozen || to.frozen) throw httpError(423, "account_frozen");
      if (from.currency !== data.currency) throw httpError(422, "currency_mismatch");
      if (from.balance_minor < data.amountMinor) throw httpError(422, "insufficient_funds");

      const txnId = crypto.randomUUID();
      // ON CONFLICT DO NOTHING makes the write itself idempotent even if
      // Redis was flushed between the probe and the commit.
      const inserted = await tx.query(
        "INSERT INTO transactions (id, idempotency_key, debit_account, credit_account, \
           amount_minor, currency, state) \
         VALUES ($1,$2,$3,$4,$5,$6,'posted') \
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        [txnId, data.idempotencyKey, data.fromAccount, data.toAccount,
         data.amountMinor, data.currency],
      );
      if (inserted.length === 0) return loadByIdempotencyKey(data.idempotencyKey);

      // Optimistic version check: concurrent writer -> 0 rows -> retry.
      const debited = await tx.query(
        "UPDATE accounts SET balance_minor = balance_minor - $2, version = version + 1, \
           updated_at = now() WHERE id = $1 AND version = $3 AND balance_minor >= $2 \
         RETURNING balance_minor", [from.id, data.amountMinor, from.version],
      );
      if (debited.length === 0) throw httpError(409, "concurrent_modification");
      const credited = await tx.query(
        "UPDATE accounts SET balance_minor = balance_minor + $2, version = version + 1 \
         WHERE id = $1 AND version = $3 RETURNING balance_minor",
        [to.id, data.amountMinor, to.version],
      );
      if (credited.length === 0) throw httpError(409, "concurrent_modification");

      await tx.query(
        "INSERT INTO ledger_entries (txn_id, account_id, direction, amount_minor, balance_after) \
         VALUES ($1,$2,-1,$4,$6), ($1,$3,1,$4,$7)",
        [txnId, from.id, to.id, data.amountMinor, null,
         debited[0].balance_minor, credited[0].balance_minor],
      );

      // Outbox write shares the commit: no dual-write inconsistency.
      await tx.query(
        "INSERT INTO outbox (topic, key, payload) VALUES ('payments.events', $1, $2)",
        [from.id, JSON.stringify({ type: "transfer.posted", txnId, ...data })],
      );

      const result = { txnId, state: "posted", balanceMinor: debited[0].balance_minor };
      await redis.set("idem:" + data.idempotencyKey, JSON.stringify(result), { EX: 86_400 });
      return result;
    }, { retries: 3, backoffMs: [5, 25, 90] });   // serialization failures retry
  });`,
  },
  {
    id: "kafka",
    label: "Outbox relay + Kafka consumer",
    lang: "typescript",
    code: String.raw`// Relay: tails the outbox and publishes in key order (per-account ordering).
export async function relayOnce(shard: Shard) {
  const batch = await shard.query(
    "SELECT id, topic, key, payload FROM outbox WHERE published_at IS NULL \
     ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED",
  );
  if (!batch.length) return 0;
  await producer.sendBatch({
    acks: -1, compression: 1,                 // all in-sync replicas, gzip
    topicMessages: [{
      topic: "payments.events",
      messages: batch.map((r) => ({ key: r.key, value: JSON.stringify(r.payload) })),
    }],
  });
  await shard.query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)",
    [batch.map((r) => r.id)]);
  return batch.length;
}

// Consumer: at-least-once delivery, made effectively-once by a dedupe table.
await consumer.subscribe({ topic: "payments.events", fromBeginning: false });
await consumer.run({
  partitionsConsumedConcurrently: 8,
  eachBatchAutoResolve: false,
  eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
    for (const msg of batch.messages) {
      if (!isRunning()) break;
      const event = JSON.parse(msg.value.toString());
      const fresh = await shardFor(event.txnId).query(
        "INSERT INTO processed_events (event_id, consumer) VALUES ($1,'settlement') \
         ON CONFLICT DO NOTHING RETURNING event_id", [event.txnId],
      );
      if (fresh.length) {
        try {
          await settle(event);                         // downstream side effect
        } catch (err) {
          if (isRetryable(err)) throw err;             // rebalance -> replay
          await publishToDlq("payments.events.dlq", msg, err);
        }
      }
      resolveOffset(msg.offset);
      await heartbeat();
    }
  },
});`,
  },
  {
    id: "resilience",
    label: "Backpressure + circuit breaker",
    lang: "typescript",
    code: String.raw`// Token-bucket admission control at the gateway: shed before the DB melts.
export class AdmissionController {
  private tokens: number;
  private last = Date.now();
  constructor(private ratePerSec: number, private burst: number) {
    this.tokens = burst;
  }
  tryAdmit(cost = 1) {
    const now = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + ((now - this.last) / 1000) * this.ratePerSec);
    this.last = now;
    if (this.tokens < cost) return false;   // -> 429 + Retry-After
    this.tokens -= cost;
    return true;
  }
}

// Circuit breaker around each shard pool; half-open probes restore traffic.
export class Breaker {
  private failures = 0;
  private openedAt = 0;
  constructor(private threshold = 20, private cooldownMs = 5_000) {}
  get state() {
    if (this.failures < this.threshold) return "closed";
    return Date.now() - this.openedAt > this.cooldownMs ? "half-open" : "open";
  }
  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === "open") throw httpError(503, "shard_unavailable");
    try {
      const out = await fn();
      this.failures = 0;
      return out;
    } catch (err) {
      if (++this.failures === this.threshold) this.openedAt = Date.now();
      throw err;
    }
  }
}

// Bounded retry with jitter for serialization failures / leader failover.
export async function withRetry<T>(fn: () => Promise<T>, tries = 3) {
  let lastErr: unknown;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (!["40001", "40P01", "57P01"].includes((err as { code?: string }).code ?? "")) throw err;
      await new Promise((r) => setTimeout(r, 5 * 2 ** i + Math.random() * 20));
    }
  }
  throw lastErr;
}`,
  },
  {
    id: "loadtest",
    label: "k6 load test",
    lang: "javascript",
    code: String.raw`// k6 run --vus 1200 --duration 30m load/payments.js
import http from "k6/http";
import { check } from "k6";
import { randomUUID } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

export const options = {
  discardResponseBodies: false,
  thresholds: {
    "http_req_duration{scenario:p2p}": ["p(99)<250", "p(50)<60"],
    "http_req_failed": ["rate<0.001"],          // 99.9% success under load
    "checks": ["rate>0.999"],
  },
  scenarios: {
    ramp: {
      executor: "ramping-arrival-rate",
      exec: "p2p",
      startRate: 500, timeUnit: "1s",
      preAllocatedVUs: 800, maxVUs: 4000,
      stages: [
        { target: 4000,  duration: "3m" },      // warm caches + JIT
        { target: 12000, duration: "5m" },      // SLO target
        { target: 12000, duration: "20m" },     // soak for leaks and lag
        { target: 18000, duration: "4m" },      // 1.5x burst headroom
        { target: 0,     duration: "2m" },
      ],
    },
    replay: {                                   // idempotency correctness
      executor: "constant-arrival-rate",
      exec: "duplicateReplay",
      rate: 200, timeUnit: "1s",
      duration: "34m", preAllocatedVUs: 100,
    },
  },
};

const BASE = __ENV.BASE_URL;
const accounts = JSON.parse(open("./accounts.json"));   // 5M pre-seeded accounts
const pick = () => accounts[Math.floor(Math.random() * accounts.length)];

export function p2p() {
  const key = randomUUID();
  const res = http.post(BASE + "/api/transfers", JSON.stringify({
    idempotencyKey: key, fromAccount: pick(), toAccount: pick(),
    amountMinor: 100 + Math.floor(Math.random() * 5000), currency: "USD",
  }), { headers: { "Content-Type": "application/json" }, tags: { scenario: "p2p" } });
  check(res, {
    "accepted or shed cleanly": (r) => [200, 201, 409, 422, 429].includes(r.status),
    "no 5xx": (r) => r.status < 500,
  });
}

export function duplicateReplay() {
  const key = randomUUID();
  const body = JSON.stringify({
    idempotencyKey: key, fromAccount: pick(), toAccount: pick(),
    amountMinor: 250, currency: "USD",
  });
  const h = { headers: { "Content-Type": "application/json" } };
  const first = http.post(BASE + "/api/transfers", body, h);
  const second = http.post(BASE + "/api/transfers", body, h);
  check(second, {
    "replay returns same txn": (r) =>
      first.status >= 400 || r.json("txnId") === first.json("txnId"),
  });
}`,
  },
  {
    id: "deploy",
    label: "Deployment + autoscaling",
    lang: "yaml",
    code: String.raw`# Blue/green with progressive traffic shift; ledger migrations are
# always additive so blue and green run against one schema version.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: txn-service
spec:
  replicas: 18
  strategy:
    rollingUpdate: { maxSurge: 25%, maxUnavailable: 0 }
  template:
    spec:
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector: { matchLabels: { app: txn-service } }
      containers:
        - name: txn
          image: registry/txn-service:GIT_SHA
          resources:
            requests: { cpu: "1500m", memory: 1Gi }
            limits:   { cpu: "2000m", memory: 2Gi }
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }   # fails when pool saturated
            periodSeconds: 2
          lifecycle:
            preStop: { exec: { command: ["sh","-c","sleep 15"] } }  # drain
          env:
            - name: PGPOOL_MAX          # bounded: shards * 120 total conns
              value: "24"
            - name: STATEMENT_TIMEOUT_MS
              value: "1200"
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: txn-service
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: txn-service }
  minReplicas: 12
  maxReplicas: 60
  behavior:
    scaleUp:   { stabilizationWindowSeconds: 15, policies: [{ type: Percent, value: 100, periodSeconds: 30 }] }
    scaleDown: { stabilizationWindowSeconds: 300, policies: [{ type: Percent, value: 10, periodSeconds: 60 }] }
  metrics:
    - type: Pods
      pods:
        metric: { name: inflight_txn_per_pod }
        target: { type: AverageValue, averageValue: "700" }
    - type: Pods
      pods:
        metric: { name: txn_commit_p99_ms }
        target: { type: AverageValue, averageValue: "180" }`,
  },
];

export function CodeLab() {
  const [active, setActive] = useState(SNIPPETS[0].id);
  const [copied, setCopied] = useState(false);
  const snippet = SNIPPETS.find((s) => s.id === active) ?? SNIPPETS[0];

  const copy = async () => {
    await navigator.clipboard.writeText(snippet.code);
    setCopied(true);
    setTimeout(() => setCopied(false), 1600);
  };

  return (
    <section id="code" className="mx-auto w-full max-w-7xl px-5 py-14">
      <p className="label-mono">Reference implementation</p>
      <h2 className="mt-1 text-2xl font-semibold sm:text-3xl">The code that runs the money</h2>
      <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
        Production-shaped snippets for every layer: sharded double-entry schema, ACID transfer with
        idempotency and optimistic concurrency, outbox-to-Kafka delivery, backpressure, load tests
        and the deploy topology.
      </p>

      <div className="mt-6 flex flex-wrap gap-2">
        {SNIPPETS.map((s) => (
          <button
            key={s.id}
            onClick={() => setActive(s.id)}
            className={`rounded-md border px-3 py-1.5 font-mono text-xs transition-colors ${
              active === s.id
                ? "border-primary bg-primary/10 text-primary"
                : "border-border bg-secondary/30 text-muted-foreground hover:text-foreground"
            }`}
          >
            {s.label}
          </button>
        ))}
      </div>

      <div className="panel mt-4 overflow-hidden">
        <div className="flex items-center justify-between border-b border-border px-4 py-2.5">
          <span className="label-mono">{snippet.lang}</span>
          <Button variant="ghost" size="sm" onClick={copy}>
            {copied ? <Check className="size-3.5 text-ok" /> : <Copy className="size-3.5" />}
            {copied ? "Copied" : "Copy"}
          </Button>
        </div>
        <pre className="max-h-[32rem] overflow-auto bg-background/50 p-4 font-mono text-xs leading-relaxed">
          <code>{snippet.code}</code>
        </pre>
      </div>
    </section>
  );
}
