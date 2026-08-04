const STEPS = [
  {
    step: "01",
    title: "Admit",
    detail:
      "Gateway validates JWT + mTLS, applies per-account token-bucket limits, stamps a trace id and routes on hash(account_id).",
    guard: "429 + Retry-After on shed",
  },
  {
    step: "02",
    title: "Claim idempotency",
    detail:
      "SET NX on idem:<key> in Redis (24h TTL). Replays return the stored response body; the unique index on transactions is the durable backstop.",
    guard: "exactly-one effect per key",
  },
  {
    step: "03",
    title: "Lock in order",
    detail:
      "REPEATABLE READ transaction, SELECT ... FOR UPDATE on both accounts sorted by uuid so crossing transfers can never deadlock.",
    guard: "deterministic lock order",
  },
  {
    step: "04",
    title: "Post double entry",
    detail:
      "Conditional UPDATE with version check plus balance >= amount, two ledger rows (-1/+1) and the outbox event in one commit.",
    guard: "atomic + balanced ledger",
  },
  {
    step: "05",
    title: "Commit + confirm",
    detail:
      "Group commit flushes WAL to a quorum of synchronous replicas, then the API returns the posted txn and balance.",
    guard: "durable before ack",
  },
  {
    step: "06",
    title: "Fan out async",
    detail:
      "Relay tails the outbox into payments.events keyed by account (ordering); consumers settle, project balances and notify with a dedupe table.",
    guard: "at-least-once, deduped",
  },
];

export function TransactionFlow() {
  return (
    <section id="flow" className="mx-auto w-full max-w-7xl px-5 py-14">
      <p className="label-mono">ACID transaction lifecycle</p>
      <h2 className="mt-1 text-2xl font-semibold sm:text-3xl">One transfer, six guarantees</h2>
      <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
        Money movement stays inside a single ACID commit. Everything that does not need to be
        synchronous - settlement, projections, notifications, fraud scoring - leaves through the
        transactional outbox.
      </p>

      <ol className="mt-6 grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {STEPS.map((s) => (
          <li key={s.step} className="panel p-5">
            <div className="flex items-baseline gap-3">
              <span className="font-mono text-sm text-primary">{s.step}</span>
              <h3 className="text-base font-semibold">{s.title}</h3>
            </div>
            <p className="mt-2 text-sm text-muted-foreground">{s.detail}</p>
            <p className="mt-3 inline-block rounded border border-primary/30 bg-primary/5 px-2 py-1 font-mono text-[0.65rem] tracking-wider text-primary uppercase">
              {s.guard}
            </p>
          </li>
        ))}
      </ol>
    </section>
  );
}
