import { createFileRoute } from "@tanstack/react-router";
import { ArrowRight, GitBranch, Github, ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";
import { LiveSimulator } from "@/components/engine/LiveSimulator";
import { TransactionFlow } from "@/components/engine/TransactionFlow";
import { CapacityPlanner } from "@/components/engine/CapacityPlanner";
import { CodeLab } from "@/components/engine/CodeLab";
import { SLO } from "@/lib/engine";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Ledgerline — 12k TPS Payments Engine Blueprint & Simulator" },
      {
        name: "description",
        content:
          "Interactive blueprint for a distributed payments and P2P engine: 12,000+ TPS, sharded ACID ledger, idempotency, Kafka outbox, chaos simulator, capacity plan and reference code.",
      },
      { property: "og:title", content: "Ledgerline — 12k TPS Payments Engine Blueprint" },
      {
        property: "og:description",
        content:
          "Design, load-test and break a high-throughput fintech transaction engine: sharding, caching, Kafka event processing, ACID flows and capacity planning in one simulator.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: Index,
});

const HERO_STATS = [
  { k: `${SLO.targetTps.toLocaleString()}+`, v: "sustained TPS" },
  { k: `< ${SLO.p99Ms}ms`, v: "p99 commit latency" },
  { k: "99.99%", v: "multi-region availability" },
  { k: `$${(SLO.monthlyBudgetUsd / 1000).toFixed(0)}k`, v: "monthly infra budget" },
];

function Index() {
  return (
    <main className="min-h-screen">
      <header className="sticky top-0 z-30 border-b border-border/70 bg-background/85 backdrop-blur">
        <nav className="mx-auto flex w-full max-w-7xl items-center justify-between px-5 py-3">
          <div className="flex items-center gap-2">
            <GitBranch className="size-4 text-primary" aria-hidden />
            <span className="font-mono text-sm font-semibold tracking-tight">ledgerline</span>
          </div>
          <div className="hidden gap-6 font-mono text-xs text-muted-foreground md:flex">
            <a className="hover:text-foreground" href="#simulator">simulator</a>
            <a className="hover:text-foreground" href="#flow">txn flow</a>
            <a className="hover:text-foreground" href="#capacity">capacity</a>
            <a className="hover:text-foreground" href="#code">code</a>
          </div>
          <Button size="sm" asChild>
            <a href="#simulator">
              Run sim <ArrowRight className="size-3.5" />
            </a>
          </Button>
        </nav>
      </header>

      <section className="mx-auto w-full max-w-7xl px-5 pt-16 pb-6">
        <p className="inline-flex items-center gap-2 rounded-full border border-primary/30 bg-primary/5 px-3 py-1 font-mono text-xs text-primary">
          <ShieldCheck className="size-3.5" aria-hidden /> distributed payments + P2P engine
        </p>
        <h1 className="mt-5 max-w-4xl text-4xl leading-[1.05] font-semibold sm:text-6xl">
          A payments engine that holds 12,000 TPS
          <span className="text-primary"> without losing a cent</span>
        </h1>
        <p className="mt-5 max-w-2xl text-base text-muted-foreground">
          Ledgerline is a full blueprint and live simulator for a high-throughput fintech transaction
          system: API gateway admission control, hash-sharded Postgres with a double-entry ledger,
          Redis-backed idempotency, Kafka event processing through a transactional outbox, and chaos
          drills that prove the failure modes before customers find them.
        </p>
        <div className="mt-8 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {HERO_STATS.map((s) => (
            <div key={s.v} className="panel p-4">
              <p className="font-mono text-2xl text-primary">{s.k}</p>
              <p className="mt-1 text-xs text-muted-foreground">{s.v}</p>
            </div>
          ))}
        </div>
      </section>

      <LiveSimulator />
      <TransactionFlow />
      <CapacityPlanner />
      <CodeLab />

      <footer className="border-t border-border/70">
        <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-3 px-5 py-8 font-mono text-xs text-muted-foreground">
          <span>ledgerline — payments engine blueprint</span>
          <span className="flex items-center gap-2">
            <Github className="size-3.5" aria-hidden /> model assumptions from benchmark runs, not a
            production guarantee
          </span>
        </div>
      </footer>
    </main>
  );
}
