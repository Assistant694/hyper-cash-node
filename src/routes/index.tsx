import { Activity, Cloud, Cpu, Database, Layers, Radio, ShieldCheck, Zap, Pause, Play, RotateCcw, Trophy, Check, Copy, ArrowRight, GitBranch, Github } from "lucide-react";
import { Slider } from "@/components/ui/slider";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Area, AreaChart, CartesianGrid, Line, LineChart, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { createFileRoute } from "@tanstack/react-router";

// ==================== src/lib/engine.ts ====================
/**
 * Capacity + latency model for the payments engine.
 * Pure functions so the simulator, planner and scoring share one source of truth.
 */

export type Topology = {
  gatewayPods: number;
  servicePods: number;
  shards: number;
  replicasPerShard: number;
  cacheNodes: number;
  kafkaPartitions: number;
  consumers: number;
  multiRegion: boolean;
  batchSize: number;
};

export const DEFAULT_TOPOLOGY: Topology = {
  gatewayPods: 12,
  servicePods: 18,
  shards: 8,
  replicasPerShard: 2,
  cacheNodes: 6,
  kafkaPartitions: 24,
  consumers: 16,
  multiRegion: true,
  batchSize: 32,
};

export const SLO = {
  targetTps: 12000,
  p99Ms: 250,
  availability: 99.95,
  monthlyBudgetUsd: 26000,
};

/** Per-unit throughput assumptions (TPS) derived from benchmark runs. */
const UNIT = {
  gatewayPod: 2600,
  servicePod: 1150,
  shardWrite: 1750,
  kafkaPartition: 900,
  consumer: 1100,
  cacheNode: 42000,
};

/** Monthly USD cost per unit (reserved pricing). */
const COST = {
  gatewayPod: 46,
  servicePod: 62,
  shard: 430,
  replica: 265,
  cacheNode: 95,
  kafkaPartition: 9,
  consumer: 48,
  observability: 900,
  multiRegionMultiplier: 1.55,
};

export type Stage = {
  id: string;
  name: string;
  capacity: number;
  utilization: number;
  baseLatency: number;
};

export type Analysis = {
  cacheHitRatio: number;
  stages: Stage[];
  maxTps: number;
  bottleneck: Stage;
  utilization: number;
  p50: number;
  p99: number;
  availability: number;
  monthlyCost: number;
  costPerMillion: number;
  meetsSlo: boolean;
  score: number;
  notes: string[];
};

const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n));

export function cacheHitRatio(t: Topology): number {
  return clamp(0.55 + 0.065 * Math.log2(Math.max(1, t.cacheNodes)) * 1.6, 0, 0.93);
}

export function analyze(t: Topology, offeredTps: number = SLO.targetTps): Analysis {
  const hit = cacheHitRatio(t);
  const batchGain = 1 + 0.35 * Math.log2(clamp(t.batchSize, 1, 256));
  const dbLoadFactor = 1 - 0.45 * hit;
  const regionGain = t.multiRegion ? 1.9 : 1;

  const stages: Stage[] = [
    {
      id: "gateway",
      name: "API gateway (auth, rate limit, routing)",
      capacity: t.gatewayPods * UNIT.gatewayPod * regionGain,
      utilization: 0,
      baseLatency: 4,
    },
    {
      id: "service",
      name: "Transaction service (ACID orchestration)",
      capacity: t.servicePods * UNIT.servicePod * regionGain,
      utilization: 0,
      baseLatency: 6,
    },
    {
      id: "cache",
      name: "Redis cluster (idempotency + balances)",
      capacity: (t.cacheNodes * UNIT.cacheNode) / 3,
      utilization: 0,
      baseLatency: 1.2,
    },
    {
      id: "db",
      name: "Postgres shards (" + t.shards + "x, RF=" + (t.replicasPerShard + 1) + ")",
      capacity: (t.shards * UNIT.shardWrite * batchGain) / dbLoadFactor,
      utilization: 0,
      baseLatency: 9,
    },
    {
      id: "kafka",
      name: "Kafka payments.events",
      capacity: t.kafkaPartitions * UNIT.kafkaPartition,
      utilization: 0,
      baseLatency: 2,
    },
    {
      id: "consumers",
      name: "Async consumers (settlement, projections)",
      capacity: t.consumers * UNIT.consumer,
      utilization: 0,
      baseLatency: 0,
    },
  ];

  const maxTps = Math.round(Math.min(...stages.map((s) => s.capacity)));
  const bottleneck = stages.reduce((a, b) => (a.capacity <= b.capacity ? a : b));
  for (const s of stages) s.utilization = clamp(offeredTps / s.capacity, 0, 1.4);

  const utilization = clamp(offeredTps / maxTps, 0, 1.4);

  const queue = (s: Stage) => {
    if (s.baseLatency === 0) return 0;
    const u = clamp(s.utilization, 0, 0.985);
    return s.baseLatency * (1 / (1 - u) - 1);
  };
  const sync = stages.filter((s) => s.baseLatency > 0);
  const p50 = sync.reduce((sum, s) => sum + s.baseLatency + queue(s) * 0.35, 0);
  const p99 = Math.round(
    sync.reduce((sum, s) => sum + s.baseLatency * 1.4 + queue(s) * 2.4, 0) +
      (t.multiRegion ? 6 : 0) +
      t.batchSize * 0.18,
  );

  let availability = 99.5;
  if (t.replicasPerShard >= 1) availability = 99.9;
  if (t.replicasPerShard >= 2) availability = 99.95;
  if (t.multiRegion && t.replicasPerShard >= 2) availability = 99.99;
  if (utilization > 0.85) availability -= (utilization - 0.85) * 2.2;
  availability = Math.round(clamp(availability, 95, 99.995) * 1000) / 1000;

  let monthlyCost =
    t.gatewayPods * COST.gatewayPod +
    t.servicePods * COST.servicePod +
    t.shards * COST.shard +
    t.shards * t.replicasPerShard * COST.replica +
    t.cacheNodes * COST.cacheNode +
    t.kafkaPartitions * COST.kafkaPartition +
    t.consumers * COST.consumer +
    COST.observability;
  if (t.multiRegion) monthlyCost *= COST.multiRegionMultiplier;
  monthlyCost = Math.round(monthlyCost);

  const txPerMonth = (offeredTps * 60 * 60 * 24 * 30) / 1_000_000;
  const costPerMillion = Math.round((monthlyCost / Math.max(1, txPerMonth)) * 100) / 100;

  const notes: string[] = [];
  if (maxTps < SLO.targetTps)
    notes.push(bottleneck.name + " caps the system at " + maxTps.toLocaleString() + " TPS.");
  if (p99 > SLO.p99Ms) notes.push("p99 " + p99 + "ms breaches the " + SLO.p99Ms + "ms SLO.");
  if (availability < SLO.availability)
    notes.push("Availability below 99.95% - add replicas or a second region.");
  if (monthlyCost > SLO.monthlyBudgetUsd)
    notes.push("Over budget by $" + (monthlyCost - SLO.monthlyBudgetUsd).toLocaleString() + "/mo.");
  if (t.batchSize > 96) notes.push("Large group-commit batches inflate tail latency.");
  if (utilization < 0.35 && maxTps > SLO.targetTps * 2)
    notes.push("Over-provisioned - trim capacity to recover budget.");
  if (!notes.length) notes.push("All SLOs green with headroom. Ship it.");

  const meetsSlo =
    maxTps >= SLO.targetTps &&
    p99 <= SLO.p99Ms &&
    availability >= SLO.availability &&
    monthlyCost <= SLO.monthlyBudgetUsd;

  const tpsScore = clamp(maxTps / SLO.targetTps, 0, 1.25) * 32;
  const latScore = clamp(SLO.p99Ms / Math.max(1, p99), 0, 1.2) * 26;
  const availScore = clamp((availability - 99) / (99.99 - 99), 0, 1) * 22;
  const costScore = clamp(SLO.monthlyBudgetUsd / Math.max(1, monthlyCost), 0, 1.15) * 20;
  const score = Math.round(clamp(tpsScore + latScore + availScore + costScore, 0, 100));

  return {
    cacheHitRatio: hit,
    stages,
    maxTps,
    bottleneck,
    utilization,
    p50: Math.round(p50 * 10) / 10,
    p99,
    availability,
    monthlyCost,
    costPerMillion,
    meetsSlo,
    score,
    notes,
  };
}

export type Incident = "none" | "shard_loss" | "cache_flush" | "kafka_lag" | "traffic_spike";

export const INCIDENTS: { id: Incident; label: string; detail: string }[] = [
  { id: "shard_loss", label: "Kill shard leader", detail: "Raft failover, writes park on replica" },
  { id: "cache_flush", label: "Flush Redis", detail: "Cold cache, lookups hit Postgres" },
  { id: "kafka_lag", label: "Consumer lag storm", detail: "Projections fall behind head offset" },
  { id: "traffic_spike", label: "Payday spike x2.4", detail: "Burst of P2P transfers" },
];

export function degrade(
  t: Topology,
  incident: Incident,
): { topology: Topology; loadMultiplier: number } {
  switch (incident) {
    case "shard_loss":
      return { topology: { ...t, shards: Math.max(1, t.shards - 1) }, loadMultiplier: 1 };
    case "cache_flush":
      return { topology: { ...t, cacheNodes: 1 }, loadMultiplier: 1 };
    case "kafka_lag":
      return {
        topology: { ...t, consumers: Math.max(1, Math.round(t.consumers * 0.35)) },
        loadMultiplier: 1,
      };
    case "traffic_spike":
      return { topology: t, loadMultiplier: 2.4 };
    default:
      return { topology: t, loadMultiplier: 1 };
  }
}

// ==================== src/components/engine/ArchitectureMap.tsx ====================
const ICONS: Record<string, typeof Cpu> = {
  gateway: ShieldCheck,
  service: Cpu,
  cache: Zap,
  db: Database,
  kafka: Radio,
  consumers: Layers,
};

function tone(u: number) {
  if (u > 0.95) return "text-crit";
  if (u > 0.8) return "text-warn";
  return "text-ok";
}

export function ArchitectureMap({
  analysis,
  topology,
  running,
}: {
  analysis: Analysis;
  topology: Topology;
  running: boolean;
}) {
  return (
    <div className="panel p-5">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-base font-semibold">Request path</h2>
        <span className="label-mono">
          {topology.multiRegion ? "active/active - 2 regions" : "single region"}
        </span>
      </div>

      <div className="space-y-2">
        {analysis.stages.map((stage, i) => {
          const Icon = ICONS[stage.id] ?? Activity;
          const pct = Math.min(100, Math.round(stage.utilization * 100));
          const isBottleneck = stage.id === analysis.bottleneck.id;
          return (
            <div key={stage.id}>
              {i > 0 && (
                <div
                  className={`mx-6 h-4 w-px ${running ? "flow-line-active" : "flow-line"}`}
                  aria-hidden
                />
              )}
              <div
                className={`flex items-center gap-4 rounded-md border px-4 py-3 ${
                  isBottleneck ? "border-warn/60 bg-warn/5" : "border-border bg-secondary/30"
                }`}
              >
                <Icon className={`size-5 shrink-0 ${tone(stage.utilization)}`} aria-hidden />
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-baseline justify-between gap-2">
                    <p className="truncate text-sm font-medium">{stage.name}</p>
                    <p className="font-mono text-xs text-muted-foreground">
                      {Math.round(stage.capacity).toLocaleString()} TPS cap
                      {stage.baseLatency > 0 ? ` - ${stage.baseLatency}ms base` : " - async"}
                    </p>
                  </div>
                  <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-grid">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${
                        stage.utilization > 0.95
                          ? "bg-crit"
                          : stage.utilization > 0.8
                            ? "bg-warn"
                            : "bg-ok"
                      }`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
                <span className={`font-mono text-sm ${tone(stage.utilization)}`}>{pct}%</span>
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-3">
        {[
          { label: "Bottleneck", value: analysis.bottleneck.name.split(" (")[0] },
          { label: "Cache hit ratio", value: `${Math.round(analysis.cacheHitRatio * 100)}%` },
          {
            label: "Failure domains",
            value: `${topology.shards} shards / ${topology.replicasPerShard + 1} copies`,
          },
        ].map((m) => (
          <div key={m.label} className="rounded-md border border-border bg-background/40 p-3">
            <p className="label-mono">{m.label}</p>
            <p className="mt-1 truncate text-sm font-medium">{m.value}</p>
          </div>
        ))}
      </div>

      <p className="mt-4 flex items-start gap-2 text-xs text-muted-foreground">
        <Cloud className="mt-0.5 size-3.5 shrink-0" aria-hidden />
        Writes are synchronous through gateway, service, cache and the shard leader; settlement,
        ledger projections and notifications run off Kafka via the transactional outbox.
      </p>
    </div>
  );
}

// ==================== src/components/engine/ControlDeck.tsx ====================
type Knob = {
  key: keyof Omit<Topology, "multiRegion">;
  label: string;
  min: number;
  max: number;
  step: number;
  unit: string;
};

const KNOBS: Knob[] = [
  { key: "gatewayPods", label: "Gateway pods", min: 2, max: 40, step: 1, unit: "pods" },
  { key: "servicePods", label: "Txn service pods", min: 2, max: 60, step: 1, unit: "pods" },
  { key: "shards", label: "Postgres shards", min: 1, max: 32, step: 1, unit: "shards" },
  { key: "replicasPerShard", label: "Replicas / shard", min: 0, max: 3, step: 1, unit: "replicas" },
  { key: "cacheNodes", label: "Redis nodes", min: 1, max: 24, step: 1, unit: "nodes" },
  { key: "kafkaPartitions", label: "Kafka partitions", min: 3, max: 96, step: 3, unit: "parts" },
  { key: "consumers", label: "Consumer workers", min: 1, max: 64, step: 1, unit: "workers" },
  { key: "batchSize", label: "Group commit batch", min: 1, max: 128, step: 1, unit: "rows" },
];

export function ControlDeck({
  topology,
  onChange,
  offeredTps,
  onOfferedTps,
  incident,
  onIncident,
}: {
  topology: Topology;
  onChange: (t: Topology) => void;
  offeredTps: number;
  onOfferedTps: (n: number) => void;
  incident: Incident;
  onIncident: (i: Incident) => void;
}) {
  return (
    <div className="panel p-5">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-base font-semibold">Control deck</h2>
        <Button variant="outline" size="sm" onClick={() => onChange(DEFAULT_TOPOLOGY)}>
          Reset
        </Button>
      </div>

      <div className="rounded-md border border-primary/30 bg-primary/5 p-3">
        <div className="flex items-baseline justify-between">
          <span className="label-mono">Offered load</span>
          <span className="font-mono text-sm text-primary">
            {offeredTps.toLocaleString()} TPS
          </span>
        </div>
        <Slider
          className="mt-3"
          value={[offeredTps]}
          min={1000}
          max={40000}
          step={500}
          onValueChange={([v]) => onOfferedTps(v ?? offeredTps)}
          aria-label="Offered load in transactions per second"
        />
      </div>

      <div className="mt-4 space-y-4">
        {KNOBS.map((k) => (
          <div key={k.key}>
            <div className="flex items-baseline justify-between">
              <span className="text-sm">{k.label}</span>
              <span className="font-mono text-xs text-muted-foreground">
                {topology[k.key]} {k.unit}
              </span>
            </div>
            <Slider
              className="mt-2"
              value={[topology[k.key]]}
              min={k.min}
              max={k.max}
              step={k.step}
              onValueChange={([v]) => onChange({ ...topology, [k.key]: v ?? topology[k.key] })}
              aria-label={k.label}
            />
          </div>
        ))}

        <div className="flex items-center justify-between rounded-md border border-border bg-secondary/30 px-3 py-2.5">
          <div>
            <p className="text-sm">Multi-region active/active</p>
            <p className="text-xs text-muted-foreground">Doubles capacity, +55% cost, +6ms p99</p>
          </div>
          <Switch
            checked={topology.multiRegion}
            onCheckedChange={(v) => onChange({ ...topology, multiRegion: v })}
            aria-label="Multi-region active/active"
          />
        </div>
      </div>

      <div className="mt-5">
        <p className="label-mono">Chaos injection</p>
        <div className="mt-2 grid gap-2 sm:grid-cols-2">
          {INCIDENTS.map((inc) => (
            <button
              key={inc.id}
              onClick={() => onIncident(incident === inc.id ? "none" : inc.id)}
              className={`rounded-md border px-3 py-2 text-left transition-colors ${
                incident === inc.id
                  ? "border-crit bg-crit/10"
                  : "border-border bg-secondary/30 hover:border-crit/50"
              }`}
            >
              <p className="text-sm font-medium">{inc.label}</p>
              <p className="text-xs text-muted-foreground">{inc.detail}</p>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

// ==================== src/components/engine/LiveSimulator.tsx ====================
type Sample = { t: number; tps: number; p99: number; errors: number };

const fmt = (n: number) => n.toLocaleString("en-US");

function Metric({
  label,
  value,
  sub,
  state,
}: {
  label: string;
  value: string;
  sub: string;
  state: "ok" | "warn" | "crit";
}) {
  const color = state === "crit" ? "text-crit" : state === "warn" ? "text-warn" : "text-ok";
  return (
    <div className="panel p-4">
      <p className="label-mono">{label}</p>
      <p className={`mt-1 font-mono text-2xl ${color}`}>{value}</p>
      <p className="mt-1 text-xs text-muted-foreground">{sub}</p>
    </div>
  );
}

export function LiveSimulator() {
  const [topology, setTopology] = useState<Topology>(DEFAULT_TOPOLOGY);
  const [offeredTps, setOfferedTps] = useState(SLO.targetTps);
  const [incident, setIncident] = useState<Incident>("none");
  const [running, setRunning] = useState(true);
  const [samples, setSamples] = useState<Sample[]>([]);
  const [log, setLog] = useState<string[]>(["engine armed - 0 in-flight transactions"]);
  const [settled, setSettled] = useState(0);
  const [rejected, setRejected] = useState(0);
  const tick = useRef(0);

  const { topology: effective, loadMultiplier } = useMemo(
    () => degrade(topology, incident),
    [topology, incident],
  );
  const demand = Math.round(offeredTps * loadMultiplier);
  const analysis = useMemo(() => analyze(effective, demand), [effective, demand]);
  const planned = useMemo(() => analyze(topology, offeredTps), [topology, offeredTps]);

  const pushLog = useCallback((line: string) => {
    setLog((prev) => [line, ...prev].slice(0, 40));
  }, []);

  useEffect(() => {
    if (incident === "none") return;
    pushLog(`chaos: ${incident} injected at t=${tick.current}s`);
  }, [incident, pushLog]);

  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => {
      tick.current += 1;
      const jitter = 1 + (Math.random() - 0.5) * 0.08;
      const served = Math.min(demand, analysis.maxTps) * jitter;
      const dropped = Math.max(0, demand - analysis.maxTps);
      const p99 = Math.round(analysis.p99 * (1 + (Math.random() - 0.4) * 0.12));
      setSamples((prev) =>
        [...prev, { t: tick.current, tps: Math.round(served), p99, errors: Math.round(dropped) }].slice(-45),
      );
      setSettled((s) => s + Math.round(served));
      setRejected((r) => r + Math.round(dropped));
      if (dropped > 0 && tick.current % 3 === 0)
        pushLog(`shed ${fmt(Math.round(dropped))} req/s at ${analysis.bottleneck.id} - 429 backpressure`);
      if (p99 > SLO.p99Ms && tick.current % 4 === 0)
        pushLog(`p99 ${p99}ms over SLO - queue depth rising`);
      if (dropped === 0 && p99 <= SLO.p99Ms && tick.current % 7 === 0)
        pushLog(`healthy: ${fmt(Math.round(served))} TPS @ p99 ${p99}ms, all shards in quorum`);
    }, 900);
    return () => clearInterval(id);
  }, [running, demand, analysis, pushLog]);

  const reset = () => {
    setSamples([]);
    setSettled(0);
    setRejected(0);
    setIncident("none");
    tick.current = 0;
    setLog(["engine reset - counters cleared"]);
  };

  const successRate = settled + rejected > 0 ? (settled / (settled + rejected)) * 100 : 100;

  return (
    <section id="simulator" className="mx-auto w-full max-w-7xl px-5 py-14">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="label-mono">Gamified reliability sim</p>
          <h2 className="mt-1 text-2xl font-semibold sm:text-3xl">Run the engine, break the engine</h2>
          <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
            Tune the topology, drive synthetic load and inject failures. Score rewards hitting 12k TPS
            at p99 &lt; 250ms and 99.95% availability without blowing the ${fmt(SLO.monthlyBudgetUsd)}/mo budget.
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant={running ? "secondary" : "default"} onClick={() => setRunning((r) => !r)}>
            {running ? <Pause className="size-4" /> : <Play className="size-4" />}
            {running ? "Pause" : "Resume"}
          </Button>
          <Button variant="outline" onClick={reset}>
            <RotateCcw className="size-4" /> Reset
          </Button>
        </div>
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-4">
        <Metric
          label="Sustained throughput"
          value={fmt(analysis.maxTps)}
          sub={`demand ${fmt(demand)} TPS - target ${fmt(SLO.targetTps)}`}
          state={analysis.maxTps >= SLO.targetTps ? "ok" : analysis.maxTps >= SLO.targetTps * 0.8 ? "warn" : "crit"}
        />
        <Metric
          label="Latency p99 / p50"
          value={`${analysis.p99}ms`}
          sub={`p50 ${analysis.p50}ms - SLO ${SLO.p99Ms}ms`}
          state={analysis.p99 <= SLO.p99Ms ? "ok" : analysis.p99 <= SLO.p99Ms * 1.5 ? "warn" : "crit"}
        />
        <Metric
          label="Availability"
          value={`${analysis.availability}%`}
          sub={`success ${successRate.toFixed(3)}% observed`}
          state={analysis.availability >= SLO.availability ? "ok" : "warn"}
        />
        <Metric
          label="Run cost"
          value={`$${fmt(analysis.monthlyCost)}`}
          sub={`$${analysis.costPerMillion}/M txn - budget $${fmt(SLO.monthlyBudgetUsd)}`}
          state={analysis.monthlyCost <= SLO.monthlyBudgetUsd ? "ok" : "crit"}
        />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-3">
        <div className="panel p-5 lg:col-span-2">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold">Throughput served vs shed</h3>
            <span className="label-mono">{samples.length ? `t+${samples[samples.length - 1]!.t}s` : "idle"}</span>
          </div>
          <div className="mt-3 h-48">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={samples}>
                <defs>
                  <linearGradient id="gTps" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--color-ok)" stopOpacity={0.5} />
                    <stop offset="100%" stopColor="var(--color-ok)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke="var(--color-grid)" vertical={false} />
                <XAxis dataKey="t" hide />
                <YAxis
                  tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
                  stroke="var(--color-grid)"
                  width={46}
                />
                <Area
                  type="monotone"
                  dataKey="tps"
                  stroke="var(--color-ok)"
                  fill="url(#gTps)"
                  strokeWidth={2}
                  isAnimationActive={false}
                />
                <Area
                  type="monotone"
                  dataKey="errors"
                  stroke="var(--color-crit)"
                  fill="var(--color-crit)"
                  fillOpacity={0.18}
                  strokeWidth={1.5}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 h-32">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={samples}>
                <CartesianGrid stroke="var(--color-grid)" vertical={false} />
                <XAxis dataKey="t" hide />
                <YAxis
                  tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
                  stroke="var(--color-grid)"
                  width={46}
                />
                <Line
                  type="monotone"
                  dataKey="p99"
                  stroke="var(--color-warn)"
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            Green: settled TPS. Red: load shed by backpressure. Amber: p99 latency (ms).
          </p>
        </div>

        <div className="panel flex flex-col p-5">
          <div className="flex items-center gap-2">
            <Trophy className="size-4 text-accent" aria-hidden />
            <h3 className="text-sm font-semibold">Reliability score</h3>
          </div>
          <p className="mt-3 font-mono text-5xl text-accent">{planned.score}</p>
          <p className="text-xs text-muted-foreground">
            of 100 - {planned.meetsSlo ? "all SLOs met" : "SLO gap detected"}
          </p>
          <ul className="mt-4 space-y-2 text-xs">
            {planned.notes.map((n) => (
              <li key={n} className="flex gap-2 text-muted-foreground">
                <span className="text-accent">›</span>
                {n}
              </li>
            ))}
          </ul>
          <div className="mt-4 grid grid-cols-2 gap-2 border-t border-border pt-4 font-mono text-xs">
            <div>
              <p className="label-mono">Settled</p>
              <p className="text-ok">{fmt(settled)}</p>
            </div>
            <div>
              <p className="label-mono">Shed</p>
              <p className="text-crit">{fmt(rejected)}</p>
            </div>
          </div>
        </div>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <ArchitectureMap analysis={analysis} topology={effective} running={running} />
          <div className="panel mt-4 p-5">
            <h3 className="text-sm font-semibold">Event stream</h3>
            <div className="mt-3 max-h-56 space-y-1 overflow-y-auto font-mono text-xs">
              {log.map((line, i) => (
                <p key={`${i}-${line}`} className={i === 0 ? "text-primary" : "text-muted-foreground"}>
                  {line}
                </p>
              ))}
            </div>
          </div>
        </div>
        <ControlDeck
          topology={topology}
          onChange={setTopology}
          offeredTps={offeredTps}
          onOfferedTps={setOfferedTps}
          incident={incident}
          onIncident={setIncident}
        />
      </div>
    </section>
  );
}

// ==================== src/components/engine/TransactionFlow.tsx ====================
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

// ==================== src/components/engine/CapacityPlanner.tsx ====================
const TIERS: { name: string; tps: number; topology: Topology }[] = [
  {
    name: "Pilot",
    tps: 2000,
    topology: { ...DEFAULT_TOPOLOGY, gatewayPods: 3, servicePods: 4, shards: 2, replicasPerShard: 1, cacheNodes: 2, kafkaPartitions: 6, consumers: 4, multiRegion: false },
  },
  {
    name: "Launch",
    tps: 6000,
    topology: { ...DEFAULT_TOPOLOGY, gatewayPods: 6, servicePods: 9, shards: 4, replicasPerShard: 2, cacheNodes: 4, kafkaPartitions: 12, consumers: 8, multiRegion: false },
  },
  { name: "Target SLO", tps: SLO.targetTps, topology: DEFAULT_TOPOLOGY },
  {
    name: "Peak / payday",
    tps: 28000,
    topology: { ...DEFAULT_TOPOLOGY, gatewayPods: 20, servicePods: 30, shards: 16, replicasPerShard: 2, cacheNodes: 10, kafkaPartitions: 48, consumers: 32 },
  },
];

const fmt = (n: number) => n.toLocaleString("en-US");

export function CapacityPlanner() {
  const rows = useMemo(
    () => TIERS.map((t) => ({ ...t, a: analyze(t.topology, t.tps) })),
    [],
  );

  return (
    <section id="capacity" className="mx-auto w-full max-w-7xl px-5 py-14">
      <p className="label-mono">Capacity plan &amp; unit economics</p>
      <h2 className="mt-1 text-2xl font-semibold sm:text-3xl">Scale steps inside the budget</h2>
      <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
        Each tier is sized for 65-75% peak utilisation so a zone loss or a leader failover is
        absorbed without shedding load. Cost per million transactions falls as fixed observability
        and control-plane spend amortises.
      </p>

      <div className="panel mt-6 overflow-x-auto">
        <table className="w-full min-w-[46rem] text-sm">
          <thead>
            <tr className="border-b border-border text-left">
              {["Tier", "Design load", "Ceiling", "p99", "Availability", "Shards", "$ / month", "$ / M txn"].map(
                (h) => (
                  <th key={h} className="label-mono px-4 py-3">
                    {h}
                  </th>
                ),
              )}
            </tr>
          </thead>
          <tbody className="font-mono">
            {rows.map((r) => (
              <tr
                key={r.name}
                className={`border-b border-border/60 last:border-0 ${
                  r.tps === SLO.targetTps ? "bg-primary/5" : ""
                }`}
              >
                <td className="px-4 py-3 font-sans font-medium">{r.name}</td>
                <td className="px-4 py-3">{fmt(r.tps)} TPS</td>
                <td className="px-4 py-3 text-ok">{fmt(r.a.maxTps)} TPS</td>
                <td className={`px-4 py-3 ${r.a.p99 <= SLO.p99Ms ? "text-ok" : "text-warn"}`}>
                  {r.a.p99}ms
                </td>
                <td className="px-4 py-3">{r.a.availability}%</td>
                <td className="px-4 py-3">
                  {r.topology.shards} x {r.topology.replicasPerShard + 1}
                </td>
                <td className="px-4 py-3">${fmt(r.a.monthlyCost)}</td>
                <td className="px-4 py-3 text-accent">${r.a.costPerMillion}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-4 grid gap-4 md:grid-cols-3">
        {[
          {
            t: "Sizing math",
            d: "12,000 TPS x 3 cache ops + 1.4 DB writes. At 1,750 write TPS per shard and 68% cache offload, 8 shards hold peak at ~70% utilisation.",
          },
          {
            t: "Headroom policy",
            d: "Trigger a reshard at 70% sustained shard utilisation for 3 days; partition-per-shard layout makes rebalancing a partition move, not a rewrite.",
          },
          {
            t: "Budget levers",
            d: "Cache nodes buy the cheapest throughput, replicas buy availability, regions are the most expensive lever - add last.",
          },
        ].map((c) => (
          <div key={c.t} className="panel p-5">
            <h3 className="text-sm font-semibold">{c.t}</h3>
            <p className="mt-2 text-sm text-muted-foreground">{c.d}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

// ==================== src/components/engine/CodeLab.tsx ====================
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
  const [active, setActive] = useState(SNIPPETS[0]!.id);
  const [copied, setCopied] = useState(false);
  const snippet = SNIPPETS.find((s) => s.id === active) ?? SNIPPETS[0]!;

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

// ==================== src/routes/index.tsx ====================
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
