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
