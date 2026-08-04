import { useMemo } from "react";
import { analyze, DEFAULT_TOPOLOGY, SLO, type Topology } from "@/lib/engine";

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
