import { Activity, Cloud, Cpu, Database, Layers, Radio, ShieldCheck, Zap } from "lucide-react";
import type { Analysis, Topology } from "@/lib/engine";

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
