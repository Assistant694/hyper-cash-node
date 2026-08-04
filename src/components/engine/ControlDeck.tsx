import { Slider } from "@/components/ui/slider";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { DEFAULT_TOPOLOGY, INCIDENTS, type Incident, type Topology } from "@/lib/engine";

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
