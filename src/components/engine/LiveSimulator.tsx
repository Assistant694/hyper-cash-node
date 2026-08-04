import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Area, AreaChart, CartesianGrid, Line, LineChart, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { Pause, Play, RotateCcw, Trophy } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ArchitectureMap } from "./ArchitectureMap";
import { ControlDeck } from "./ControlDeck";
import { analyze, degrade, DEFAULT_TOPOLOGY, SLO, type Incident, type Topology } from "@/lib/engine";

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
