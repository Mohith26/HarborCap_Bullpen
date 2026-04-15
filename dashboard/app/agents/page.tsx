'use client';

import { useEffect, useState, useCallback } from 'react';
import { supabase } from '@/lib/supabase';
import { AgentHealth, AgentRun } from '@/lib/types';
import AgentStatusCard from '@/components/AgentStatusCard';
import { formatDistanceToNow } from 'date-fns';

const SCHEDULER_API = process.env.NEXT_PUBLIC_SCHEDULER_API_URL || '';

export default function AgentHealthPage() {
  const [healthData, setHealthData] = useState<AgentHealth[]>([]);
  const [recentRuns, setRecentRuns] = useState<AgentRun[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    // Fetch schedule data from Railway scheduler API
    let scheduleMap: Record<string, { next_run?: string; cron?: string }> = {};
    if (SCHEDULER_API) {
      try {
        const resp = await fetch(`${SCHEDULER_API}/agents`);
        if (resp.ok) {
          const body = await resp.json();
          for (const a of body.agents || []) {
            scheduleMap[a.name] = { next_run: a.next_run, cron: a.cron };
          }
        }
      } catch {
        // Scheduler API may be unreachable — continue without schedule data
      }
    }

    // Try the view first, fall back to client-side aggregation
    const { data: healthView, error: healthError } = await supabase
      .from('v_agent_health')
      .select('*');

    if (!healthError && healthView && healthView.length > 0) {
      const merged = (healthView as AgentHealth[]).map((h) => ({
        ...h,
        ...scheduleMap[h.agent_name],
      }));
      setHealthData(merged);
    } else {
      // Fallback: aggregate from agent_runs
      const { data: runs } = await supabase
        .from('agent_runs')
        .select('*')
        .order('started_at', { ascending: false });

      if (runs && runs.length > 0) {
        const agentMap = new Map<string, AgentHealth>();

        for (const run of runs as AgentRun[]) {
          const existing = agentMap.get(run.agent_name);
          const duration = run.started_at && run.finished_at
            ? (new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000
            : 0;

          if (!existing) {
            agentMap.set(run.agent_name, {
              agent_name: run.agent_name,
              total_runs: 1,
              successful: run.status === 'success' ? 1 : 0,
              failed: run.status === 'failed' ? 1 : 0,
              last_run: run.started_at,
              avg_duration_seconds: duration,
              total_new_records: run.records_new || 0,
            });
          } else {
            existing.total_runs += 1;
            if (run.status === 'success') existing.successful += 1;
            if (run.status === 'failed') existing.failed += 1;
            if (new Date(run.started_at) > new Date(existing.last_run)) {
              existing.last_run = run.started_at;
            }
            existing.avg_duration_seconds =
              (existing.avg_duration_seconds * (existing.total_runs - 1) + duration) /
              existing.total_runs;
            existing.total_new_records += run.records_new || 0;
          }
        }

        const merged = Array.from(agentMap.values()).map((h) => ({
          ...h,
          ...scheduleMap[h.agent_name],
        }));
        setHealthData(merged);
      }
    }

    // Recent runs
    const { data: recent } = await supabase
      .from('agent_runs')
      .select('*')
      .order('started_at', { ascending: false })
      .limit(10);

    if (recent) {
      setRecentRuns(recent as AgentRun[]);
    }

    setLoading(false);
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const statusBadge = (status: string) => {
    const styles: Record<string, { bg: string; color: string; border: string }> = {
      success: { bg: 'rgba(91, 217, 168, 0.12)', color: '#5BD9A8', border: 'rgba(91, 217, 168, 0.35)' },
      failed: { bg: 'rgba(229, 75, 75, 0.12)', color: '#FF8585', border: 'rgba(229, 75, 75, 0.35)' },
      running: { bg: 'rgba(91, 156, 255, 0.12)', color: '#8DB8FF', border: 'rgba(91, 156, 255, 0.35)' },
    };
    const s = styles[status] || styles.running;
    return (
      <span
        className="text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded border"
        style={{ background: s.bg, color: s.color, borderColor: s.border }}
      >
        {status}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-32">
        <div className="flex items-center gap-3 text-text-tertiary">
          <span className="w-1.5 h-1.5 rounded-full bg-[var(--hc-peach)] hc-pulse" />
          Loading agent health…
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-7">
      {/* Header */}
      <header>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
          Agents
        </p>
        <h1 className="text-3xl font-semibold tracking-tight text-text-primary leading-tight">
          Agent health
        </h1>
        <p className="text-[13px] text-text-secondary mt-1">
          Performance and execution history for all 17 autonomous agents
        </p>
      </header>

      {/* Agent cards grid */}
      {healthData.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {healthData.map((agent) => (
            <AgentStatusCard key={agent.agent_name} agent={agent} onRunComplete={fetchData} />
          ))}
        </div>
      ) : (
        <div className="surface p-12 text-center">
          <p className="text-text-secondary text-sm">No agent health data available</p>
          <p className="text-text-tertiary text-xs mt-1">Agent runs will appear here once agents start executing</p>
        </div>
      )}

      {/* Recent Runs Table */}
      <section>
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-text-tertiary mb-3">
          Recent runs · last 10 executions
        </h2>
        <div className="surface overflow-hidden">
          {recentRuns.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[640px]">
                <thead>
                  <tr className="border-b border-divider text-[9px] text-text-tertiary uppercase tracking-wider">
                    <th className="text-left px-4 py-3 font-semibold">Status</th>
                    <th className="text-left px-4 py-3 font-semibold">Agent</th>
                    <th className="text-left px-4 py-3 font-semibold">Started</th>
                    <th className="text-right px-4 py-3 font-semibold">Duration</th>
                    <th className="text-right px-4 py-3 font-semibold">Pulled</th>
                    <th className="text-right px-4 py-3 font-semibold">New</th>
                    <th className="text-left px-4 py-3 font-semibold hidden sm:table-cell">Error</th>
                  </tr>
                </thead>
                <tbody>
                  {recentRuns.map((run) => {
                    const duration =
                      run.started_at && run.finished_at
                        ? ((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000).toFixed(1)
                        : '—';
                    const startedAgo = formatDistanceToNow(new Date(run.started_at), { addSuffix: true });

                    return (
                      <tr key={run.id} className="border-b border-divider/50 hover:bg-[var(--bg-elevated-hover)] transition-colors">
                        <td className="px-4 py-2.5">{statusBadge(run.status)}</td>
                        <td className="px-4 py-2.5">
                          <span className="text-[12.5px] text-text-primary capitalize">
                            {run.agent_name.replace(/_/g, ' ')}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 text-[11.5px] text-text-tertiary">{startedAgo}</td>
                        <td className="px-4 py-2.5 text-right text-[11.5px] text-text-secondary tabular-nums">
                          {duration === '—' ? '—' : `${duration}s`}
                        </td>
                        <td className="px-4 py-2.5 text-right text-[11.5px] text-text-secondary tabular-nums">
                          {run.records_pulled.toLocaleString()}
                        </td>
                        <td className="px-4 py-2.5 text-right text-[11.5px] text-text-primary font-medium tabular-nums">
                          {run.records_new.toLocaleString()}
                        </td>
                        <td className="px-4 py-2.5 text-[10.5px] text-[#FF8585] max-w-[200px] truncate hidden sm:table-cell">
                          {run.error_message || '—'}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="px-4 py-12 text-center">
              <p className="text-text-tertiary text-sm">No recent runs</p>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
