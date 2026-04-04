'use client';

import { useEffect, useState } from 'react';
import { supabase } from '@/lib/supabase';
import { AgentHealth, AgentRun } from '@/lib/types';
import AgentStatusCard from '@/components/AgentStatusCard';
import { formatDistanceToNow } from 'date-fns';

export default function AgentHealthPage() {
  const [healthData, setHealthData] = useState<AgentHealth[]>([]);
  const [recentRuns, setRecentRuns] = useState<AgentRun[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchData() {
      // Try the view first, fall back to client-side aggregation
      const { data: healthView, error: healthError } = await supabase
        .from('v_agent_health')
        .select('*');

      if (!healthError && healthView && healthView.length > 0) {
        setHealthData(healthView as AgentHealth[]);
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

          setHealthData(Array.from(agentMap.values()));
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
    }

    fetchData();
  }, []);

  const statusDot = (status: string) => {
    const color = status === 'success' ? 'bg-green-500' : status === 'failed' ? 'bg-red-500' : 'bg-yellow-500';
    return <span className={`w-2 h-2 rounded-full inline-block ${color}`} />;
  };

  const statusBadge = (status: string) => {
    const styles: Record<string, string> = {
      success: 'bg-green-900/40 text-green-400 border-green-800',
      failed: 'bg-red-900/40 text-red-400 border-red-800',
      running: 'bg-yellow-900/40 text-yellow-400 border-yellow-800',
    };
    return (
      <span className={`text-[11px] font-medium px-2 py-0.5 rounded border ${styles[status] || styles.running}`}>
        {status}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="flex items-center gap-3 text-gray-500">
          <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading agent health data...
        </div>
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-white">Agent Health</h1>
        <p className="text-sm text-gray-500 mt-1">
          Monitor agent performance and run history
        </p>
      </div>

      {/* Agent cards grid */}
      {healthData.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
          {healthData.map((agent) => (
            <AgentStatusCard key={agent.agent_name} agent={agent} />
          ))}
        </div>
      ) : (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-8 text-center mb-8">
          <p className="text-gray-500 text-sm">No agent health data available</p>
          <p className="text-gray-600 text-xs mt-1">Agent runs will appear here once agents start executing</p>
        </div>
      )}

      {/* Recent Runs Table */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="text-sm font-semibold text-white">Recent Runs</h2>
          <p className="text-[11px] text-gray-500">Last 10 agent executions</p>
        </div>

        {recentRuns.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-800 text-[10px] text-gray-500 uppercase tracking-wider">
                  <th className="text-left px-4 py-2.5 font-medium">Status</th>
                  <th className="text-left px-4 py-2.5 font-medium">Agent</th>
                  <th className="text-left px-4 py-2.5 font-medium">Started</th>
                  <th className="text-right px-4 py-2.5 font-medium">Duration</th>
                  <th className="text-right px-4 py-2.5 font-medium">Records Pulled</th>
                  <th className="text-right px-4 py-2.5 font-medium">New Records</th>
                  <th className="text-left px-4 py-2.5 font-medium">Error</th>
                </tr>
              </thead>
              <tbody>
                {recentRuns.map((run) => {
                  const duration =
                    run.started_at && run.finished_at
                      ? ((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000).toFixed(1)
                      : '--';
                  const startedAgo = formatDistanceToNow(new Date(run.started_at), { addSuffix: true });

                  return (
                    <tr key={run.id} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                      <td className="px-4 py-2.5">{statusBadge(run.status)}</td>
                      <td className="px-4 py-2.5">
                        <span className="text-sm text-white capitalize">
                          {run.agent_name.replace(/_/g, ' ')}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-400">{startedAgo}</td>
                      <td className="px-4 py-2.5 text-right text-[12px] text-gray-400 font-mono">
                        {duration}s
                      </td>
                      <td className="px-4 py-2.5 text-right text-[12px] text-gray-400 font-mono">
                        {run.records_pulled}
                      </td>
                      <td className="px-4 py-2.5 text-right text-[12px] text-white font-mono font-medium">
                        {run.records_new}
                      </td>
                      <td className="px-4 py-2.5 text-[11px] text-red-400 max-w-[200px] truncate">
                        {run.error_message || '--'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-4 py-8 text-center">
            <p className="text-gray-500 text-sm">No recent runs</p>
          </div>
        )}
      </div>
    </div>
  );
}
