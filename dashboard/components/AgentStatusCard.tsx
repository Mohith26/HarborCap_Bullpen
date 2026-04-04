'use client';

import { AgentHealth } from '@/lib/types';
import { formatDistanceToNow } from 'date-fns';

const agentDisplayNames: Record<string, string> = {
  permit_scanner: 'Permit Scanner',
  listing_tracker: 'Listing Tracker',
  demographic_monitor: 'Demographic Monitor',
  materials_watcher: 'Materials Watcher',
  court_monitor: 'Court Monitor',
};

export default function AgentStatusCard({ agent }: { agent: AgentHealth }) {
  const successRate = agent.total_runs > 0 ? Math.round((agent.successful / agent.total_runs) * 100) : 0;
  const lastRunAgo = agent.last_run
    ? formatDistanceToNow(new Date(agent.last_run), { addSuffix: true })
    : 'Never';
  const isHealthy = agent.failed === 0 || successRate >= 80;
  const displayName = agentDisplayNames[agent.agent_name] || agent.agent_name.replace(/_/g, ' ');

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-gray-700 transition-colors">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span
            className={`w-2.5 h-2.5 rounded-full ${
              isHealthy ? 'bg-green-500' : 'bg-red-500'
            }`}
          />
          <h3 className="text-sm font-semibold text-white capitalize">{displayName}</h3>
        </div>
        <span className={`text-[11px] font-medium px-2 py-0.5 rounded ${
          isHealthy
            ? 'bg-green-900/40 text-green-400 border border-green-800'
            : 'bg-red-900/40 text-red-400 border border-red-800'
        }`}>
          {isHealthy ? 'Healthy' : 'Degraded'}
        </span>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 gap-3 mb-4">
        <div>
          <p className="text-[10px] text-gray-500 uppercase tracking-wider">Total Runs (7d)</p>
          <p className="text-lg font-semibold text-white">{agent.total_runs}</p>
        </div>
        <div>
          <p className="text-[10px] text-gray-500 uppercase tracking-wider">Success Rate</p>
          <p className={`text-lg font-semibold ${successRate >= 80 ? 'text-green-400' : successRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
            {successRate}%
          </p>
        </div>
        <div>
          <p className="text-[10px] text-gray-500 uppercase tracking-wider">Avg Duration</p>
          <p className="text-lg font-semibold text-white">{agent.avg_duration_seconds.toFixed(1)}s</p>
        </div>
        <div>
          <p className="text-[10px] text-gray-500 uppercase tracking-wider">New Records</p>
          <p className="text-lg font-semibold text-white">{agent.total_new_records}</p>
        </div>
      </div>

      {/* Success/failure bar */}
      <div className="mb-2">
        <div className="flex h-2 rounded-full overflow-hidden bg-gray-800">
          {agent.total_runs > 0 && (
            <>
              <div
                className="bg-green-500 transition-all"
                style={{ width: `${successRate}%` }}
              />
              <div
                className="bg-red-500 transition-all"
                style={{ width: `${100 - successRate}%` }}
              />
            </>
          )}
        </div>
        <div className="flex justify-between mt-1">
          <span className="text-[10px] text-gray-500">{agent.successful} passed</span>
          <span className="text-[10px] text-gray-500">{agent.failed} failed</span>
        </div>
      </div>

      {/* Last run */}
      <p className="text-[11px] text-gray-500 mt-2">
        Last run: <span className="text-gray-400">{lastRunAgo}</span>
      </p>
    </div>
  );
}
