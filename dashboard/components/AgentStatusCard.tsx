'use client';

import { useState } from 'react';
import { AgentHealth } from '@/lib/types';
import { formatDistanceToNow } from 'date-fns';

const SCHEDULER_API = process.env.NEXT_PUBLIC_SCHEDULER_API_URL || '';

function cronToLabel(cron: string): string {
  const parts = cron.split(' ');
  if (parts.length !== 5) return cron;
  const [min, hour, dom, , dow] = parts;

  if (min === '0' && hour === '*') return 'Hourly';
  if (dom !== '*' && dow === '*') return `Monthly ${dom === '1' ? '1st' : `${dom}th`} ${hour}:${min.padStart(2, '0')} CT`;
  if (dow !== '*') return `Weekly Mon ${hour}:${min.padStart(2, '0')} CT`;
  if (hour !== '*') return `Daily ${hour}:${min.padStart(2, '0')} CT`;
  return cron;
}

export default function AgentStatusCard({ agent }: { agent: AgentHealth }) {
  const [triggering, setTriggering] = useState(false);
  const [triggerStatus, setTriggerStatus] = useState<'idle' | 'started' | 'running' | 'error'>('idle');
  const [triggerMessage, setTriggerMessage] = useState('');

  const handleRunNow = async () => {
    if (!SCHEDULER_API) return;
    setTriggering(true);
    setTriggerStatus('idle');
    setTriggerMessage('');

    try {
      const resp = await fetch(`${SCHEDULER_API}/agents/${agent.agent_name}/run`, { method: 'POST' });

      if (resp.ok) {
        setTriggerStatus('started');
        setTriggerMessage('Agent triggered');
      } else if (resp.status === 409) {
        setTriggerStatus('running');
        setTriggerMessage('Already running');
      } else {
        const body = await resp.json().catch(() => ({}));
        setTriggerStatus('error');
        setTriggerMessage(body.detail || `Error ${resp.status}`);
      }
    } catch {
      setTriggerStatus('error');
      setTriggerMessage('Cannot reach scheduler');
    }

    setTriggering(false);
    setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 5000);
  };

  const successRate = agent.total_runs > 0 ? Math.round((agent.successful / agent.total_runs) * 100) : 0;
  const lastRunAgo = agent.last_run
    ? formatDistanceToNow(new Date(agent.last_run), { addSuffix: true })
    : 'Never';
  const isHealthy = agent.failed === 0 || successRate >= 80;
  const displayName = agent.agent_name.replace(/_/g, ' ');

  const nextRunLabel = agent.next_run
    ? formatDistanceToNow(new Date(agent.next_run), { addSuffix: true })
    : null;
  const scheduleLabel = agent.cron ? cronToLabel(agent.cron) : null;

  const buttonColor =
    triggerStatus === 'started' ? 'bg-green-900/40 text-green-400 border-green-800' :
    triggerStatus === 'running' ? 'bg-yellow-900/40 text-yellow-400 border-yellow-800' :
    triggerStatus === 'error' ? 'bg-red-900/40 text-red-400 border-red-800' :
    'bg-gray-800 text-gray-300 border-gray-700 hover:bg-gray-700 hover:text-white';

  const buttonText =
    triggering ? 'Starting...' :
    triggerStatus === 'started' ? 'Started' :
    triggerStatus === 'running' ? 'Already Running' :
    triggerStatus === 'error' ? 'Failed' :
    'Run Now';

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
      <div className="mb-3">
        <div className="flex h-2 rounded-full overflow-hidden bg-gray-800">
          {agent.total_runs > 0 && (
            <>
              <div className="bg-green-500 transition-all" style={{ width: `${successRate}%` }} />
              <div className="bg-red-500 transition-all" style={{ width: `${100 - successRate}%` }} />
            </>
          )}
        </div>
        <div className="flex justify-between mt-1">
          <span className="text-[10px] text-gray-500">{agent.successful} passed</span>
          <span className="text-[10px] text-gray-500">{agent.failed} failed</span>
        </div>
      </div>

      {/* Schedule info */}
      <div className="border-t border-gray-800 pt-3 space-y-1.5">
        <div className="flex items-center justify-between">
          <p className="text-[11px] text-gray-500">
            Last run: <span className="text-gray-400">{lastRunAgo}</span>
          </p>
          {scheduleLabel && (
            <p className="text-[10px] text-gray-600">{scheduleLabel}</p>
          )}
        </div>

        {nextRunLabel && (
          <p className="text-[11px] text-gray-500">
            Next run: <span className="text-blue-400">{nextRunLabel}</span>
          </p>
        )}

        {triggerMessage && (
          <p className={`text-[10px] ${triggerStatus === 'started' ? 'text-green-400' : triggerStatus === 'running' ? 'text-yellow-400' : 'text-red-400'}`}>
            {triggerMessage}
          </p>
        )}
      </div>

      {/* Run Now button */}
      <button
        onClick={handleRunNow}
        disabled={triggering || !SCHEDULER_API}
        className={`w-full mt-3 text-[11px] font-medium px-3 py-1.5 rounded border transition-colors ${buttonColor} disabled:opacity-40 disabled:cursor-not-allowed`}
        title={!SCHEDULER_API ? 'Set NEXT_PUBLIC_SCHEDULER_API_URL in env to enable' : `Trigger ${agent.agent_name} now`}
      >
        {buttonText}
      </button>
    </div>
  );
}
