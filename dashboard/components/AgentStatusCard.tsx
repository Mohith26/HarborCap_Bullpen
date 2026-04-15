'use client';

import { useState, useRef, useCallback } from 'react';
import { AgentHealth } from '@/lib/types';
import { formatDistanceToNow } from 'date-fns';
import { getAgent, CATEGORY_COLORS } from '@/lib/theme';

const SCHEDULER_API = process.env.NEXT_PUBLIC_SCHEDULER_API_URL || '';

function cronToLabel(cron: string): string {
  const parts = cron.split(' ');
  if (parts.length !== 5) return cron;
  const [min, hour, dom, , dow] = parts;

  if (min === '0' && hour === '*') return 'Hourly';
  if (dom !== '*' && dow === '*') return `${dom === '1' ? '1st' : `${dom}th`} of month · ${hour}:${min.padStart(2, '0')} CT`;
  if (dow !== '*') return `Mondays · ${hour}:${min.padStart(2, '0')} CT`;
  if (hour !== '*') return `Daily · ${hour}:${min.padStart(2, '0')} CT`;
  return cron;
}

interface Props {
  agent: AgentHealth;
  onRunComplete?: () => void;
}

type TriggerStatus = 'idle' | 'starting' | 'running' | 'completed' | 'already_running' | 'error';

export default function AgentStatusCard({ agent, onRunComplete }: Props) {
  const [triggerStatus, setTriggerStatus] = useState<TriggerStatus>('idle');
  const [triggerMessage, setTriggerMessage] = useState('');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const handleRunNow = async () => {
    if (!SCHEDULER_API) return;
    stopPolling();
    setTriggerStatus('starting');
    setTriggerMessage('');

    try {
      const resp = await fetch(`${SCHEDULER_API}/agents/${agent.agent_name}/run`, { method: 'POST' });

      if (resp.ok) {
        setTriggerStatus('running');
        setTriggerMessage('Agent running…');

        let polls = 0;
        pollRef.current = setInterval(async () => {
          polls++;
          if (polls > 100) {
            stopPolling();
            setTriggerStatus('completed');
            setTriggerMessage('Still running (check logs)');
            setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 5000);
            onRunComplete?.();
            return;
          }
          try {
            const statusResp = await fetch(`${SCHEDULER_API}/agents/${agent.agent_name}/status`);
            if (statusResp.ok) {
              const { is_running } = await statusResp.json();
              if (!is_running) {
                stopPolling();
                setTriggerStatus('completed');
                setTriggerMessage('Run complete');
                onRunComplete?.();
                setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 4000);
              }
            }
          } catch { /* keep polling */ }
        }, 3000);

      } else if (resp.status === 409) {
        setTriggerStatus('already_running');
        setTriggerMessage('Already running');
        setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 4000);
      } else {
        const body = await resp.json().catch(() => ({}));
        setTriggerStatus('error');
        setTriggerMessage(body.detail || `Error ${resp.status}`);
        setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 5000);
      }
    } catch {
      setTriggerStatus('error');
      setTriggerMessage('Cannot reach scheduler');
      setTimeout(() => { setTriggerStatus('idle'); setTriggerMessage(''); }, 5000);
    }
  };

  const meta = getAgent(agent.agent_name);
  const catColor = CATEGORY_COLORS[meta.category];

  const successRate = agent.total_runs > 0 ? Math.round((agent.successful / agent.total_runs) * 100) : 0;
  const lastRunAgo = agent.last_run
    ? formatDistanceToNow(new Date(agent.last_run), { addSuffix: true })
    : 'Never';
  const isHealthy = agent.failed === 0 || successRate >= 80;

  const nextRunLabel = agent.next_run
    ? formatDistanceToNow(new Date(agent.next_run), { addSuffix: true })
    : null;
  const scheduleLabel = agent.cron ? cronToLabel(agent.cron) : null;

  // Hide 0.0s — show "—" instead
  const durationDisplay = agent.avg_duration_seconds > 0.05
    ? `${agent.avg_duration_seconds.toFixed(1)}s`
    : '—';

  const buttonStyle: React.CSSProperties =
    triggerStatus === 'completed' ? { background: 'rgba(91, 217, 168, 0.12)', borderColor: 'rgba(91, 217, 168, 0.4)', color: '#5BD9A8' } :
    triggerStatus === 'running' ? { background: 'rgba(91, 156, 255, 0.12)', borderColor: 'rgba(91, 156, 255, 0.4)', color: '#5B9CFF' } :
    triggerStatus === 'already_running' ? { background: 'rgba(255, 189, 96, 0.12)', borderColor: 'rgba(255, 189, 96, 0.4)', color: '#FFBD60' } :
    triggerStatus === 'error' ? { background: 'rgba(229, 75, 75, 0.12)', borderColor: 'rgba(229, 75, 75, 0.4)', color: '#FF8585' } :
    {};

  const buttonText =
    triggerStatus === 'starting' ? 'Starting…' :
    triggerStatus === 'running' ? 'Running…' :
    triggerStatus === 'completed' ? 'Completed' :
    triggerStatus === 'already_running' ? 'Already Running' :
    triggerStatus === 'error' ? 'Failed' :
    'Run Now';

  const isDisabled = triggerStatus === 'starting' || triggerStatus === 'running' || !SCHEDULER_API;

  return (
    <div className="surface p-4 flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-3 min-w-0 flex-1">
          <div
            className="shrink-0 w-9 h-9 rounded-lg flex items-center justify-center text-[10px] font-bold tracking-wider"
            style={{
              background: `${catColor}1A`,
              color: catColor,
              border: `1px solid ${catColor}33`,
            }}
          >
            {meta.short}
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-1.5 mb-0.5">
              <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${isHealthy ? 'bg-[#5BD9A8]' : 'bg-[#E54B4B]'}`} />
              <h3 className="text-[13.5px] font-semibold text-text-primary truncate">
                {meta.label}
              </h3>
            </div>
            <p className="text-[10px] text-text-tertiary uppercase tracking-wider truncate">
              {meta.category}
            </p>
          </div>
        </div>
      </div>

      {/* Stats grid — labels shortened to avoid truncation */}
      <div className="grid grid-cols-2 gap-x-3 gap-y-2.5">
        <div>
          <p className="text-[9px] font-semibold text-text-tertiary uppercase tracking-wider">Runs · 7d</p>
          <p className="text-lg font-semibold tabular-nums text-text-primary leading-tight">{agent.total_runs}</p>
        </div>
        <div>
          <p className="text-[9px] font-semibold text-text-tertiary uppercase tracking-wider">Success</p>
          <p
            className="text-lg font-semibold tabular-nums leading-tight"
            style={{
              color: successRate >= 80 ? '#5BD9A8' : successRate >= 50 ? '#FFBD60' : '#E54B4B',
            }}
          >
            {agent.total_runs > 0 ? `${successRate}%` : '—'}
          </p>
        </div>
        <div>
          <p className="text-[9px] font-semibold text-text-tertiary uppercase tracking-wider">Avg run</p>
          <p className="text-lg font-semibold tabular-nums text-text-primary leading-tight">{durationDisplay}</p>
        </div>
        <div>
          <p className="text-[9px] font-semibold text-text-tertiary uppercase tracking-wider">New rows</p>
          <p className="text-lg font-semibold tabular-nums text-text-primary leading-tight">
            {agent.total_new_records.toLocaleString()}
          </p>
        </div>
      </div>

      {/* Success/failure bar */}
      <div>
        <div className="flex h-1 rounded-full overflow-hidden bg-[var(--input-bg)]">
          {agent.total_runs > 0 && (
            <>
              <div className="bg-[#5BD9A8] transition-all" style={{ width: `${successRate}%` }} />
              <div className="bg-[#E54B4B] transition-all" style={{ width: `${100 - successRate}%` }} />
            </>
          )}
        </div>
        <div className="flex justify-between mt-1.5">
          <span className="text-[9px] text-text-tertiary">{agent.successful} passed</span>
          <span className="text-[9px] text-text-tertiary">{agent.failed} failed</span>
        </div>
      </div>

      {/* Schedule info */}
      <div className="border-t border-divider pt-3 space-y-1">
        <div className="flex items-center justify-between gap-2">
          <p className="text-[10.5px] text-text-tertiary truncate">
            Last: <span className="text-text-secondary">{lastRunAgo}</span>
          </p>
          {scheduleLabel && (
            <p className="text-[9px] text-text-tertiary uppercase tracking-wider truncate">{scheduleLabel}</p>
          )}
        </div>
        {nextRunLabel && (
          <p className="text-[10.5px] text-text-tertiary">
            Next: <span className="text-[var(--hc-peach)]">{nextRunLabel}</span>
          </p>
        )}
        {triggerMessage && (
          <p className="text-[10px] font-medium" style={{ color: buttonStyle.color || 'var(--text-tertiary)' }}>
            {triggerMessage}
          </p>
        )}
      </div>

      {/* Run Now button */}
      <button
        onClick={handleRunNow}
        disabled={isDisabled}
        className="w-full text-[11px] font-semibold px-3 py-2 rounded-lg border border-border bg-[var(--input-bg)] text-text-secondary hover:text-text-primary hover:border-border-strong transition-all disabled:opacity-50 disabled:cursor-not-allowed"
        style={buttonStyle.background ? buttonStyle : undefined}
        title={!SCHEDULER_API ? 'Set NEXT_PUBLIC_SCHEDULER_API_URL in env to enable' : `Trigger ${agent.agent_name} now`}
      >
        {buttonText}
      </button>
    </div>
  );
}
