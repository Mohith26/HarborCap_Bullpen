'use client';

import { useState } from 'react';
import { Signal } from '@/lib/types';
import SignalCard from './SignalCard';
import { SEVERITY, getAgent, type Severity } from '@/lib/theme';

interface Props {
  title: string;
  subtitle?: string;
  signals: Signal[];
  collapsedByDefault?: boolean;
  highestSeverity?: Severity;
  /** Auto-collapse if the group has more than this many signals of the same agent */
  autoClusterThreshold?: number;
}

export default function SignalGroup({
  title,
  subtitle,
  signals,
  collapsedByDefault = false,
  highestSeverity = 'info',
  autoClusterThreshold = 4,
}: Props) {
  const [open, setOpen] = useState(!collapsedByDefault);
  const sev = SEVERITY[highestSeverity];

  if (signals.length === 0) return null;

  // If most signals share the same agent, surface that as a cluster summary
  const agentCounts = signals.reduce<Record<string, number>>((acc, s) => {
    acc[s.agent_name] = (acc[s.agent_name] || 0) + 1;
    return acc;
  }, {});
  const topAgent = Object.entries(agentCounts).sort((a, b) => b[1] - a[1])[0];
  const isCluster = topAgent && topAgent[1] >= autoClusterThreshold;
  const topAgentMeta = topAgent ? getAgent(topAgent[0]) : null;

  return (
    <section className="surface overflow-hidden">
      {/* Header — clickable */}
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between gap-3 px-4 py-3 hover:bg-[var(--bg-elevated-hover)] transition-colors text-left"
      >
        <div className="flex items-center gap-3 min-w-0">
          <span
            className="shrink-0 rounded-full w-2 h-2"
            style={{
              background: sev.color,
              boxShadow: highestSeverity === 'critical' || highestSeverity === 'alert' ? `0 0 8px ${sev.color}` : undefined,
            }}
          />
          <div className="min-w-0">
            <h2 className="text-[13px] font-semibold text-text-primary truncate">
              {title}
            </h2>
            {subtitle && (
              <p className="text-[11px] text-text-tertiary truncate">{subtitle}</p>
            )}
          </div>
        </div>

        <div className="flex items-center gap-3 shrink-0">
          {isCluster && topAgentMeta && (
            <span className="hidden sm:inline-flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-text-tertiary">
              {topAgent[1]} from {topAgentMeta.label}
            </span>
          )}
          <span className="text-[11px] font-mono text-text-tertiary tabular-nums">
            {signals.length}
          </span>
          <svg
            className={`w-4 h-4 text-text-tertiary transition-transform ${open ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {open && (
        <div className="border-t border-divider p-3 space-y-2 bg-[var(--bg)]/40">
          {signals.map((s) => (
            <SignalCard key={s.id} signal={s} compact={isCluster} />
          ))}
        </div>
      )}
    </section>
  );
}
