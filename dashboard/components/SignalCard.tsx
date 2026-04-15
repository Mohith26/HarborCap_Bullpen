'use client';

import { Signal } from '@/lib/types';
import { supabase } from '@/lib/supabase';
import { formatDistanceToNow } from 'date-fns';
import { useState } from 'react';
import { SEVERITY, getAgent, CATEGORY_COLORS, type Severity } from '@/lib/theme';

interface Props {
  signal: Signal;
  compact?: boolean;
}

export default function SignalCard({ signal, compact = false }: Props) {
  const [acknowledged, setAcknowledged] = useState(signal.acknowledged);
  const [loading, setLoading] = useState(false);

  const handleAcknowledge = async () => {
    setLoading(true);
    const { error } = await supabase
      .from('signals')
      .update({ acknowledged: true, acknowledged_by: 'dashboard_user' })
      .eq('id', signal.id);

    if (!error) setAcknowledged(true);
    setLoading(false);
  };

  const sev = SEVERITY[signal.severity as Severity];
  const agent = getAgent(signal.agent_name);
  const catColor = CATEGORY_COLORS[agent.category];
  const timeAgo = formatDistanceToNow(new Date(signal.created_at), { addSuffix: true });

  return (
    <article
      className={`group relative surface overflow-hidden transition-all ${
        acknowledged ? 'opacity-60' : 'opacity-100'
      } ${compact ? 'p-3' : 'p-4'}`}
      style={{
        borderLeftWidth: '3px',
        borderLeftColor: sev.color,
      }}
    >
      {/* Header row */}
      <div className="flex items-start gap-3 mb-2">
        {/* Agent mark */}
        <div
          className="shrink-0 w-9 h-9 rounded-lg flex items-center justify-center text-[10px] font-bold tracking-wider mt-0.5"
          style={{
            background: `${catColor}1A`,
            color: catColor,
            border: `1px solid ${catColor}33`,
          }}
          title={agent.label}
        >
          {agent.short}
        </div>

        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 flex-wrap mb-0.5">
            <span className="text-[11px] font-medium text-text-secondary">
              {agent.label}
            </span>
            <span className="text-text-tertiary text-[10px]">·</span>
            <span className="text-[11px] text-text-tertiary">{timeAgo}</span>
            {signal.submarket && (
              <>
                <span className="text-text-tertiary text-[10px]">·</span>
                <span className="text-[11px] text-text-tertiary truncate">
                  {signal.submarket}
                </span>
              </>
            )}
          </div>
          <h3 className="text-[14px] font-semibold text-text-primary leading-snug line-clamp-2">
            {signal.title}
          </h3>
        </div>

        {/* Severity tag floats top right */}
        <div className="shrink-0">
          <span
            className="inline-flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded"
            style={{
              background: sev.bg,
              color: sev.text,
              border: `1px solid ${sev.border}`,
            }}
          >
            <span
              className="rounded-full w-1.5 h-1.5"
              style={{
                background: sev.color,
                boxShadow: signal.severity === 'critical' || signal.severity === 'alert'
                  ? `0 0 5px ${sev.color}`
                  : undefined,
              }}
            />
            {sev.label}
          </span>
        </div>
      </div>

      {/* Summary */}
      {!compact && signal.summary && (
        <p className="text-[12.5px] text-text-secondary line-clamp-2 leading-relaxed pl-12">
          {signal.summary}
        </p>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between mt-3 pl-12">
        {signal.source_url ? (
          <a
            href={signal.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-[11px] font-medium text-[var(--hc-peach)] hover:text-[var(--hc-peach-dim)] inline-flex items-center gap-1 transition-colors"
          >
            View source
            <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M14 5l7 7m0 0l-7 7m7-7H3" />
            </svg>
          </a>
        ) : (
          <span />
        )}
        {!acknowledged ? (
          <button
            onClick={handleAcknowledge}
            disabled={loading}
            className="text-[11px] font-medium px-2.5 py-1 rounded-md border border-border bg-[var(--input-bg)] text-text-secondary hover:text-text-primary hover:border-border-strong transition-colors disabled:opacity-50"
          >
            {loading ? 'Saving…' : 'Acknowledge'}
          </button>
        ) : (
          <span className="text-[11px] text-text-tertiary inline-flex items-center gap-1">
            <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
            Acknowledged
          </span>
        )}
      </div>
    </article>
  );
}
