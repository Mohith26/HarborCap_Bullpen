'use client';

import { Signal } from '@/lib/types';
import { supabase } from '@/lib/supabase';
import { formatDistanceToNow } from 'date-fns';
import { useState } from 'react';

const severityColors: Record<Signal['severity'], string> = {
  critical: '#ef4444',
  alert: '#f59e0b',
  watch: '#3b82f6',
  info: '#6b7280',
};

const severityLabels: Record<Signal['severity'], string> = {
  critical: 'CRITICAL',
  alert: 'ALERT',
  watch: 'WATCH',
  info: 'INFO',
};

const agentColors: Record<string, string> = {
  permit_scanner: 'bg-purple-900/50 text-purple-300 border-purple-700',
  listing_tracker: 'bg-blue-900/50 text-blue-300 border-blue-700',
  demographic_monitor: 'bg-green-900/50 text-green-300 border-green-700',
  materials_watcher: 'bg-orange-900/50 text-orange-300 border-orange-700',
  court_monitor: 'bg-red-900/50 text-red-300 border-red-700',
};

function getAgentColor(agentName: string): string {
  return agentColors[agentName] || 'bg-gray-800 text-gray-300 border-gray-600';
}

export default function SignalCard({ signal }: { signal: Signal }) {
  const [acknowledged, setAcknowledged] = useState(signal.acknowledged);
  const [loading, setLoading] = useState(false);

  const handleAcknowledge = async () => {
    setLoading(true);
    const { error } = await supabase
      .from('signals')
      .update({ acknowledged: true, acknowledged_by: 'dashboard_user' })
      .eq('id', signal.id);

    if (!error) {
      setAcknowledged(true);
    }
    setLoading(false);
  };

  const borderColor = severityColors[signal.severity];
  const timeAgo = formatDistanceToNow(new Date(signal.created_at), { addSuffix: true });

  return (
    <div
      className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden hover:border-gray-700 transition-colors"
      style={{ borderLeftWidth: '4px', borderLeftColor: borderColor }}
    >
      <div className="p-4">
        {/* Top row: agent badge + severity + timestamp */}
        <div className="flex items-center gap-2 mb-2 flex-wrap">
          <span
            className={`inline-flex items-center px-2 py-0.5 rounded text-[11px] font-medium border ${getAgentColor(
              signal.agent_name
            )}`}
          >
            {signal.agent_name.replace(/_/g, ' ')}
          </span>
          <span className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider" style={{ color: borderColor }}>
            <span
              className="w-2 h-2 rounded-full inline-block"
              style={{ backgroundColor: borderColor }}
            />
            {severityLabels[signal.severity]}
          </span>
          {signal.submarket && (
            <span className="text-[11px] text-gray-500 bg-gray-800 px-2 py-0.5 rounded">
              {signal.submarket}
            </span>
          )}
          <span className="text-[11px] text-gray-500 ml-auto whitespace-nowrap">{timeAgo}</span>
        </div>

        {/* Title */}
        <h3 className="text-sm font-semibold text-white truncate mb-1">{signal.title}</h3>

        {/* Summary */}
        <p className="text-[13px] text-gray-400 line-clamp-3 leading-relaxed">{signal.summary}</p>

        {/* Footer: source link + acknowledge */}
        <div className="flex items-center justify-between mt-3 pt-3 border-t border-gray-800/50">
          <div>
            {signal.source_url && (
              <a
                href={signal.source_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[11px] text-blue-400 hover:text-blue-300 transition-colors"
              >
                View Source &rarr;
              </a>
            )}
          </div>
          {!acknowledged ? (
            <button
              onClick={handleAcknowledge}
              disabled={loading}
              className="text-[11px] font-medium px-3 py-1 rounded bg-gray-800 text-gray-300 hover:bg-gray-700 hover:text-white transition-colors disabled:opacity-50"
            >
              {loading ? 'Updating...' : 'Acknowledge'}
            </button>
          ) : (
            <span className="text-[11px] text-green-500 font-medium flex items-center gap-1">
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
              </svg>
              Acknowledged
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
