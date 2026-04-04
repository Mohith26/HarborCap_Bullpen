'use client';

import { useEffect, useState, useMemo } from 'react';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import SignalCard from '@/components/SignalCard';

const SEVERITY_ORDER: Record<string, number> = {
  critical: 0,
  alert: 1,
  watch: 2,
  info: 3,
};

const SIGNAL_TYPES = [
  'All Types',
  'new_permit',
  'permit_surge',
  'new_listing',
  'price_change',
  'demographic_shift',
  'material_price_alert',
  'court_filing',
];

const SEVERITIES: Signal['severity'][] = ['critical', 'alert', 'watch', 'info'];

const severityColors: Record<string, string> = {
  critical: 'bg-red-500/20 text-red-400 border-red-500/40',
  alert: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/40',
  watch: 'bg-blue-500/20 text-blue-400 border-blue-500/40',
  info: 'bg-gray-500/20 text-gray-400 border-gray-500/40',
};

const severityColorsActive: Record<string, string> = {
  critical: 'bg-red-500 text-white border-red-500',
  alert: 'bg-yellow-500 text-white border-yellow-500',
  watch: 'bg-blue-500 text-white border-blue-500',
  info: 'bg-gray-500 text-white border-gray-500',
};

export default function SignalFeedPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);
  const [typeFilter, setTypeFilter] = useState('All Types');
  const [severityFilter, setSeverityFilter] = useState<Set<Signal['severity']>>(new Set(SEVERITIES));
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    async function fetchSignals() {
      const { data, error } = await supabase
        .from('signals')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(200);

      if (!error && data) {
        const sorted = (data as Signal[]).sort((a, b) => {
          const sevDiff = (SEVERITY_ORDER[a.severity] ?? 3) - (SEVERITY_ORDER[b.severity] ?? 3);
          if (sevDiff !== 0) return sevDiff;
          return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
        });
        setSignals(sorted);
      }
      setLoading(false);
    }

    fetchSignals();

    // Realtime subscription for new signals
    const channel = supabase
      .channel('signals-realtime')
      .on(
        'postgres_changes',
        { event: 'INSERT', schema: 'public', table: 'signals' },
        (payload) => {
          const newSignal = payload.new as Signal;
          setSignals((prev) => {
            const updated = [newSignal, ...prev];
            return updated.sort((a, b) => {
              const sevDiff = (SEVERITY_ORDER[a.severity] ?? 3) - (SEVERITY_ORDER[b.severity] ?? 3);
              if (sevDiff !== 0) return sevDiff;
              return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
            });
          });
        }
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, []);

  const toggleSeverity = (sev: Signal['severity']) => {
    setSeverityFilter((prev) => {
      const next = new Set(prev);
      if (next.has(sev)) {
        if (next.size > 1) next.delete(sev);
      } else {
        next.add(sev);
      }
      return next;
    });
  };

  const filteredSignals = useMemo(() => {
    return signals.filter((s) => {
      if (typeFilter !== 'All Types' && s.signal_type !== typeFilter) return false;
      if (!severityFilter.has(s.severity)) return false;
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        return (
          s.title.toLowerCase().includes(q) ||
          s.summary.toLowerCase().includes(q) ||
          s.agent_name.toLowerCase().includes(q) ||
          (s.submarket && s.submarket.toLowerCase().includes(q))
        );
      }
      return true;
    });
  }, [signals, typeFilter, severityFilter, searchQuery]);

  const unacknowledgedCount = signals.filter((s) => !s.acknowledged).length;

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Signal Feed</h1>
          <p className="text-sm text-gray-500 mt-1">
            Real-time intelligence from all agents
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="text-right">
            <p className="text-2xl font-bold text-white">{signals.length}</p>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider">Total Signals</p>
          </div>
          <div className="w-px h-10 bg-gray-800" />
          <div className="text-right">
            <p className="text-2xl font-bold text-red-400">{unacknowledgedCount}</p>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider">Unacknowledged</p>
          </div>
        </div>
      </div>

      {/* Filter bar */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 mb-6">
        <div className="flex flex-col md:flex-row gap-4 items-start md:items-center">
          {/* Search */}
          <div className="relative flex-1 min-w-0">
            <svg
              className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
            <input
              type="text"
              placeholder="Search signals..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-gray-800 border border-gray-700 rounded-lg pl-10 pr-4 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-gray-600 focus:ring-1 focus:ring-gray-600"
            />
          </div>

          {/* Type filter */}
          <select
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-gray-600 cursor-pointer"
          >
            {SIGNAL_TYPES.map((t) => (
              <option key={t} value={t}>
                {t === 'All Types' ? t : t.replace(/_/g, ' ')}
              </option>
            ))}
          </select>

          {/* Severity chips */}
          <div className="flex gap-2">
            {SEVERITIES.map((sev) => {
              const active = severityFilter.has(sev);
              return (
                <button
                  key={sev}
                  onClick={() => toggleSeverity(sev)}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-semibold uppercase tracking-wider border transition-colors ${
                    active ? severityColorsActive[sev] : severityColors[sev]
                  }`}
                >
                  {sev}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Signal list */}
      {loading ? (
        <div className="flex items-center justify-center py-20">
          <div className="flex items-center gap-3 text-gray-500">
            <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Loading signals...
          </div>
        </div>
      ) : filteredSignals.length === 0 ? (
        <div className="text-center py-20">
          <p className="text-gray-500 text-sm">No signals match your filters</p>
          <p className="text-gray-600 text-xs mt-1">Try adjusting your search or filter criteria</p>
        </div>
      ) : (
        <div className="grid gap-3">
          {filteredSignals.map((signal) => (
            <SignalCard key={signal.id} signal={signal} />
          ))}
        </div>
      )}
    </div>
  );
}
