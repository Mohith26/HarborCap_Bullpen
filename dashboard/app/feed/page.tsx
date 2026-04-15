'use client';

import { useEffect, useState, useMemo } from 'react';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import SignalCard from '@/components/SignalCard';
import SignalGroup from '@/components/SignalGroup';
import { SEVERITY_RANK, SEVERITY, AGENTS, type Severity } from '@/lib/theme';

const SEVERITIES: Severity[] = ['critical', 'alert', 'watch', 'info'];
const SIGNAL_TYPES = ['All Types', 'corporate_relocation', 'permit_filed', 'building_permit', 'job_cluster', 'environmental_risk', 'txdot_project', 'material_price_spike', 'flood_risk', 'demographic_shift', 'irs_migration_shift', 'new_business', 'freight_shift', 'grid_load_shift'];

type GroupingMode = 'submarket' | 'agent' | 'severity' | 'flat';

export default function FeedPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);
  const [typeFilter, setTypeFilter] = useState('All Types');
  const [severityFilter, setSeverityFilter] = useState<Set<Severity>>(new Set(SEVERITIES));
  const [searchQuery, setSearchQuery] = useState('');
  const [grouping, setGrouping] = useState<GroupingMode>('submarket');
  const [hideAcked, setHideAcked] = useState(true);

  useEffect(() => {
    async function fetchSignals() {
      const { data } = await supabase
        .from('signals')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(500);
      if (data) setSignals(data as Signal[]);
      setLoading(false);
    }
    fetchSignals();

    const channel = supabase
      .channel('signals-realtime-feed')
      .on(
        'postgres_changes',
        { event: 'INSERT', schema: 'public', table: 'signals' },
        (payload) => {
          setSignals((prev) => [payload.new as Signal, ...prev].slice(0, 500));
        }
      )
      .subscribe();

    return () => { supabase.removeChannel(channel); };
  }, []);

  const toggleSeverity = (sev: Severity) => {
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
      if (!severityFilter.has(s.severity as Severity)) return false;
      if (hideAcked && s.acknowledged) return false;
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        return (
          s.title.toLowerCase().includes(q) ||
          (s.summary || '').toLowerCase().includes(q) ||
          s.agent_name.toLowerCase().includes(q) ||
          (s.submarket && s.submarket.toLowerCase().includes(q))
        );
      }
      return true;
    });
  }, [signals, typeFilter, severityFilter, searchQuery, hideAcked]);

  // Build pinned alerts (always at top, unfiltered by acknowledged)
  const pinnedAlerts = useMemo(() => {
    return filteredSignals
      .filter((s) => s.severity === 'critical' || s.severity === 'alert')
      .slice(0, 8);
  }, [filteredSignals]);

  // Build grouped signals (everything that isn't pinned)
  const groups = useMemo(() => {
    const remaining = filteredSignals.filter((s) => s.severity !== 'critical' && s.severity !== 'alert');
    const map = new Map<string, Signal[]>();

    for (const s of remaining) {
      let key: string;
      if (grouping === 'submarket') {
        key = s.submarket || 'Unassigned';
      } else if (grouping === 'agent') {
        key = AGENTS[s.agent_name]?.label || s.agent_name;
      } else if (grouping === 'severity') {
        key = SEVERITY[s.severity as Severity]?.label || s.severity;
      } else {
        key = 'All';
      }
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(s);
    }

    // Compute the highest severity per group
    const result = Array.from(map.entries())
      .map(([key, sigs]) => {
        const ranks = sigs.map((s) => SEVERITY_RANK[s.severity as Severity] ?? 3);
        const minRank = Math.min(...ranks);
        const highest = (Object.entries(SEVERITY_RANK).find(([, v]) => v === minRank)?.[0] || 'info') as Severity;
        return { key, signals: sigs, highest };
      })
      .sort((a, b) => {
        // Sort by alert count desc
        const ca = a.signals.length;
        const cb = b.signals.length;
        return cb - ca;
      });

    return result;
  }, [filteredSignals, grouping]);

  const totalUnacked = signals.filter((s) => !s.acknowledged).length;

  return (
    <div className="space-y-6">
      {/* Header */}
      <header className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
            Signal Feed
          </p>
          <h1 className="text-2xl md:text-3xl font-semibold tracking-tight text-text-primary leading-tight">
            {filteredSignals.length} signal{filteredSignals.length !== 1 ? 's' : ''}
          </h1>
          <p className="text-[13px] text-text-secondary mt-1">
            {totalUnacked} unacknowledged · grouped by {grouping}
          </p>
        </div>
      </header>

      {/* Filter bar */}
      <div className="surface p-3 md:p-4 space-y-3">
        <div className="flex flex-col md:flex-row gap-3 items-stretch md:items-center">
          {/* Search */}
          <div className="relative flex-1 min-w-0">
            <svg
              className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-tertiary pointer-events-none"
              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5-5m2-5a7 7 0 1 1-14 0 7 7 0 0 1 14 0z" />
            </svg>
            <input
              type="text"
              placeholder="Search signals…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-[var(--input-bg)] border border-border rounded-lg pl-9 pr-3 py-2 text-[13px] text-text-primary placeholder-text-tertiary focus:outline-none focus:border-[var(--hc-peach)]/40 focus:ring-1 focus:ring-[var(--hc-peach)]/20"
            />
          </div>

          {/* Type filter */}
          <select
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            className="bg-[var(--input-bg)] border border-border rounded-lg px-3 py-2 text-[13px] text-text-primary focus:outline-none focus:border-[var(--hc-peach)]/40 cursor-pointer"
          >
            {SIGNAL_TYPES.map((t) => (
              <option key={t} value={t}>
                {t === 'All Types' ? t : t.replace(/_/g, ' ')}
              </option>
            ))}
          </select>

          {/* Group by */}
          <select
            value={grouping}
            onChange={(e) => setGrouping(e.target.value as GroupingMode)}
            className="bg-[var(--input-bg)] border border-border rounded-lg px-3 py-2 text-[13px] text-text-primary focus:outline-none focus:border-[var(--hc-peach)]/40 cursor-pointer"
          >
            <option value="submarket">Group by submarket</option>
            <option value="agent">Group by agent</option>
            <option value="severity">Group by severity</option>
            <option value="flat">Flat list</option>
          </select>
        </div>

        {/* Severity chips + ack toggle */}
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex gap-1.5">
            {SEVERITIES.map((sev) => {
              const active = severityFilter.has(sev);
              const s = SEVERITY[sev];
              return (
                <button
                  key={sev}
                  onClick={() => toggleSeverity(sev)}
                  className="text-[10px] font-bold uppercase tracking-wider rounded-md border px-2.5 py-1 transition-all"
                  style={{
                    background: active ? s.bg : 'transparent',
                    borderColor: active ? s.border : 'var(--border)',
                    color: active ? s.text : 'var(--text-tertiary)',
                  }}
                >
                  <span
                    className="inline-block w-1.5 h-1.5 rounded-full mr-1.5 align-middle"
                    style={{ background: active ? s.color : 'var(--text-tertiary)' }}
                  />
                  {s.label}
                </button>
              );
            })}
          </div>

          <label className="flex items-center gap-2 text-[11px] text-text-secondary cursor-pointer select-none">
            <input
              type="checkbox"
              checked={hideAcked}
              onChange={(e) => setHideAcked(e.target.checked)}
              className="rounded border-border bg-[var(--input-bg)] accent-[var(--hc-peach)]"
            />
            Hide acknowledged
          </label>
        </div>
      </div>

      {/* Pinned alerts strip */}
      {pinnedAlerts.length > 0 && (
        <section>
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--hc-peach)] mb-2.5">
            Needs attention · {pinnedAlerts.length}
          </h2>
          <div className="grid md:grid-cols-2 gap-2.5">
            {pinnedAlerts.map((s) => (
              <SignalCard key={s.id} signal={s} />
            ))}
          </div>
        </section>
      )}

      {/* Groups */}
      {loading ? (
        <div className="flex items-center justify-center py-20 text-text-tertiary text-sm">
          Loading signals…
        </div>
      ) : groups.length === 0 && pinnedAlerts.length === 0 ? (
        <div className="surface p-12 text-center">
          <p className="text-text-secondary text-sm">No signals match your filters</p>
          <p className="text-text-tertiary text-xs mt-1">Try clearing filters or running an agent</p>
        </div>
      ) : (
        <div className="space-y-3">
          {groups.map((g) => (
            <SignalGroup
              key={g.key}
              title={g.key}
              subtitle={`${g.signals.length} signal${g.signals.length !== 1 ? 's' : ''}`}
              signals={g.signals}
              highestSeverity={g.highest}
              collapsedByDefault={g.signals.length > 6}
              autoClusterThreshold={5}
            />
          ))}
        </div>
      )}
    </div>
  );
}
