'use client';

import { useEffect, useState, useMemo, use } from 'react';
import Link from 'next/link';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import SignalCard from '@/components/SignalCard';
import StatTile from '@/components/StatTile';
import { submarketBySlug, SUBMARKETS, AGENTS, SEVERITY_RANK, type Severity } from '@/lib/theme';
import { notFound } from 'next/navigation';

interface PageProps {
  params: Promise<{ slug: string }>;
}

export default function SubmarketDetail({ params }: PageProps) {
  const { slug } = use(params);
  const submarket = submarketBySlug(slug);
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!submarket) return;
    async function load() {
      const { data } = await supabase
        .from('signals')
        .select('*')
        .eq('submarket', submarket!.name)
        .order('created_at', { ascending: false })
        .limit(200);
      if (data) setSignals(data as Signal[]);
      setLoading(false);
    }
    load();
  }, [submarket]);

  const sortedSignals = useMemo(() => {
    return [...signals].sort((a, b) => {
      const sa = SEVERITY_RANK[a.severity as Severity] ?? 3;
      const sb = SEVERITY_RANK[b.severity as Severity] ?? 3;
      if (sa !== sb) return sa - sb;
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });
  }, [signals]);

  const stats = useMemo(() => {
    const total = signals.length;
    const critical = signals.filter((s) => s.severity === 'critical').length;
    const alerts = signals.filter((s) => s.severity === 'alert').length;
    const byAgent = new Map<string, number>();
    for (const s of signals) {
      byAgent.set(s.agent_name, (byAgent.get(s.agent_name) || 0) + 1);
    }
    const topAgents = Array.from(byAgent.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    return { total, critical, alerts, topAgents };
  }, [signals]);

  if (!submarket) {
    notFound();
  }

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <Link
        href="/submarkets"
        className="inline-flex items-center gap-1.5 text-[11px] font-medium text-text-tertiary hover:text-[var(--hc-peach)] transition-colors"
      >
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
        </svg>
        All submarkets
      </Link>

      {/* Header */}
      <header>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
          {submarket.metro}
        </p>
        <h1 className="text-3xl font-semibold tracking-tight text-text-primary leading-tight">
          {submarket.name}
        </h1>
        <p className="text-[13px] text-text-secondary mt-1">
          {submarket.lat.toFixed(2)}, {submarket.lng.toFixed(2)} · {stats.total} signal{stats.total !== 1 ? 's' : ''} in last 30 days
        </p>
      </header>

      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatTile label="Total Signals" value={stats.total} hint="last 30 days" />
        <StatTile label="Critical" value={stats.critical} accent={stats.critical > 0} />
        <StatTile label="Alerts" value={stats.alerts} accent={stats.alerts > 0} />
        <StatTile label="Active Agents" value={stats.topAgents.length} hint="reporting in" />
      </div>

      {/* Top agents in submarket */}
      {stats.topAgents.length > 0 && (
        <section>
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-text-tertiary mb-3">
            Most active agents
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
            {stats.topAgents.map(([name, count]) => {
              const meta = AGENTS[name];
              return (
                <div key={name} className="surface px-3 py-2.5">
                  <p className="text-[12px] font-medium text-text-primary truncate">
                    {meta?.label || name.replace(/_/g, ' ')}
                  </p>
                  <p className="text-[10px] text-text-tertiary mt-0.5">{count} signals</p>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {/* Signals list */}
      <section>
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-text-tertiary mb-3">
          Recent signals
        </h2>
        {loading ? (
          <div className="surface p-12 text-center text-text-tertiary text-sm">
            Loading signals…
          </div>
        ) : sortedSignals.length === 0 ? (
          <div className="surface p-12 text-center">
            <p className="text-text-secondary text-sm">No signals yet for this submarket</p>
          </div>
        ) : (
          <div className="grid md:grid-cols-2 gap-3">
            {sortedSignals.map((s) => (
              <SignalCard key={s.id} signal={s} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
