'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import StatTile from '@/components/StatTile';
import SignalCard from '@/components/SignalCard';
import { SUBMARKETS, SEVERITY_RANK, type Severity } from '@/lib/theme';

interface DashboardData {
  signals: Signal[];
  todaySignals: number;
  alertsThisWeek: number;
  acknowledged: number;
  total: number;
  agentRunsToday: number;
}

export default function DashboardHome() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const now = new Date();
      const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).toISOString();
      const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();

      const [signalsRes, totalRes, todayRes, weekAlertRes, ackRes, runsRes] = await Promise.all([
        supabase
          .from('signals')
          .select('*')
          .order('created_at', { ascending: false })
          .limit(200),
        supabase.from('signals').select('id', { count: 'exact', head: true }),
        supabase.from('signals').select('id', { count: 'exact', head: true }).gte('created_at', todayStart),
        supabase.from('signals').select('id', { count: 'exact', head: true }).in('severity', ['critical', 'alert']).gte('created_at', weekAgo),
        supabase.from('signals').select('id', { count: 'exact', head: true }).eq('acknowledged', true),
        supabase.from('agent_runs').select('id', { count: 'exact', head: true }).gte('started_at', todayStart),
      ]);

      setData({
        signals: (signalsRes.data || []) as Signal[],
        total: totalRes.count || 0,
        todaySignals: todayRes.count || 0,
        alertsThisWeek: weekAlertRes.count || 0,
        acknowledged: ackRes.count || 0,
        agentRunsToday: runsRes.count || 0,
      });
      setLoading(false);
    }
    load();
  }, []);

  // Top signals: critical + alert, then most recent
  const topSignals = useMemo(() => {
    if (!data) return [];
    return [...data.signals]
      .sort((a, b) => {
        const sa = SEVERITY_RANK[a.severity as Severity] ?? 3;
        const sb = SEVERITY_RANK[b.severity as Severity] ?? 3;
        if (sa !== sb) return sa - sb;
        return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
      })
      .slice(0, 6);
  }, [data]);

  // Submarket signal counts
  const submarketCounts = useMemo(() => {
    if (!data) return new Map<string, { total: number; alerts: number }>();
    const m = new Map<string, { total: number; alerts: number }>();
    for (const s of data.signals) {
      if (!s.submarket) continue;
      const existing = m.get(s.submarket) || { total: 0, alerts: 0 };
      existing.total += 1;
      if (s.severity === 'critical' || s.severity === 'alert') existing.alerts += 1;
      m.set(s.submarket, existing);
    }
    return m;
  }, [data]);

  if (loading || !data) {
    return (
      <div className="flex items-center justify-center py-32">
        <div className="flex items-center gap-3 text-text-tertiary">
          <span className="w-1.5 h-1.5 rounded-full bg-[var(--hc-peach)] hc-pulse" />
          Loading intelligence…
        </div>
      </div>
    );
  }

  const unacknowledged = data.total - data.acknowledged;
  const ackPct = data.total > 0 ? Math.round((data.acknowledged / data.total) * 100) : 0;

  return (
    <div className="space-y-8">
      {/* Page header */}
      <header className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
            HarborCap Bullpen
          </p>
          <h1 className="text-3xl font-semibold tracking-tight text-text-primary leading-tight">
            Today's intelligence
          </h1>
          <p className="text-[13px] text-text-secondary mt-1">
            Live signals from {data.agentRunsToday > 0 ? `${data.agentRunsToday} agent runs today` : '17 autonomous agents'} ·
            Last update {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </p>
        </div>
        <div className="flex gap-2">
          <Link
            href="/feed"
            className="text-[12px] font-medium px-3.5 py-2 rounded-lg border border-border bg-[var(--bg-elevated)] text-text-secondary hover:text-text-primary hover:border-border-strong transition-colors"
          >
            Open feed →
          </Link>
          <Link
            href="/map"
            className="text-[12px] font-medium px-3.5 py-2 rounded-lg border border-[var(--hc-peach)]/40 bg-[var(--hc-peach)]/10 text-[var(--hc-peach)] hover:bg-[var(--hc-peach)]/20 transition-colors"
          >
            Open map →
          </Link>
        </div>
      </header>

      {/* KPI strip */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 md:gap-4">
        <StatTile
          label="Total Signals"
          value={data.total.toLocaleString()}
          hint={`${data.todaySignals} today`}
        />
        <StatTile
          label="Alerts · 7d"
          value={data.alertsThisWeek}
          accent={data.alertsThisWeek > 0}
          hint="critical + alert"
        />
        <StatTile
          label="Unacknowledged"
          value={unacknowledged}
          delta={{ value: `${ackPct}% reviewed`, direction: 'flat' }}
        />
        <StatTile
          label="Live Agents"
          value="17"
          hint={`${data.agentRunsToday} runs today`}
        />
      </div>

      {/* Top Signals */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h2 className="text-[15px] font-semibold text-text-primary">Top signals</h2>
            <p className="text-[11px] text-text-tertiary">Critical and alert priority, newest first</p>
          </div>
          <Link href="/feed" className="text-[11px] font-medium text-[var(--hc-peach)] hover:text-[var(--hc-peach-dim)]">
            See all signals →
          </Link>
        </div>
        {topSignals.length === 0 ? (
          <div className="surface p-8 text-center text-text-tertiary text-sm">
            No signals yet. Run an agent from the Agents page to fetch live data.
          </div>
        ) : (
          <div className="grid md:grid-cols-2 gap-3">
            {topSignals.map((s) => (
              <SignalCard key={s.id} signal={s} />
            ))}
          </div>
        )}
      </section>

      {/* Submarket strip */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h2 className="text-[15px] font-semibold text-text-primary">Submarkets at a glance</h2>
            <p className="text-[11px] text-text-tertiary">Activity in your tracked Texas industrial corridors</p>
          </div>
          <Link href="/submarkets" className="text-[11px] font-medium text-[var(--hc-peach)] hover:text-[var(--hc-peach-dim)]">
            All submarkets →
          </Link>
        </div>
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
          {SUBMARKETS.map((sm) => {
            const counts = submarketCounts.get(sm.name) || { total: 0, alerts: 0 };
            return (
              <Link
                key={sm.slug}
                href={`/submarkets/${sm.slug}`}
                className="surface p-4 group hover:border-[var(--hc-peach)]/40 transition-colors"
              >
                <p className="text-[9px] font-semibold uppercase tracking-[0.12em] text-text-tertiary">
                  {sm.metro}
                </p>
                <h3 className="text-[13px] font-semibold text-text-primary mt-0.5 group-hover:text-[var(--hc-peach)] transition-colors leading-tight">
                  {sm.name.replace(' Industrial', '')}
                </h3>
                <div className="flex items-baseline gap-1.5 mt-3">
                  <span className="text-2xl font-semibold tabular-nums leading-none">
                    {counts.total}
                  </span>
                  <span className="text-[10px] text-text-tertiary uppercase tracking-wider">signals</span>
                </div>
                {counts.alerts > 0 && (
                  <p className="text-[10px] text-[var(--hc-peach)] mt-1.5 font-medium">
                    {counts.alerts} alert{counts.alerts > 1 ? 's' : ''}
                  </p>
                )}
              </Link>
            );
          })}
        </div>
      </section>
    </div>
  );
}
