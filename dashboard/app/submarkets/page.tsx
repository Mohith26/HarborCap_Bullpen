'use client';

import { useEffect, useState, useMemo } from 'react';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import { SUBMARKETS } from '@/lib/theme';
import SubmarketCard from '@/components/SubmarketCard';

export default function SubmarketsIndex() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
      const { data } = await supabase
        .from('signals')
        .select('*')
        .gte('created_at', since)
        .limit(2000);
      if (data) setSignals(data as Signal[]);
      setLoading(false);
    }
    load();
  }, []);

  const stats = useMemo(() => {
    const map = new Map<string, { total: number; alerts: number; types: Map<string, number> }>();
    for (const s of signals) {
      if (!s.submarket) continue;
      const existing = map.get(s.submarket) || { total: 0, alerts: 0, types: new Map() };
      existing.total += 1;
      if (s.severity === 'critical' || s.severity === 'alert') existing.alerts += 1;
      existing.types.set(s.signal_type, (existing.types.get(s.signal_type) || 0) + 1);
      map.set(s.submarket, existing);
    }
    return map;
  }, [signals]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-32 text-text-tertiary text-sm">
        Loading submarkets…
      </div>
    );
  }

  return (
    <div className="space-y-7">
      {/* Header */}
      <header>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
          Submarkets
        </p>
        <h1 className="text-3xl font-semibold tracking-tight text-text-primary leading-tight">
          Texas industrial corridors
        </h1>
        <p className="text-[13px] text-text-secondary mt-1">
          Signal activity across {SUBMARKETS.length} tracked submarkets · last 30 days
        </p>
      </header>

      {/* Submarket grid */}
      <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {SUBMARKETS.map((sm) => {
          const st = stats.get(sm.name) || { total: 0, alerts: 0, types: new Map() };
          const topTypes = Array.from(st.types.entries())
            .sort((a, b) => b[1] - a[1])
            .map(([type]) => type);

          return (
            <SubmarketCard
              key={sm.slug}
              submarket={sm}
              signalCount={st.total}
              alertCount={st.alerts}
              topSignalTypes={topTypes}
            />
          );
        })}
      </div>
    </div>
  );
}
