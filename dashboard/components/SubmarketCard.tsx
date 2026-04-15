import Link from 'next/link';
import { Submarket } from '@/lib/theme';

interface Props {
  submarket: Submarket;
  signalCount: number;
  alertCount: number;
  topSignalTypes?: string[];
}

export default function SubmarketCard({ submarket, signalCount, alertCount, topSignalTypes = [] }: Props) {
  return (
    <Link
      href={`/submarkets/${submarket.slug}`}
      className="surface group p-5 block relative overflow-hidden hover:border-[var(--hc-peach)]/40 transition-all"
    >
      {alertCount > 0 && (
        <div
          className="absolute top-0 right-0 w-24 h-24 rounded-full opacity-10 blur-2xl"
          style={{ background: 'var(--hc-peach)' }}
        />
      )}

      <div className="relative">
        {/* Metro tag */}
        <div className="flex items-center gap-2 mb-3">
          <span className="text-[10px] font-semibold uppercase tracking-[0.12em] text-text-tertiary">
            {submarket.metro}
          </span>
          {alertCount > 0 && (
            <span className="inline-flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider text-[var(--hc-peach)]">
              <span className="w-1.5 h-1.5 rounded-full bg-[var(--hc-peach)] hc-pulse" />
              {alertCount} alert{alertCount > 1 ? 's' : ''}
            </span>
          )}
        </div>

        <h3 className="text-[17px] font-semibold text-text-primary tracking-tight mb-1 group-hover:text-[var(--hc-peach)] transition-colors">
          {submarket.name}
        </h3>

        <div className="flex items-baseline gap-2 mt-4">
          <span className="text-3xl font-semibold text-text-primary leading-none tabular-nums">
            {signalCount}
          </span>
          <span className="text-[11px] uppercase tracking-wider text-text-tertiary">
            signals · 30d
          </span>
        </div>

        {topSignalTypes.length > 0 && (
          <div className="mt-4 flex flex-wrap gap-1.5">
            {topSignalTypes.slice(0, 3).map((t) => (
              <span
                key={t}
                className="text-[10px] px-2 py-0.5 rounded bg-[var(--input-bg)] border border-border text-text-secondary"
              >
                {t.replace(/_/g, ' ')}
              </span>
            ))}
          </div>
        )}

        {/* Bottom arrow */}
        <div className="flex items-center justify-end mt-4 text-text-tertiary group-hover:text-[var(--hc-peach)] transition-colors">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M14 5l7 7m0 0l-7 7m7-7H3" />
          </svg>
        </div>
      </div>
    </Link>
  );
}
