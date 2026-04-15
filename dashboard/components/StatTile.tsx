interface Props {
  label: string;
  value: string | number;
  delta?: { value: string; direction: 'up' | 'down' | 'flat' };
  hint?: string;
  accent?: boolean;
  icon?: React.ReactNode;
}

export default function StatTile({ label, value, delta, hint, accent, icon }: Props) {
  const deltaColor =
    delta?.direction === 'up'
      ? 'text-[#5BD9A8]'
      : delta?.direction === 'down'
      ? 'text-[#E54B4B]'
      : 'text-text-tertiary';

  return (
    <div
      className="surface p-5 relative overflow-hidden"
      style={accent ? { borderColor: 'rgba(255, 189, 96, 0.4)' } : undefined}
    >
      {accent && (
        <div
          className="absolute -top-12 -right-12 w-32 h-32 rounded-full opacity-10 blur-2xl"
          style={{ background: 'var(--hc-peach)' }}
        />
      )}
      <div className="relative">
        <div className="flex items-center justify-between mb-3">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-text-tertiary">
            {label}
          </p>
          {icon && <span className="text-text-tertiary">{icon}</span>}
        </div>
        <p className="text-3xl font-semibold tracking-tight text-text-primary leading-none">
          {value}
        </p>
        <div className="flex items-center gap-2 mt-2">
          {delta && (
            <span className={`text-[11px] font-medium ${deltaColor}`}>
              {delta.direction === 'up' ? '↑' : delta.direction === 'down' ? '↓' : '·'} {delta.value}
            </span>
          )}
          {hint && <span className="text-[11px] text-text-tertiary">{hint}</span>}
        </div>
      </div>
    </div>
  );
}
