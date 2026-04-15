import { SEVERITY, type Severity } from '@/lib/theme';

interface Props {
  severity: Severity;
  size?: 'sm' | 'md';
  showLabel?: boolean;
}

export default function SeverityPill({ severity, size = 'sm', showLabel = true }: Props) {
  const s = SEVERITY[severity];
  const sizeClasses = size === 'sm'
    ? 'text-[10px] px-2 py-0.5 gap-1.5'
    : 'text-[11px] px-2.5 py-1 gap-2';

  return (
    <span
      className={`inline-flex items-center font-medium uppercase tracking-wider rounded-md border ${sizeClasses}`}
      style={{
        background: s.bg,
        borderColor: s.border,
        color: s.text,
      }}
    >
      <span
        className="rounded-full"
        style={{
          width: 5,
          height: 5,
          background: s.color,
          boxShadow: severity === 'critical' || severity === 'alert' ? `0 0 6px ${s.color}` : undefined,
        }}
      />
      {showLabel && s.label}
    </span>
  );
}
