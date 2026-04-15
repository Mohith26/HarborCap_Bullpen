/**
 * HarborCap brand tokens — sourced from the Logo Masters folder.
 * Use these constants instead of hardcoding hex values across components.
 */

export const BRAND = {
  peach: '#FFBD60',
  peachDim: '#D99B47',
  peachGlow: 'rgba(255, 189, 96, 0.15)',
  charcoal: '#1F1E1E',
  black: '#141414',
  slate: '#0E0E0E',
  softWhite: '#F2F0F0',
} as const;

export const SURFACE = {
  bg: '#0B0A0A',         // deepest body bg (slightly bluer than pure black)
  card: '#161514',       // card surface
  cardHover: '#1F1D1B',  // card hover
  border: '#2A2826',     // standard border
  borderStrong: '#3A3735', // emphasized border
  inputBg: '#1A1918',    // input fields
  divider: '#26241F',    // hairline dividers
} as const;

export const TEXT = {
  primary: '#F5F4F2',    // body text
  secondary: '#A8A39B',  // labels
  tertiary: '#6E6962',   // muted
  disabled: '#4A4540',   // disabled
} as const;

export type Severity = 'critical' | 'alert' | 'watch' | 'info';

export const SEVERITY: Record<Severity, {
  color: string;
  bg: string;
  border: string;
  text: string;
  label: string;
  radius: number;
}> = {
  critical: {
    color: '#E54B4B',
    bg: 'rgba(229, 75, 75, 0.12)',
    border: 'rgba(229, 75, 75, 0.35)',
    text: '#FF8585',
    label: 'Critical',
    radius: 13,
  },
  alert: {
    color: '#FFBD60',  // brand peach
    bg: 'rgba(255, 189, 96, 0.12)',
    border: 'rgba(255, 189, 96, 0.35)',
    text: '#FFD699',
    label: 'Alert',
    radius: 10,
  },
  watch: {
    color: '#5B9CFF',
    bg: 'rgba(91, 156, 255, 0.10)',
    border: 'rgba(91, 156, 255, 0.30)',
    text: '#8DB8FF',
    label: 'Watch',
    radius: 7,
  },
  info: {
    color: '#7A746C',
    bg: 'rgba(122, 116, 108, 0.10)',
    border: 'rgba(122, 116, 108, 0.30)',
    text: '#A8A39B',
    label: 'Info',
    radius: 5,
  },
};

export const SEVERITY_RANK: Record<Severity, number> = {
  critical: 0,
  alert: 1,
  watch: 2,
  info: 3,
};

/**
 * Per-agent display metadata. Falls back to a default for unknown agents.
 */
export interface AgentMeta {
  label: string;
  short: string;     // 2-letter code for compact badges
  category: 'demand' | 'supply' | 'risk' | 'macro' | 'deals';
  description: string;
}

export const AGENTS: Record<string, AgentMeta> = {
  building_permits: {
    label: 'Building Permits',
    short: 'BP',
    category: 'supply',
    description: 'Tracks new commercial construction permits',
  },
  materials_price: {
    label: 'Materials Price',
    short: 'MP',
    category: 'macro',
    description: 'FRED construction materials indices',
  },
  freight_volume: {
    label: 'Freight Volume',
    short: 'FV',
    category: 'demand',
    description: 'FRED freight transportation indices',
  },
  txdot_infra: {
    label: 'TxDOT Infra',
    short: 'TX',
    category: 'supply',
    description: 'TxDOT highway and infrastructure projects',
  },
  job_demand: {
    label: 'Job Demand',
    short: 'JD',
    category: 'demand',
    description: 'Indeed warehouse / logistics job clusters',
  },
  biz_registration: {
    label: 'Business Registration',
    short: 'BR',
    category: 'demand',
    description: 'TX Comptroller new sales-tax permits',
  },
  environmental_risk: {
    label: 'Environmental Risk',
    short: 'ER',
    category: 'risk',
    description: 'TCEQ contamination sites near targets',
  },
  flood_risk: {
    label: 'Flood Risk',
    short: 'FR',
    category: 'risk',
    description: 'FEMA flood zones at watch locations',
  },
  zoning_changes: {
    label: 'Zoning Changes',
    short: 'ZC',
    category: 'supply',
    description: 'Pending industrial rezoning cases',
  },
  census_demographics: {
    label: 'Census Demographics',
    short: 'CD',
    category: 'macro',
    description: 'ACS5 population + income shifts',
  },
  census_migration: {
    label: 'Census Migration',
    short: 'CM',
    category: 'macro',
    description: 'County-to-county migration flows',
  },
  irs_migration: {
    label: 'IRS Migration',
    short: 'IM',
    category: 'macro',
    description: 'High-income household migration',
  },
  hud_vacancy: {
    label: 'HUD Vacancy',
    short: 'HV',
    category: 'demand',
    description: 'USPS commercial vacancy crosswalk',
  },
  ercot_load: {
    label: 'ERCOT Grid Load',
    short: 'EL',
    category: 'macro',
    description: 'Texas grid load by weather zone',
  },
  sales_tax_revenue: {
    label: 'Sales Tax Revenue',
    short: 'ST',
    category: 'macro',
    description: 'TX Comptroller sales tax allocation',
  },
  toll_traffic: {
    label: 'Toll Traffic',
    short: 'TT',
    category: 'demand',
    description: 'TxDOT traffic counts on key corridors',
  },
  corporate_relocations: {
    label: 'Corporate Relocations',
    short: 'CR',
    category: 'deals',
    description: 'TX Governor + business journal announcements',
  },
};

export function getAgent(name: string): AgentMeta {
  return (
    AGENTS[name] || {
      label: name.replace(/_/g, ' '),
      short: name.slice(0, 2).toUpperCase(),
      category: 'macro',
      description: '',
    }
  );
}

export const CATEGORY_COLORS: Record<AgentMeta['category'], string> = {
  demand: '#5B9CFF',
  supply: '#FFBD60',
  risk: '#E54B4B',
  macro: '#9C8FFF',
  deals: '#5BD9A8',
};

/**
 * Canonical submarket list. Slugs match what agents write to signals.submarket.
 */
export interface Submarket {
  slug: string;
  name: string;
  metro: string;
  lat: number;
  lng: number;
}

export const SUBMARKETS: Submarket[] = [
  { slug: 'nw-houston', name: 'NW Houston Industrial', metro: 'Houston', lat: 29.85, lng: -95.55 },
  { slug: 'ne-houston', name: 'NE Houston Industrial', metro: 'Houston', lat: 29.85, lng: -95.20 },
  { slug: 'dfw-south', name: 'DFW South Industrial', metro: 'DFW', lat: 32.65, lng: -97.10 },
  { slug: 'austin-east', name: 'Austin East', metro: 'Austin', lat: 30.25, lng: -97.60 },
  { slug: 'san-antonio-ne', name: 'San Antonio NE', metro: 'San Antonio', lat: 29.50, lng: -98.35 },
];

export function submarketBySlug(slug: string): Submarket | undefined {
  return SUBMARKETS.find((s) => s.slug === slug);
}

export function submarketByName(name: string | null | undefined): Submarket | undefined {
  if (!name) return undefined;
  return SUBMARKETS.find((s) => s.name === name);
}
