export interface Signal {
  id: string;
  agent_name: string;
  signal_type: string;
  severity: 'info' | 'watch' | 'alert' | 'critical';
  title: string;
  summary: string;
  property_id: string | null;
  location: { lat: number; lng: number } | null;
  submarket: string | null;
  data: Record<string, any>;
  source_url: string | null;
  created_at: string;
  acknowledged: boolean;
  acknowledged_by: string | null;
  notes: string | null;
}

export interface AgentRun {
  id: string;
  agent_name: string;
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'success' | 'failed';
  records_pulled: number;
  records_new: number;
  error_message: string | null;
}

export interface AgentHealth {
  agent_name: string;
  total_runs: number;
  successful: number;
  failed: number;
  last_run: string;
  avg_duration_seconds: number;
  total_new_records: number;
}

export interface MaterialPrice {
  id: string;
  series_id: string;
  series_name: string;
  observation_date: string;
  value: number;
  unit: string;
  yoy_change: number | null;
  mom_change: number | null;
}
