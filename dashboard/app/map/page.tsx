'use client';

import { useEffect, useRef, useState, useMemo } from 'react';
import { supabase } from '@/lib/supabase';
import { formatDistanceToNow } from 'date-fns';
import { SEVERITY, getAgent, type Severity } from '@/lib/theme';

interface MapSignal {
  id: string;
  agent_name: string;
  signal_type: string;
  severity: Severity;
  title: string;
  summary: string;
  submarket: string | null;
  created_at: string;
  lat: number;
  lng: number;
}

const SEVERITIES: Severity[] = ['critical', 'alert', 'watch', 'info'];

export default function MapPage() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const mapRef = useRef<any>(null);
  const [signals, setSignals] = useState<MapSignal[]>([]);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [noToken, setNoToken] = useState(false);
  const [severityFilter, setSeverityFilter] = useState<Set<Severity>>(new Set(SEVERITIES));
  const [agentFilter, setAgentFilter] = useState<string>('all');

  // Fetch signals
  useEffect(() => {
    const token = process.env.NEXT_PUBLIC_MAPBOX_TOKEN;
    if (!token) {
      setNoToken(true);
      return;
    }
    async function fetchSignals() {
      const { data } = await supabase.rpc('signals_with_coords');
      if (data) setSignals(data as MapSignal[]);
    }
    fetchSignals();
  }, []);

  // Init map
  useEffect(() => {
    const token = process.env.NEXT_PUBLIC_MAPBOX_TOKEN;
    if (!token || !mapContainer.current || mapRef.current) return;

    let cancelled = false;
    import('mapbox-gl').then((mapboxgl) => {
      if (cancelled || !mapContainer.current) return;
      import('mapbox-gl/dist/mapbox-gl.css');
      mapboxgl.default.accessToken = token;

      const map = new mapboxgl.default.Map({
        container: mapContainer.current,
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [-97.5, 31.2],
        zoom: 5.4,
        attributionControl: false,
      });

      map.addControl(new mapboxgl.default.NavigationControl({ showCompass: false }), 'top-right');
      map.on('load', () => setMapLoaded(true));
      mapRef.current = map;
    });

    return () => {
      cancelled = true;
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
    };
  }, [noToken]);

  // Filtered signals
  const filtered = useMemo(() => {
    return signals.filter((s) => {
      if (!severityFilter.has(s.severity)) return false;
      if (agentFilter !== 'all' && s.agent_name !== agentFilter) return false;
      return true;
    });
  }, [signals, severityFilter, agentFilter]);

  // Render layer
  useEffect(() => {
    if (!mapRef.current || !mapLoaded) return;
    const map = mapRef.current;

    const geojson = {
      type: 'FeatureCollection' as const,
      features: filtered
        .filter((s) => s.lat != null && s.lng != null)
        .map((s) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [s.lng, s.lat] },
          properties: {
            id: s.id,
            title: s.title,
            summary: s.summary || '',
            severity: s.severity,
            agent_name: s.agent_name,
            agent_label: getAgent(s.agent_name).label,
            created_at: s.created_at,
            color: SEVERITY[s.severity]?.color || '#7A746C',
            radius: SEVERITY[s.severity]?.radius || 5,
          },
        })),
    };

    const source = map.getSource('signals');
    if (source) {
      source.setData(geojson);
    } else {
      map.addSource('signals', { type: 'geojson', data: geojson });

      // Outer halo for critical / alert
      map.addLayer({
        id: 'signals-halo',
        type: 'circle',
        source: 'signals',
        filter: ['in', ['get', 'severity'], ['literal', ['critical', 'alert']]],
        paint: {
          'circle-color': ['get', 'color'],
          'circle-radius': ['*', ['get', 'radius'], 2.4],
          'circle-opacity': 0.18,
          'circle-blur': 0.6,
        },
      });

      map.addLayer({
        id: 'signals-circle',
        type: 'circle',
        source: 'signals',
        paint: {
          'circle-color': ['get', 'color'],
          'circle-radius': ['get', 'radius'],
          'circle-opacity': 0.92,
          'circle-stroke-width': 1.5,
          'circle-stroke-color': '#0B0A0A',
          'circle-stroke-opacity': 0.7,
        },
      });

      // Click → popup
      map.on('click', 'signals-circle', (e: any) => {
        if (!e.features || e.features.length === 0) return;
        const feature = e.features[0];
        const coords = feature.geometry.coordinates.slice();
        const props = feature.properties;
        const sev = SEVERITY[props.severity as Severity];
        const timeAgo = formatDistanceToNow(new Date(props.created_at), { addSuffix: true });

        import('mapbox-gl').then((mapboxgl) => {
          new mapboxgl.default.Popup({ offset: 15, maxWidth: '320px', closeButton: true })
            .setLngLat(coords)
            .setHTML(
              `<div style="font-family: Inter, system-ui, sans-serif; min-width: 240px;">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
                  <span style="display:inline-flex;align-items:center;gap:6px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:${sev.text};background:${sev.bg};border:1px solid ${sev.border};padding:3px 8px;border-radius:5px;">
                    <span style="width:5px;height:5px;border-radius:50%;background:${sev.color};display:inline-block;"></span>
                    ${sev.label}
                  </span>
                  <span style="font-size:10px;color:#6E6962;margin-left:auto;">${timeAgo}</span>
                </div>
                <h3 style="font-size:13px;font-weight:600;color:#F5F4F2;margin:0 0 6px 0;line-height:1.35;">${props.title}</h3>
                <p style="font-size:11.5px;color:#A8A39B;margin:0 0 10px 0;line-height:1.5;">${props.summary || ''}</p>
                <span style="display:inline-block;font-size:10px;font-weight:500;color:#A8A39B;background:#1A1918;border:1px solid #2A2826;padding:3px 8px;border-radius:4px;">${props.agent_label}</span>
              </div>`
            )
            .addTo(map);
        });
      });

      map.on('mouseenter', 'signals-circle', () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', 'signals-circle', () => { map.getCanvas().style.cursor = ''; });
    }
  }, [filtered, mapLoaded]);

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

  const uniqueAgents = useMemo(() => {
    const set = new Set<string>();
    signals.forEach((s) => set.add(s.agent_name));
    return Array.from(set).sort();
  }, [signals]);

  if (noToken) {
    return (
      <div className="flex items-center justify-center min-h-[calc(100vh-8rem)]">
        <div className="surface p-8 max-w-md text-center">
          <svg className="w-12 h-12 text-text-tertiary mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.75V15m6-6v8.25m.5 3.5L20.4 18.3c.4-.2.6-.6.6-1V4.8c0-.8-.9-1.4-1.6-1L15.5 5.7c-.3.2-.7.2-1 0L9.5 3.3a1.1 1.1 0 0 0-1 0L3.6 5.7c-.4.2-.6.6-.6 1v12.5c0 .8.9 1.4 1.6 1l3.9-1.9c.3-.2.7-.2 1 0l5 2.5c.3.2.7.2 1 0z" />
          </svg>
          <h2 className="text-lg font-semibold text-text-primary mb-2">Map View Unavailable</h2>
          <p className="text-sm text-text-secondary">
            Set <code className="text-[var(--hc-peach)] bg-[var(--input-bg)] px-1.5 py-0.5 rounded text-xs">NEXT_PUBLIC_MAPBOX_TOKEN</code> in{' '}
            <code className="text-text-secondary bg-[var(--input-bg)] px-1.5 py-0.5 rounded text-xs">.env.local</code>{' '}
            to enable the map.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Header (in-flow, not absolute) */}
      <header className="flex items-end justify-between flex-wrap gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--hc-peach)] mb-1.5">
            Map
          </p>
          <h1 className="text-2xl md:text-3xl font-semibold tracking-tight text-text-primary leading-tight">
            Texas signal map
          </h1>
          <p className="text-[13px] text-text-secondary mt-1">
            {filtered.length} of {signals.length} signals shown
          </p>
        </div>
      </header>

      {/* Filter bar */}
      <div className="surface p-3 flex items-center gap-3 flex-wrap">
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

        <select
          value={agentFilter}
          onChange={(e) => setAgentFilter(e.target.value)}
          className="bg-[var(--input-bg)] border border-border rounded-lg px-3 py-1.5 text-[12px] text-text-primary focus:outline-none focus:border-[var(--hc-peach)]/40 cursor-pointer"
        >
          <option value="all">All agents</option>
          {uniqueAgents.map((a) => (
            <option key={a} value={a}>{getAgent(a).label}</option>
          ))}
        </select>

        <span className="text-[10px] text-text-tertiary uppercase tracking-wider ml-auto">
          {signals.length} total · click marker for details
        </span>
      </div>

      {/* Map container */}
      <div className="relative surface overflow-hidden" style={{ height: 'calc(100vh - 240px)', minHeight: '520px' }}>
        <div ref={mapContainer} className="w-full h-full" />
      </div>
    </div>
  );
}
