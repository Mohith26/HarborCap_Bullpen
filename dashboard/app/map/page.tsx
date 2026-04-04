'use client';

import { useEffect, useRef, useState } from 'react';
import { supabase } from '@/lib/supabase';
import { Signal } from '@/lib/types';
import { formatDistanceToNow } from 'date-fns';

const severityColors: Record<string, string> = {
  critical: '#ef4444',
  alert: '#f59e0b',
  watch: '#3b82f6',
  info: '#6b7280',
};

const severityRadius: Record<string, number> = {
  critical: 12,
  alert: 9,
  watch: 6,
  info: 4,
};

export default function MapPage() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const mapRef = useRef<any>(null);
  const [signals, setSignals] = useState<Signal[]>([]);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [noToken, setNoToken] = useState(false);

  useEffect(() => {
    const token = process.env.NEXT_PUBLIC_MAPBOX_TOKEN;
    if (!token) {
      setNoToken(true);
      return;
    }

    async function fetchSignals() {
      // Use RPC function that extracts lat/lng from PostGIS geography
      const { data } = await supabase.rpc('signals_with_coords');

      if (data) {
        setSignals(data as any[]);
      }
    }

    fetchSignals();
  }, []);

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
        center: [-97.0, 31.0],
        zoom: 6,
      });

      map.addControl(new mapboxgl.default.NavigationControl(), 'top-right');

      map.on('load', () => {
        setMapLoaded(true);
      });

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

  useEffect(() => {
    if (!mapRef.current || !mapLoaded || signals.length === 0) return;

    const map = mapRef.current;

    const geojson = {
      type: 'FeatureCollection' as const,
      features: signals
        .filter((s: any) => s.lat != null && s.lng != null)
        .map((s: any) => ({
          type: 'Feature' as const,
          geometry: {
            type: 'Point' as const,
            coordinates: [s.lng, s.lat],
          },
          properties: {
            id: s.id,
            title: s.title,
            summary: s.summary || '',
            severity: s.severity,
            agent_name: s.agent_name,
            created_at: s.created_at,
            color: severityColors[s.severity] || '#6b7280',
            radius: severityRadius[s.severity] || 4,
          },
        })),
    };

    // Remove existing source/layer if re-rendering
    if (map.getSource('signals')) {
      map.removeLayer('signals-circle');
      map.removeSource('signals');
    }

    map.addSource('signals', {
      type: 'geojson',
      data: geojson,
    });

    map.addLayer({
      id: 'signals-circle',
      type: 'circle',
      source: 'signals',
      paint: {
        'circle-color': ['get', 'color'],
        'circle-radius': ['get', 'radius'],
        'circle-opacity': 0.85,
        'circle-stroke-width': 1.5,
        'circle-stroke-color': ['get', 'color'],
        'circle-stroke-opacity': 0.4,
      },
    });

    // Click handler for popups
    map.on('click', 'signals-circle', (e: any) => {
      if (!e.features || e.features.length === 0) return;
      const feature = e.features[0];
      const coords = feature.geometry.coordinates.slice();
      const props = feature.properties;
      const timeAgo = formatDistanceToNow(new Date(props.created_at), { addSuffix: true });

      import('mapbox-gl').then((mapboxgl) => {
        new mapboxgl.default.Popup({ offset: 15, maxWidth: '300px' })
          .setLngLat(coords)
          .setHTML(
            `<div style="font-family: Inter, system-ui, sans-serif;">
              <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">
                <span style="width:8px;height:8px;border-radius:50%;background:${props.color};display:inline-block;"></span>
                <span style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.05em;">${props.severity}</span>
                <span style="font-size:11px;color:#6b7280;margin-left:auto;">${timeAgo}</span>
              </div>
              <h3 style="font-size:14px;font-weight:600;color:#ffffff;margin:0 0 6px 0;">${props.title}</h3>
              <p style="font-size:12px;color:#9ca3af;margin:0 0 8px 0;line-height:1.4;">${props.summary}</p>
              <span style="font-size:10px;background:#1f2937;color:#d1d5db;padding:2px 8px;border-radius:4px;">${props.agent_name.replace(/_/g, ' ')}</span>
            </div>`
          )
          .addTo(map);
      });
    });

    // Change cursor on hover
    map.on('mouseenter', 'signals-circle', () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', 'signals-circle', () => {
      map.getCanvas().style.cursor = '';
    });
  }, [signals, mapLoaded]);

  if (noToken) {
    return (
      <div className="flex items-center justify-center min-h-[calc(100vh-8rem)]">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-8 max-w-md text-center">
          <svg className="w-12 h-12 text-gray-600 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z" />
          </svg>
          <h2 className="text-lg font-semibold text-white mb-2">Map View Unavailable</h2>
          <p className="text-sm text-gray-400">
            Set <code className="text-red-400 bg-gray-800 px-1.5 py-0.5 rounded text-xs">NEXT_PUBLIC_MAPBOX_TOKEN</code> in{' '}
            <code className="text-gray-300 bg-gray-800 px-1.5 py-0.5 rounded text-xs">.env.local</code>{' '}
            to enable the map view.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="-m-4 md:-m-6 lg:-m-8">
      {/* Legend */}
      <div className="absolute z-10 top-2 md:top-4 left-2 md:left-4 bg-gray-900/95 border border-gray-800 rounded-lg p-3 backdrop-blur-sm">
        <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-2 font-semibold">Severity</p>
        <div className="flex flex-col gap-1.5">
          {(['critical', 'alert', 'watch', 'info'] as const).map((sev) => (
            <div key={sev} className="flex items-center gap-2">
              <span
                className="rounded-full"
                style={{
                  width: severityRadius[sev] * 2,
                  height: severityRadius[sev] * 2,
                  backgroundColor: severityColors[sev],
                  opacity: 0.85,
                }}
              />
              <span className="text-[11px] text-gray-400 capitalize">{sev}</span>
            </div>
          ))}
        </div>
        <p className="text-[10px] text-gray-600 mt-2">{signals.length} signals on map</p>
      </div>

      <div ref={mapContainer} className="w-full h-screen" />
    </div>
  );
}
