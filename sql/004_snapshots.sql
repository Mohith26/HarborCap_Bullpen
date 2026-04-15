-- ============================================================
-- The Bullpen: PDF Snapshot Support
-- ============================================================
-- Adds a snapshot_url column to signals and creates a Supabase
-- Storage bucket named 'snapshots' for storing PDF captures of
-- the webpages agents scraped. Public-read, anon-write (narrow
-- RLS policy so only the 'snapshots' bucket is writable).
-- ============================================================

-- 1. Signals.snapshot_url column
ALTER TABLE signals ADD COLUMN IF NOT EXISTS snapshot_url TEXT;

-- 2. Storage bucket
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
    'snapshots',
    'snapshots',
    true,
    52428800,                               -- 50 MB
    ARRAY['application/pdf']
)
ON CONFLICT (id) DO UPDATE SET
    public = EXCLUDED.public,
    file_size_limit = EXCLUDED.file_size_limit,
    allowed_mime_types = EXCLUDED.allowed_mime_types;

-- 3. RLS policies on storage.objects for the snapshots bucket
DROP POLICY IF EXISTS "Public read snapshots" ON storage.objects;
CREATE POLICY "Public read snapshots"
ON storage.objects
FOR SELECT
USING (bucket_id = 'snapshots');

DROP POLICY IF EXISTS "Anon insert snapshots" ON storage.objects;
CREATE POLICY "Anon insert snapshots"
ON storage.objects
FOR INSERT
WITH CHECK (bucket_id = 'snapshots');

DROP POLICY IF EXISTS "Anon update snapshots" ON storage.objects;
CREATE POLICY "Anon update snapshots"
ON storage.objects
FOR UPDATE
USING (bucket_id = 'snapshots')
WITH CHECK (bucket_id = 'snapshots');

DROP POLICY IF EXISTS "Anon delete snapshots" ON storage.objects;
CREATE POLICY "Anon delete snapshots"
ON storage.objects
FOR DELETE
USING (bucket_id = 'snapshots');
