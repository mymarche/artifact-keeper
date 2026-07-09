-- Backfill repository_id for existing download_statistics rows
-- where artifact_id is known. Proxy-only downloads (no artifact_id)
-- remain NULL — there's no source of truth for their repository.
--
-- Run AFTER migration 149 has been applied:
--   psql $DATABASE_URL -f 149_backfill_repository_id.sql
--
-- On large tables, consider batching:
--   UPDATE download_statistics ds SET repository_id = a.repository_id
--   FROM artifacts a
--   WHERE ds.artifact_id = a.id
--     AND ds.repository_id IS NULL
--     AND ds.id IN (SELECT id FROM download_statistics WHERE repository_id IS NULL LIMIT 10000);
-- Repeat until 0 rows updated.

UPDATE download_statistics ds
SET repository_id = a.repository_id
FROM artifacts a
WHERE ds.artifact_id = a.id
  AND ds.repository_id IS NULL;
