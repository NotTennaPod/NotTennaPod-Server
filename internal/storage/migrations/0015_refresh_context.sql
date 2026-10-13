-- The fetch context (generation, refresh URL, credentials) a podcast's
-- schedule belongs to, so a restart notices a replaced generation or a changed
-- URL before applying the old backoff.
ALTER TABLE refresh_schedule ADD COLUMN context TEXT NOT NULL DEFAULT '';
