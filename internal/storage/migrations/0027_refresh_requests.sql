-- Refresh requests made through the refresh API (PROTOCOL §9.6), kept with
-- the schedule so a restart loses none and a client can tell when its own
-- request was answered. request_seq counts the requests for the podcast;
-- request_force_seq is the sequence of the newest single-podcast (forced)
-- request; done_seq is the newest request an ended attempt answered. A
-- request is pending while request_seq > done_seq, and forced while
-- request_force_seq > done_seq. All three only grow.
ALTER TABLE refresh_schedule ADD COLUMN request_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE refresh_schedule ADD COLUMN request_force_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE refresh_schedule ADD COLUMN done_seq INTEGER NOT NULL DEFAULT 0;
