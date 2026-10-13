-- Contributions stored before session continuations name no continuation. Each session then
-- had exactly one, its start's author's (another author's facts were refused), so
-- every session effect gets that author, a finalization also its target, and every
-- statistics-reset cut that author: what Apply writes for the same facts. Keys
-- stay in canonical order. Effects that already name their author are untouched.

UPDATE contributions SET effect = (
	SELECT json_group_object(j.key, CASE j.type
			WHEN 'true' THEN json('true') WHEN 'false' THEN json('false')
			WHEN 'object' THEN json(j.value) WHEN 'array' THEN json(j.value)
			ELSE j.value END ORDER BY j.key)
	FROM json_each(CASE json_extract(contributions.effect, '$.kind')
		WHEN 'session_finalize' THEN json_set(contributions.effect, '$.author', s.author_id, '$.target', s.author_id)
		ELSE json_set(contributions.effect, '$.author', s.author_id) END) j)
FROM listening_sessions s
WHERE s.account_id = contributions.account_id
	AND s.session_id = json_extract(contributions.effect, '$.session')
	AND json_extract(contributions.effect, '$.kind') IN ('session_start', 'session_checkpoint', 'session_end', 'session_finalize')
	AND json_type(contributions.effect, '$.author') IS NULL;

UPDATE contributions SET effect = json_set(effect, '$.cuts', (
	SELECT json_group_array(CASE WHEN s.author_id IS NULL OR json_type(c.value, '$.author') IS NOT NULL THEN json(c.value)
			ELSE json_object('author', s.author_id, 'ordinal', json_extract(c.value, '$.ordinal'), 'session', json_extract(c.value, '$.session')) END
		ORDER BY c.key)
	FROM json_each(contributions.effect, '$.cuts') c
	LEFT JOIN listening_sessions s ON s.account_id = contributions.account_id AND s.session_id = json_extract(c.value, '$.session')))
WHERE json_extract(effect, '$.kind') = 'statistics_reset'
	AND EXISTS (SELECT 1 FROM json_each(effect, '$.cuts') c WHERE json_type(c.value, '$.author') IS NULL);
