SHOW migrations;

-- Both SET forms, quoted names, and empty values.
ALTER SYSTEM MIGRATION SET init = abc123;
ALTER SYSTEM MIGRATION SET 'MixedCase' TO 'sha256:abc';
ALTER SYSTEM MIGRATION SET 'init/next.sql' = '';
ALTER SYSTEM MIGRATION SET '../init' TO 'sha256:def';
SHOW migrations;

-- SET replaces an existing value without adding another entry.
ALTER SYSTEM MIGRATION SET init TO 'sha256:updated';
ALTER SYSTEM MIGRATION SET init = 'sha256:updated';
SHOW migrations WHERE name = 'init';
SHOW migrations (name) WHERE value = 'sha256:abc';
SHOW migrations ORDER BY name DESC;

-- A new console connection sees the same journal.
\connect
SHOW migrations;

-- RESET is exact and idempotent; init/next.sql must survive resetting init.
ALTER SYSTEM MIGRATION RESET init;
ALTER SYSTEM MIGRATION RESET init;
ALTER SYSTEM MIGRATION RESET missing;
SHOW migrations;

ALTER SYSTEM MIGRATION RESET 'MixedCase';
ALTER SYSTEM MIGRATION RESET 'init/next.sql';
ALTER SYSTEM MIGRATION RESET '../init';
SHOW migrations;
