-- Removes everything messenger.sql created.
--
--   psql "host=<router> port=6432 dbname=<db> user=<user>" -f drop.sql
--
-- The admin console is expected on port 7432 of the same host; pass
-- -v admin_port=<port> to change it.

\set ON_ERROR_STOP on
\if :{?admin_port}
\else
\set admin_port 7432
\endif
\set router_port :PORT

DROP TABLE IF EXISTS reactions;
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS chat_members;
DROP TABLE IF EXISTS chats;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS emoji;
DROP TABLE IF EXISTS languages;

\connect - - - :admin_port

DROP DISTRIBUTION IF EXISTS ds_chats CASCADE;
DROP REFERENCE TABLE IF EXISTS users;
DROP REFERENCE TABLE IF EXISTS emoji;
DROP REFERENCE TABLE IF EXISTS languages;

\connect - - - :router_port
