-- Runs once, the first time the postgres container starts with an empty volume.
-- Each project gets its own database; tables are created by the apps themselves.
CREATE DATABASE jobqueue;
CREATE DATABASE ledger;
CREATE DATABASE collab;
