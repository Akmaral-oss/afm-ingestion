-- Ensure the database user has sufficient privileges
-- This script runs automatically on the first container startup

-- Grant superuser status to afmuser to simplify development/migration tasks
ALTER USER afmuser WITH SUPERUSER;

-- Ensure afmuser owns the database
ALTER DATABASE afmdb OWNER TO afmuser;

-- Grant all privileges on the database to afmuser
GRANT ALL PRIVILEGES ON DATABASE afmdb TO afmuser;

-- Also explicitly grant on public schema to avoid issues
GRANT ALL ON SCHEMA public TO afmuser;

-- Finally, create the afm schema and set the owner
CREATE SCHEMA IF NOT EXISTS afm;
ALTER SCHEMA afm OWNER TO afmuser;
GRANT ALL PRIVILEGES ON SCHEMA afm TO afmuser;
