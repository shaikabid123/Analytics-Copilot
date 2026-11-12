#!/bin/bash
# Runs ONCE, on first creation of the Postgres data volume (docker-entrypoint-initdb.d).
# Creates the three least-privilege roles and the layered schemas.
#   app_rw       backend application (owns schema "app"; migrations run as this role)
#   etl_rw       Spark ETL (owns bronze / silver / gold)
#   source_ro    connector credential: SELECT on src only (demo Postgres source)
#   analytics_ro generated-SQL executor: SELECT on gold only, nothing else
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_pw="$APP_RW_PASSWORD" -v etl_pw="$ETL_RW_PASSWORD" -v ro_pw="$ANALYTICS_RO_PASSWORD" -v src_pw="$SOURCE_RO_PASSWORD" \
  -v dbname="$POSTGRES_DB" <<'SQL'

CREATE ROLE app_rw LOGIN PASSWORD :'app_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE etl_rw LOGIN PASSWORD :'etl_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE source_ro LOGIN PASSWORD :'src_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE analytics_ro LOGIN PASSWORD :'ro_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;

-- Nobody gets to create objects in public.
REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA app    AUTHORIZATION app_rw;
CREATE SCHEMA src    AUTHORIZATION etl_rw;   -- operational source tables (seeded in Phase 2)
CREATE SCHEMA bronze AUTHORIZATION etl_rw;
CREATE SCHEMA silver AUTHORIZATION etl_rw;
CREATE SCHEMA gold   AUTHORIZATION etl_rw;

-- Read-only analytics role: sees gold only (not src/bronze/silver). Tables the ETL role creates later
-- in gold are automatically SELECT-able; app/bronze/silver are not.
GRANT USAGE ON SCHEMA gold TO analytics_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_rw IN SCHEMA gold GRANT SELECT ON TABLES TO analytics_ro;

-- source_ro: the connector's credential for the demo Postgres source. SELECT on src only.
GRANT USAGE ON SCHEMA src TO source_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_rw IN SCHEMA src GRANT SELECT ON TABLES TO source_ro;
ALTER ROLE source_ro SET default_transaction_read_only = on;
ALTER ROLE source_ro SET statement_timeout = '30s';

-- Defense in depth (the SQL guard in Phase 5 is the primary control).
ALTER ROLE analytics_ro SET default_transaction_read_only = on;
ALTER ROLE analytics_ro SET statement_timeout = '10s';
SQL
