#!/bin/bash
echo "host replication $POSTGRES_DB 127.0.0.1/32 md5" >> "$PGDATA/pg_hba.conf"
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $PG_REP_USER WITH REPLICATION PASSWORD '$PG_REP_PASSWORD';
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO $PG_REP_USER;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $PG_REP_USER;
EOSQL
cat >> ${PGDATA}/postgresql.conf <<EOF
wal_level=logical
max_wal_senders=5
max_replication_slots=5
EOF
