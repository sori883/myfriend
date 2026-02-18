FROM pgvector/pgvector:pg16

# Apache AGE build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-16 \
    git \
    ca-certificates \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
  && cd /tmp \
  && git clone --depth 1 --branch PG16/v1.6.0-rc0 https://github.com/apache/age.git \
  && cd age \
  && make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config \
  && make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config install \
  && cd / && rm -rf /tmp/age \
  && apt-get purge -y build-essential git flex bison \
  && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# pg_bigm (Japanese bi-gram full-text search) — ソースからビルド
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-16 \
    git \
    ca-certificates \
  && cd /tmp \
  && git clone --depth 1 --branch v1.2-20250903 https://github.com/pgbigm/pg_bigm.git \
  && cd pg_bigm \
  && make USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config \
  && make USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config install \
  && cd / && rm -rf /tmp/pg_bigm \
  && apt-get purge -y build-essential git \
  && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY init/ /docker-entrypoint-initdb.d/

EXPOSE 5432
