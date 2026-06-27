# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Postgres 16 + pg_cron (pg_stat_statements ships in contrib already).
# ---------------------------------------------------------------------------
FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-cron \
    && rm -rf /var/lib/apt/lists/*
