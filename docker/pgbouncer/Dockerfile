# syntax=docker/dockerfile:1
# PgBouncer image with psql client for building userlist from the live role.
FROM edoburu/pgbouncer:1.21.0-p2

USER root
# alpine-based image: add postgres client tools.
RUN apk add --no-cache postgresql-client

COPY docker/pgbouncer/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
COPY docker/pgbouncer/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && touch /etc/pgbouncer/userlist.txt \
    && chown -R postgres:postgres /etc/pgbouncer

USER postgres
ENTRYPOINT ["/entrypoint.sh"]
