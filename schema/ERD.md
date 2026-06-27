# ERD — PgPulse

```mermaid
erDiagram
    TENANTS ||--o{ EVENTS : "owns (RLS-scoped)"
    TENANTS {
        uuid        id PK "gen_random_uuid()"
        text        name
        text        api_key UK "encode(gen_random_bytes(32),'hex')"
        text        plan "free | pro | enterprise"
        timestamptz created_at
    }
    EVENTS {
        bigserial   id PK "(id, ingested_at) composite PK"
        uuid        tenant_id FK
        text        endpoint
        text        method "GET|POST|PUT|PATCH|DELETE"
        smallint    status_code
        integer     latency_ms
        text        user_agent
        inet        ip_address
        jsonb       metadata
        timestamptz ingested_at "RANGE partition key"
    }
    EVENTS ||..|| HOURLY_STATS : "rolled up hourly (MV)"
    HOURLY_STATS {
        uuid        tenant_id "PK part"
        timestamptz hour "PK part, date_trunc('hour')"
        text        endpoint "PK part"
        bigint      total_requests
        bigint      error_count
        numeric     avg_latency_ms
        double      p50_latency_ms
        double      p95_latency_ms
        double      p99_latency_ms
    }
```

## Partitioning layout

`events` is the partitioned parent; physical rows live in monthly children:

```
events (PARTITION BY RANGE (ingested_at))
├── events_2026_05   FROM ('2026-05-01') TO ('2026-06-01')
├── events_2026_06   FROM ('2026-06-01') TO ('2026-07-01')   <- current (hot)
├── events_2026_07   FROM ('2026-07-01') TO ('2026-08-01')
├── events_2026_08   FROM ('2026-08-01') TO ('2026-09-01')
└── events_default   (catch-all; should normally stay empty)
```

Indexes, RLS policies, and the FK are declared on the parent and inherited by
every current and future child partition.
