package openpanel

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ddlStatements contains the CREATE IF NOT EXISTS DDL for all OpenPanel
// ClickHouse tables and materialized views. The placeholder {{database}} is
// replaced at runtime with the configured database name.
var ddlStatements = []string{
	// 1. events
	`CREATE TABLE IF NOT EXISTS {{database}}.events
(
    id UUID DEFAULT generateUUIDv4(),
    name LowCardinality(String),
    sdk_name LowCardinality(String),
    sdk_version LowCardinality(String),
    device_id String CODEC(ZSTD(3)),
    profile_id String CODEC(ZSTD(3)),
    project_id String CODEC(ZSTD(3)),
    session_id String CODEC(LZ4),
    path String CODEC(ZSTD(3)),
    origin String CODEC(ZSTD(3)),
    referrer String CODEC(ZSTD(3)),
    referrer_name String CODEC(ZSTD(3)),
    referrer_type LowCardinality(String),
    revenue UInt64,
    duration UInt64 CODEC(Delta(4), LZ4),
    properties Map(String, String) CODEC(ZSTD(3)),
    created_at DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    country LowCardinality(FixedString(2)),
    city String,
    region LowCardinality(String),
    longitude Nullable(Float32) CODEC(Gorilla(4), LZ4),
    latitude Nullable(Float32) CODEC(Gorilla(4), LZ4),
    os LowCardinality(String),
    os_version LowCardinality(String),
    browser LowCardinality(String),
    browser_version LowCardinality(String),
    device LowCardinality(String),
    brand LowCardinality(String),
    model LowCardinality(String),
    imported_at Nullable(DateTime) CODEC(Delta(4), LZ4),
    INDEX idx_name name TYPE bloom_filter GRANULARITY 1,
    INDEX idx_properties_bounce properties['__bounce'] TYPE set(3) GRANULARITY 1,
    INDEX idx_origin origin TYPE bloom_filter(0.05) GRANULARITY 1,
    INDEX idx_path path TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (project_id, toDate(created_at), created_at, name)
SETTINGS index_granularity = 8192`,

	// 2. sessions
	`CREATE TABLE IF NOT EXISTS {{database}}.sessions
(
    id String,
    project_id String CODEC(ZSTD(3)),
    profile_id String CODEC(ZSTD(3)),
    device_id String CODEC(ZSTD(3)),
    created_at DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    ended_at DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    is_bounce Bool,
    entry_origin LowCardinality(String),
    entry_path String CODEC(ZSTD(3)),
    exit_origin LowCardinality(String),
    exit_path String CODEC(ZSTD(3)),
    screen_view_count Int32,
    revenue Float64,
    event_count Int32,
    duration UInt32,
    country LowCardinality(FixedString(2)),
    region LowCardinality(String),
    city String,
    longitude Nullable(Float32) CODEC(Gorilla(4), LZ4),
    latitude Nullable(Float32) CODEC(Gorilla(4), LZ4),
    device LowCardinality(String),
    brand LowCardinality(String),
    model LowCardinality(String),
    browser LowCardinality(String),
    browser_version LowCardinality(String),
    os LowCardinality(String),
    os_version LowCardinality(String),
    utm_medium String CODEC(ZSTD(3)),
    utm_source String CODEC(ZSTD(3)),
    utm_campaign String CODEC(ZSTD(3)),
    utm_content String CODEC(ZSTD(3)),
    utm_term String CODEC(ZSTD(3)),
    referrer String CODEC(ZSTD(3)),
    referrer_name String CODEC(ZSTD(3)),
    referrer_type LowCardinality(String),
    sign Int8,
    version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (project_id, toDate(created_at), created_at)
SETTINGS index_granularity = 8192`,

	// 3. profiles
	`CREATE TABLE IF NOT EXISTS {{database}}.profiles
(
    id String CODEC(ZSTD(3)),
    is_external Bool,
    first_name String CODEC(ZSTD(3)),
    last_name String CODEC(ZSTD(3)),
    email String CODEC(ZSTD(3)),
    avatar String CODEC(ZSTD(3)),
    properties Map(String, String) CODEC(ZSTD(3)),
    project_id String CODEC(ZSTD(3)),
    created_at DateTime64(3) CODEC(Delta(4), LZ4),
    INDEX idx_first_name first_name TYPE bloom_filter GRANULARITY 1,
    INDEX idx_last_name last_name TYPE bloom_filter GRANULARITY 1,
    INDEX idx_email email TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (project_id, id)
SETTINGS index_granularity = 8192`,

	// 4. profile_aliases
	`CREATE TABLE IF NOT EXISTS {{database}}.profile_aliases
(
    project_id String,
    profile_id String,
    alias String,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY (project_id, profile_id, alias, created_at)
SETTINGS index_granularity = 8192`,

	// 5. session_replay_chunks (empty — required for dashboard LEFT JOIN)
	`CREATE TABLE IF NOT EXISTS {{database}}.session_replay_chunks
(
    project_id String CODEC(ZSTD(3)),
    session_id String CODEC(ZSTD(3)),
    chunk_index UInt16,
    started_at DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    ended_at DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    events_count UInt16,
    is_full_snapshot Bool,
    payload String CODEC(ZSTD(6))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(started_at)
ORDER BY (project_id, session_id, started_at, chunk_index)
SETTINGS index_granularity = 8192`,

	// 6. MV: dau_mv
	`CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.dau_mv
(
    date Date,
    profile_id AggregateFunction(uniq, String),
    project_id String
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(date)
ORDER BY (project_id, date)
SETTINGS index_granularity = 8192
AS SELECT
    toDate(created_at) AS date,
    uniqState(profile_id) AS profile_id,
    project_id
FROM {{database}}.events
GROUP BY date, project_id`,

	// 7. MV: cohort_events_mv
	`CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.cohort_events_mv
(
    project_id String,
    name LowCardinality(String),
    created_at Date,
    profile_id String,
    event_count UInt64
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, name, created_at, profile_id)
SETTINGS index_granularity = 8192
AS SELECT
    project_id,
    name,
    toDate(created_at) AS created_at,
    profile_id,
    count() AS event_count
FROM {{database}}.events
WHERE profile_id != device_id
GROUP BY project_id, name, created_at, profile_id`,

	// 8. MV: distinct_event_names_mv
	`CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.distinct_event_names_mv
(
    project_id String,
    name LowCardinality(String),
    created_at DateTime64(3),
    event_count UInt64
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, name, created_at)
SETTINGS index_granularity = 8192
AS SELECT
    project_id,
    name,
    max(created_at) AS created_at,
    count() AS event_count
FROM {{database}}.events
GROUP BY project_id, name`,

	// 9. MV: event_property_values_mv
	`CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.event_property_values_mv
(
    project_id String,
    name LowCardinality(String),
    property_key String,
    property_value String,
    created_at DateTime64(3)
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, name, property_key, property_value)
SETTINGS index_granularity = 8192
AS SELECT
    project_id,
    name,
    key_value.keys AS property_key,
    key_value.values AS property_value,
    created_at
FROM
(
    SELECT
        project_id,
        name,
        untuple(arrayJoin(properties)) AS key_value,
        max(created_at) AS created_at
    FROM {{database}}.events
    GROUP BY project_id, name, key_value
)
WHERE (property_value != '') AND (property_key != '') AND (property_key NOT IN ('__duration_from', '__properties_from'))
GROUP BY project_id, name, property_key, property_value, created_at`,
}

// EnsureSchema creates the OpenPanel database, tables, and materialized views
// if they do not already exist. Safe to call on every startup.
// When replicated is true, MergeTree engines are converted to their Replicated
// variants for data replication. The Replicated database engine only replicates
// DDL — explicit ReplicatedMergeTree is required for data replication.
// Inside a Replicated database, ZooKeeper paths are auto-assigned.
func EnsureSchema(ctx context.Context, conn driver.Conn, database string, replicated bool) error {
	for i, tmpl := range ddlStatements {
		stmt := strings.ReplaceAll(tmpl, "{{database}}", database)
		if replicated {
			stmt = toReplicatedEngines(stmt)
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema statement %d failed: %w", i, err)
		}
	}
	return nil
}

// toReplicatedEngines converts MergeTree-family engines to their Replicated variants.
func toReplicatedEngines(stmt string) string {
	// Order matters: replace specific variants before base MergeTree
	replacements := []struct{ from, to string }{
		{"ENGINE = VersionedCollapsingMergeTree", "ENGINE = ReplicatedVersionedCollapsingMergeTree"},
		{"ENGINE = ReplacingMergeTree", "ENGINE = ReplicatedReplacingMergeTree"},
		{"ENGINE = AggregatingMergeTree", "ENGINE = ReplicatedAggregatingMergeTree"},
		{"ENGINE = MergeTree", "ENGINE = ReplicatedMergeTree"},
	}
	for _, r := range replacements {
		stmt = strings.Replace(stmt, r.from, r.to, 1)
	}
	return stmt
}

