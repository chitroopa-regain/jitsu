package openpanel

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// WriteEvents batch-inserts event rows into the events table.
func WriteEvents(ctx context.Context, conn driver.Conn, database string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.events", database))
	if err != nil {
		return fmt.Errorf("prepare events batch: %w", err)
	}

	for _, r := range rows {
		err = batch.Append(
			r["id"],
			r["name"],
			r["sdk_name"],
			r["sdk_version"],
			r["device_id"],
			r["profile_id"],
			r["project_id"],
			r["session_id"],
			r["path"],
			r["origin"],
			r["referrer"],
			r["referrer_name"],
			r["referrer_type"],
			r["revenue"],
			r["duration"],
			r["properties"],
			toTime(r["created_at"]),
			toNullTime(r["incorrect_event_timestamp"]),
			fixCountry(r["country"]),
			r["city"],
			r["region"],
			toNullFloat32(r["longitude"]),
			toNullFloat32(r["latitude"]),
			r["os"],
			r["os_version"],
			r["browser"],
			r["browser_version"],
			r["device"],
			r["brand"],
			r["model"],
			r["app_name"],
			r["app_version"],
			r["app_namespace"],
			r["app_build"],
			toNullTime(r["imported_at"]),
		)
		if err != nil {
			return fmt.Errorf("append event row: %w", err)
		}
	}

	return batch.Send()
}

// WriteSessions batch-inserts session rows into the sessions table.
func WriteSessions(ctx context.Context, conn driver.Conn, database string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.sessions", database))
	if err != nil {
		return fmt.Errorf("prepare sessions batch: %w", err)
	}

	for _, r := range rows {
		err = batch.Append(
			r["id"],
			r["project_id"],
			r["profile_id"],
			r["device_id"],
			toTime(r["created_at"]),
			toTime(r["ended_at"]),
			r["is_bounce"],
			r["entry_origin"],
			r["entry_path"],
			r["exit_origin"],
			r["exit_path"],
			r["screen_view_count"],
			r["revenue"],
			r["event_count"],
			r["duration"],
			fixCountry(r["country"]),
			r["region"],
			r["city"],
			toNullFloat32(r["longitude"]),
			toNullFloat32(r["latitude"]),
			r["device"],
			r["brand"],
			r["model"],
			r["browser"],
			r["browser_version"],
			r["os"],
			r["os_version"],
			r["utm_medium"],
			r["utm_source"],
			r["utm_campaign"],
			r["utm_content"],
			r["utm_term"],
			r["referrer"],
			r["referrer_name"],
			r["referrer_type"],
			r["sign"],
			r["version"],
		)
		if err != nil {
			return fmt.Errorf("append session row: %w", err)
		}
	}

	return batch.Send()
}

// WriteProfiles batch-inserts profile rows into the profiles table.
func WriteProfiles(ctx context.Context, conn driver.Conn, database string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.profiles", database))
	if err != nil {
		return fmt.Errorf("prepare profiles batch: %w", err)
	}

	for _, r := range rows {
		err = batch.Append(
			r["id"],
			r["is_external"],
			r["first_name"],
			r["last_name"],
			r["email"],
			r["avatar"],
			r["properties"],
			r["project_id"],
			toTime(r["created_at"]),
		)
		if err != nil {
			return fmt.Errorf("append profile row: %w", err)
		}
	}

	return batch.Send()
}

// WriteTraits batch-inserts trait rows into the profile_traits table.
func WriteTraits(ctx context.Context, conn driver.Conn, database string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.profile_traits", database))
	if err != nil {
		return fmt.Errorf("prepare traits batch: %w", err)
	}

	for _, r := range rows {
		err = batch.Append(
			r["project_id"],
			r["profile_id"],
			r["key"],
			r["value"],
			toTime(r["updated_at"]),
		)
		if err != nil {
			return fmt.Errorf("append trait row: %w", err)
		}
	}

	return batch.Send()
}

// WriteAliases batch-inserts alias rows into the profile_aliases table.
func WriteAliases(ctx context.Context, conn driver.Conn, database string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.profile_aliases", database))
	if err != nil {
		return fmt.Errorf("prepare aliases batch: %w", err)
	}

	for _, r := range rows {
		err = batch.Append(
			r["project_id"],
			r["profile_id"],
			r["alias"],
			toTime(r["created_at"]),
		)
		if err != nil {
			return fmt.Errorf("append alias row: %w", err)
		}
	}

	return batch.Send()
}

// Helpers for type conversion

func toTime(v any) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case string:
		if t == "" {
			return time.Now().UTC()
		}
		parsed, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return time.Now().UTC()
		}
		return parsed.UTC()
	default:
		return time.Now().UTC()
	}
}

func toNullFloat32(v any) *float32 {
	switch f := v.(type) {
	case float32:
		return &f
	case float64:
		f32 := float32(f)
		return &f32
	case nil:
		return nil
	default:
		return nil
	}
}

func toNullTime(v any) *time.Time {
	switch t := v.(type) {
	case time.Time:
		utc := t.UTC()
		return &utc
	case *time.Time:
		if t == nil {
			return nil
		}
		utc := t.UTC()
		return &utc
	case nil:
		return nil
	default:
		return nil
	}
}

func fixCountry(v any) string {
	s, ok := v.(string)
	if !ok || len(s) < 2 {
		return "\x00\x00"
	}
	return s[:2]
}
