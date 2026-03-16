package openpanel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// stripKeys are internal properties that should be excluded from event properties.
var stripKeys = map[string]bool{
	"mixpanel_token":            true,
	"importance":                true,
	"distinct_id":               true,
	"log_once_a_day":            true,
	"is_first_event_of_the_day": true,
	"cycle_number":              true,
}

// systemKeys are top-level Segment/Jitsu keys that aren't custom properties.
var systemKeys = map[string]bool{
	"context": true, "event": true, "type": true, "timestamp": true,
	"message_id": true, "messageId": true,
	"user_id": true, "userId": true,
	"anonymous_id": true, "anonymousId": true,
	"integrations": true, "_metadata": true,
	"received_at": true, "receivedAt": true,
	"request_ip": true, "requestIp": true,
	"sent_at": true, "sentAt": true,
	"original_timestamp": true, "originalTimestamp": true,
	"properties": true, "has_name": true,
}

func getNested(data map[string]any, path string) string {
	keys := strings.Split(path, ".")
	var val any = data
	for _, k := range keys {
		m, ok := val.(map[string]any)
		if !ok {
			return ""
		}
		val = m[k]
	}
	if val == nil {
		return ""
	}
	return fmt.Sprint(val)
}

func tryParseTimestamp(raw any) (time.Time, bool) {
	if raw == nil || raw == "" {
		return time.Time{}, false
	}
	switch v := raw.(type) {
	case time.Time:
		return v, true
	case string:
		s := strings.Replace(v, "Z", "+00:00", 1)
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05.999999999", s)
			if err != nil {
				return time.Time{}, false
			}
		}
		return t, true
	default:
		return time.Time{}, false
	}
}

const maxPastSkew = 7 * 24 * time.Hour

func adjustTimestamp(msg map[string]any) (time.Time, *time.Time) {
	ts, tsOk := tryParseTimestamp(msg["timestamp"])

	rawSentAt := msg["sentAt"]
	if rawSentAt == nil {
		rawSentAt = msg["sent_at"]
	}
	rawReceivedAt := msg["receivedAt"]
	if rawReceivedAt == nil {
		rawReceivedAt = msg["received_at"]
	}

	sentAt, sentOk := tryParseTimestamp(rawSentAt)
	receivedAt, recvOk := tryParseTimestamp(rawReceivedAt)

	if !tsOk {
		if recvOk {
			return receivedAt, nil
		}
		return time.Now().UTC(), nil
	}

	result := ts
	if sentOk && recvOk {
		offset := receivedAt.Sub(sentAt)
		if offset > -maxPastSkew && offset < maxPastSkew {
			result = ts.Add(offset)
		}
	}

	if recvOk {
		if result.After(receivedAt) || receivedAt.Sub(result) > maxPastSkew {
			return receivedAt, &ts
		}
	}

	return result, nil
}

// MapEvent transforms a Segment track/screen message into an OpenPanel event dict.
// Returns the event map and the raw IP for geo enrichment.
func MapEvent(msg map[string]any, projectID string) (map[string]any, string) {
	ctx := getMap(msg, "context")
	props := getMap(msg, "properties")
	eventType := getString(msg, "type", "track")

	// Event name
	var name string
	if eventType == "screen" {
		name = "screen_view"
	} else {
		name = getString(msg, "event", "unknown")
	}

	// Profile ID
	userID := firstNonEmpty(msg, "userId", "user_id")
	anonymousID := firstNonEmpty(msg, "anonymousId", "anonymous_id")
	profileID := userID
	if profileID == "" {
		profileID = anonymousID
	}

	// Merge properties: start with nested props, add top-level custom fields
	mergedProps := make(map[string]any)
	for k, v := range props {
		mergedProps[k] = v
	}
	for k, v := range msg {
		if !systemKeys[k] && mergedProps[k] == nil && !stripKeys[k] {
			mergedProps[k] = v
		}
	}

	// Clean properties: strip internal keys, cast to string
	cleanProps := make(map[string]string)
	for k, v := range mergedProps {
		if !stripKeys[k] {
			cleanProps[k] = fmt.Sprint(v)
		}
	}

	// Path for screen events
	var path string
	if eventType == "screen" {
		path = getString(mergedProps, "name", "")
		if path == "" {
			path = getString(props, "name", "")
		}
	}

	// Device type normalization
	deviceType := getNested(ctx, "device.type")
	if strings.EqualFold(deviceType, "android") {
		deviceType = "mobile"
	}

	// Revenue
	var revenue uint64
	if rawRev := mergedProps["revenue"]; rawRev != nil {
		if f, err := toFloat64(rawRev); err == nil && f > 0 {
			revenue = uint64(math.Max(0, f))
		}
	}

	ip := firstNonEmpty(msg, "requestIp", "request_ip")
	if ip == "" {
		ip = getNested(ctx, "ip")
	}

	adjusted, originalTs := adjustTimestamp(msg)

	event := map[string]any{
		"id":             uuid.New().String(),
		"name":           name,
		"sdk_name":       getNested(ctx, "library.name"),
		"sdk_version":    getNested(ctx, "library.version"),
		"device_id":      anonymousID,
		"profile_id":     profileID,
		"project_id":     projectID,
		"session_id":     "",
		"path":           path,
		"origin":         "",
		"referrer":       "",
		"referrer_name":  "",
		"referrer_type":  "",
		"revenue":        revenue,
		"duration":       uint64(0),
		"properties":     cleanProps,
		"created_at":     adjusted,
		"incorrect_event_timestamp": originalTs,
		"country":        "\x00\x00",
		"city":           "",
		"region":         "",
		"longitude":      nil,
		"latitude":       nil,
		"os":             getNested(ctx, "os.name"),
		"os_version":     getNested(ctx, "os.version"),
		"browser":        "",
		"browser_version": "",
		"device":         deviceType,
		"brand":          getNested(ctx, "device.manufacturer"),
		"model":          getNested(ctx, "device.model"),
		"app_name":       strings.TrimSpace(getNested(ctx, "app.name")),
		"app_version":    strings.TrimSpace(getNested(ctx, "app.version")),
		"app_namespace":  strings.TrimSpace(getNested(ctx, "app.namespace")),
		"app_build":      strings.TrimSpace(getNested(ctx, "app.build")),
		"imported_at":    nil,
	}

	return event, ip
}

func getMap(m map[string]any, key string) map[string]any {
	if v, ok := m[key]; ok {
		if mm, ok := v.(map[string]any); ok {
			return mm
		}
	}
	return map[string]any{}
}

// getTraits returns merged traits from top-level "traits" and "context.traits".
// The Segment Kotlin SDK puts identify traits under context.traits,
// while some other SDKs use top-level traits. Top-level wins on conflict.
func getTraits(msg map[string]any) map[string]any {
	ctxTraits := map[string]any{}
	if ctx, ok := msg["context"]; ok {
		if ctxMap, ok := ctx.(map[string]any); ok {
			ctxTraits = getMap(ctxMap, "traits")
		}
	}
	topTraits := getMap(msg, "traits")

	if len(topTraits) == 0 && len(ctxTraits) == 0 {
		return map[string]any{}
	}
	merged := make(map[string]any, len(ctxTraits)+len(topTraits))
	for k, v := range ctxTraits {
		merged[k] = v
	}
	for k, v := range topTraits {
		merged[k] = v
	}
	return merged
}

func getString(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return def
}

func firstNonEmpty(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v := getString(m, k, ""); v != "" {
			return v
		}
	}
	return ""
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
