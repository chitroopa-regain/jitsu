package openpanel

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/redis/go-redis/v9"
)

const sessionTTL = 4 * time.Hour

type SessionManager struct {
	rdb       *redis.Client
	projectID string
}

type sessionState struct {
	ID              string  `json:"id"`
	CreatedAt       string  `json:"created_at"`
	EndedAt         string  `json:"ended_at"`
	ProfileID       string  `json:"profile_id"`
	DeviceID        string  `json:"device_id"`
	EventCount      int     `json:"event_count"`
	ScreenViewCount int     `json:"screen_view_count"`
	Revenue         float64 `json:"revenue"`
	Version         uint64  `json:"version"`
	Country         string  `json:"country"`
	City            string  `json:"city"`
	Region          string  `json:"region"`
	Longitude       any     `json:"longitude"`
	Latitude        any     `json:"latitude"`
	OS              string  `json:"os"`
	OSVersion       string  `json:"os_version"`
	Device          string  `json:"device"`
	Brand           string  `json:"brand"`
	Model           string  `json:"model"`
	EntryPath       string  `json:"entry_path"`
	ExitPath        string  `json:"exit_path"`
}

func NewSessionManager(rdb *redis.Client, projectID string) *SessionManager {
	return &SessionManager{rdb: rdb, projectID: projectID}
}

func (sm *SessionManager) sessionKey(deviceID string) string {
	return "session:" + deviceID
}

func (sm *SessionManager) getSession(deviceID string) *sessionState {
	raw, err := sm.rdb.Get(context.Background(), sm.sessionKey(deviceID)).Bytes()
	if err != nil {
		if err != redis.Nil {
			logging.Errorf("[session-mgr] redis GET %s failed: %v", sm.sessionKey(deviceID), err)
		}
		return nil
	}
	var s sessionState
	if err := json.Unmarshal(raw, &s); err != nil {
		logging.Errorf("[session-mgr] unmarshal session for device %s failed: %v", deviceID, err)
		return nil
	}
	return &s
}

func (sm *SessionManager) saveSession(deviceID string, s *sessionState) {
	data, err := json.Marshal(s)
	if err != nil {
		logging.Errorf("[session-mgr] marshal session for device %s failed: %v", deviceID, err)
		return
	}
	if err := sm.rdb.SetEx(context.Background(), sm.sessionKey(deviceID), data, sessionTTL).Err(); err != nil {
		logging.Errorf("[session-mgr] redis SETEX %s failed: %v", sm.sessionKey(deviceID), err)
	}
}

func (sm *SessionManager) deleteSession(deviceID string) {
	if err := sm.rdb.Del(context.Background(), sm.sessionKey(deviceID)).Err(); err != nil {
		logging.Errorf("[session-mgr] redis DEL %s failed: %v", sm.sessionKey(deviceID), err)
	}
}

func (sm *SessionManager) newSession(event map[string]any) *sessionState {
	ts := formatTime(event["created_at"])
	return &sessionState{
		ID:              uuid.New().String(),
		CreatedAt:       ts,
		EndedAt:         ts,
		ProfileID:       stringVal(event, "profile_id"),
		DeviceID:        stringVal(event, "device_id"),
		EventCount:      0,
		ScreenViewCount: 0,
		Revenue:         0,
		Version:         1,
		Country:         stringVal(event, "country"),
		City:            stringVal(event, "city"),
		Region:          stringVal(event, "region"),
		Longitude:       event["longitude"],
		Latitude:        event["latitude"],
		OS:              stringVal(event, "os"),
		OSVersion:       stringVal(event, "os_version"),
		Device:          stringVal(event, "device"),
		Brand:           stringVal(event, "brand"),
		Model:           stringVal(event, "model"),
		EntryPath:       stringVal(event, "path"),
		ExitPath:        stringVal(event, "path"),
	}
}

func (sm *SessionManager) sessionToRow(s *sessionState, sign int8) map[string]any {
	createdMs := parseTimestampMs(s.CreatedAt)
	endedMs := parseTimestampMs(s.EndedAt)
	duration := uint32(0)
	if endedMs > createdMs {
		duration = uint32(endedMs - createdMs)
	}

	return map[string]any{
		"id":                s.ID,
		"project_id":        sm.projectID,
		"profile_id":        s.ProfileID,
		"device_id":         s.DeviceID,
		"created_at":        s.CreatedAt,
		"ended_at":          s.EndedAt,
		"is_bounce":         s.ScreenViewCount <= 1,
		"entry_origin":      "",
		"entry_path":        s.EntryPath,
		"exit_origin":       "",
		"exit_path":         s.ExitPath,
		"screen_view_count": int32(s.ScreenViewCount),
		"revenue":           s.Revenue,
		"event_count":       int32(s.EventCount),
		"duration":          duration,
		"country":           orDefault(s.Country, "\x00\x00"),
		"region":            s.Region,
		"city":              s.City,
		"longitude":         s.Longitude,
		"latitude":          s.Latitude,
		"device":            s.Device,
		"brand":             s.Brand,
		"model":             s.Model,
		"browser":           "",
		"browser_version":   "",
		"os":                s.OS,
		"os_version":        s.OSVersion,
		"utm_medium":        "",
		"utm_source":        "",
		"utm_campaign":      "",
		"utm_content":       "",
		"utm_term":          "",
		"referrer":          "",
		"referrer_name":     "",
		"referrer_type":     "",
		"sign":              sign,
		"version":           s.Version,
	}
}

// ProcessEventsBatch processes all events through session management using Redis Pipeline.
// Performs 2 Redis round-trips total (Pipeline GET + Pipeline SET/DEL) instead of 2-3 per event.
// Returns error if Redis read pipeline fails (caller should abort batch so Kafka retries).
func (sm *SessionManager) ProcessEventsBatch(events []map[string]any, sessionRows *[]map[string]any, syntheticEvents *[]map[string]any) error {
	if len(events) == 0 {
		return nil
	}

	// 1. Collect unique device_ids
	deviceIDs := make(map[string]bool)
	for _, event := range events {
		did := stringVal(event, "device_id")
		if did != "" {
			deviceIDs[did] = true
		}
	}
	if len(deviceIDs) == 0 {
		return nil
	}

	// 2. Pipeline GET: fetch all sessions in one round-trip
	ctx := context.Background()
	pipe := sm.rdb.Pipeline()
	getCmds := make(map[string]*redis.StringCmd, len(deviceIDs))
	for did := range deviceIDs {
		getCmds[did] = pipe.Get(ctx, sm.sessionKey(did))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("[session-mgr] pipeline GET failed: %w", err)
	}

	// 3. Build in-memory session map
	sessions := make(map[string]*sessionState, len(deviceIDs))
	for did, cmd := range getCmds {
		raw, err := cmd.Bytes()
		if err != nil {
			if err != redis.Nil {
				logging.Errorf("[session-mgr] pipeline GET %s failed: %v", sm.sessionKey(did), err)
			}
			continue
		}
		var s sessionState
		if err := json.Unmarshal(raw, &s); err != nil {
			logging.Errorf("[session-mgr] unmarshal session for device %s failed: %v", did, err)
			continue
		}
		sessions[did] = &s
	}

	// 4. Process events sequentially (preserving order per device_id)
	// Track which sessions need save/delete
	modifiedSessions := make(map[string]bool)
	deletedSessions := make(map[string]bool)

	for _, event := range events {
		deviceID := stringVal(event, "device_id")
		if deviceID == "" {
			continue
		}

		eventName := stringVal(event, "name")

		if eventName == "Application Opened" {
			// End existing session
			if existing, ok := sessions[deviceID]; ok {
				sm.endSessionInMemory(existing, deviceID, event, sessionRows, syntheticEvents)
				delete(sessions, deviceID)
				deletedSessions[deviceID] = true
			}

			// Create new session
			session := sm.newSession(event)
			session.EventCount = 1
			sessions[deviceID] = session
			modifiedSessions[deviceID] = true
			delete(deletedSessions, deviceID)
			event["session_id"] = session.ID

			*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
			*syntheticEvents = append(*syntheticEvents, sm.syntheticEvent(event, "session_start", session.ID))
			continue
		}

		if eventName == "Application Backgrounded" {
			session, ok := sessions[deviceID]
			if ok {
				event["session_id"] = session.ID
				session.EventCount++
				session.EndedAt = formatTime(event["created_at"])
				if p := stringVal(event, "path"); p != "" {
					if session.EntryPath == "" {
						session.EntryPath = p
					}
					session.ExitPath = p
				}
				sm.endSessionInMemory(session, deviceID, event, sessionRows, syntheticEvents)
				delete(sessions, deviceID)
				deletedSessions[deviceID] = true
				delete(modifiedSessions, deviceID)
			}
			continue
		}

		// Regular event
		session, ok := sessions[deviceID]
		if !ok {
			// Crash recovery: create new session
			session = sm.newSession(event)
			sessions[deviceID] = session
			delete(deletedSessions, deviceID)
			*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
		}

		// Cancel previous version
		*sessionRows = append(*sessionRows, sm.sessionToRow(session, -1))

		session.EventCount++
		if stringVal(event, "name") == "screen_view" {
			session.ScreenViewCount++
		}
		session.EndedAt = formatTime(event["created_at"])
		if p := stringVal(event, "path"); p != "" {
			if session.EntryPath == "" {
				session.EntryPath = p
			}
			session.ExitPath = p
		}
		if rev, ok := event["revenue"].(uint64); ok {
			session.Revenue += float64(rev)
		}
		session.Version++

		pid := stringVal(event, "profile_id")
		did := stringVal(event, "device_id")
		if pid != "" && pid != did {
			session.ProfileID = pid
		}

		modifiedSessions[deviceID] = true
		event["session_id"] = session.ID

		*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
	}

	// 5. Pipeline WRITE: batch all saves/deletes in one round-trip
	writePipe := sm.rdb.Pipeline()
	for did := range modifiedSessions {
		session := sessions[did]
		if session == nil {
			continue
		}
		data, err := json.Marshal(session)
		if err != nil {
			logging.Errorf("[session-mgr] marshal session for device %s failed: %v", did, err)
			continue
		}
		writePipe.SetEx(ctx, sm.sessionKey(did), data, sessionTTL)
	}
	for did := range deletedSessions {
		writePipe.Del(ctx, sm.sessionKey(did))
	}
	if _, err := writePipe.Exec(ctx); err != nil && err != redis.Nil {
		logging.Errorf("[session-mgr] pipeline WRITE failed: %v (session cache may be stale)", err)
	}
	return nil
}

// endSessionInMemory ends a session without Redis I/O (used by batch processing).
func (sm *SessionManager) endSessionInMemory(session *sessionState, deviceID string, event map[string]any, sessionRows *[]map[string]any, syntheticEvents *[]map[string]any) {
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, -1))
	session.Version++
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
	*syntheticEvents = append(*syntheticEvents, sm.syntheticEvent(event, "session_end", session.ID))
}

// ProcessEvent processes an event through session management.
// Mutates event["session_id"]. Appends session rows and synthetic events.
func (sm *SessionManager) ProcessEvent(event map[string]any, sessionRows *[]map[string]any, syntheticEvents *[]map[string]any) {
	deviceID := stringVal(event, "device_id")
	if deviceID == "" {
		return
	}

	eventName := stringVal(event, "name")

	if eventName == "Application Opened" {
		// End any existing session first
		existing := sm.getSession(deviceID)
		if existing != nil {
			sm.endSession(existing, deviceID, event, sessionRows, syntheticEvents)
		}

		// Create new session
		session := sm.newSession(event)
		session.EventCount = 1
		sm.saveSession(deviceID, session)
		event["session_id"] = session.ID

		// Write initial session row (sign=+1, v=1)
		*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))

		// Synthetic session_start event
		*syntheticEvents = append(*syntheticEvents, sm.syntheticEvent(event, "session_start", session.ID))
		return
	}

	if eventName == "Application Backgrounded" {
		session := sm.getSession(deviceID)
		if session != nil {
			event["session_id"] = session.ID
			session.EventCount++
			session.EndedAt = formatTime(event["created_at"])
			if p := stringVal(event, "path"); p != "" {
				if session.EntryPath == "" {
					session.EntryPath = p
				}
				session.ExitPath = p
			}
			sm.endSession(session, deviceID, event, sessionRows, syntheticEvents)
		}
		return
	}

	// Regular event — attach to current session
	session := sm.getSession(deviceID)
	if session == nil {
		// Crash recovery: create a new session
		session = sm.newSession(event)
		*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
	}

	// Cancel previous version
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, -1))

	// Update session state
	session.EventCount++
	if stringVal(event, "name") == "screen_view" {
		session.ScreenViewCount++
	}
	session.EndedAt = formatTime(event["created_at"])
	if p := stringVal(event, "path"); p != "" {
		if session.EntryPath == "" {
			session.EntryPath = p
		}
		session.ExitPath = p
	}
	if rev, ok := event["revenue"].(uint64); ok {
		session.Revenue += float64(rev)
	}
	session.Version++

	// Update profile_id if user identified during session
	pid := stringVal(event, "profile_id")
	did := stringVal(event, "device_id")
	if pid != "" && pid != did {
		session.ProfileID = pid
	}

	sm.saveSession(deviceID, session)
	event["session_id"] = session.ID

	// Write updated session row (sign=+1)
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))
}

func (sm *SessionManager) endSession(session *sessionState, deviceID string, event map[string]any, sessionRows *[]map[string]any, syntheticEvents *[]map[string]any) {
	// Cancel previous version
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, -1))

	// Write final version
	session.Version++
	*sessionRows = append(*sessionRows, sm.sessionToRow(session, 1))

	// Synthetic session_end event
	*syntheticEvents = append(*syntheticEvents, sm.syntheticEvent(event, "session_end", session.ID))

	sm.deleteSession(deviceID)
}

func (sm *SessionManager) syntheticEvent(sourceEvent map[string]any, name, sessionID string) map[string]any {
	return map[string]any{
		"id":              uuid.New().String(),
		"name":            name,
		"sdk_name":        stringVal(sourceEvent, "sdk_name"),
		"sdk_version":     stringVal(sourceEvent, "sdk_version"),
		"device_id":       stringVal(sourceEvent, "device_id"),
		"profile_id":      stringVal(sourceEvent, "profile_id"),
		"project_id":      sm.projectID,
		"session_id":      sessionID,
		"path":            "",
		"origin":          "",
		"referrer":        "",
		"referrer_name":   "",
		"referrer_type":   "",
		"revenue":         uint64(0),
		"duration":        uint64(0),
		"properties":      map[string]string{},
		"created_at":      sourceEvent["created_at"],
		"country":         stringValOr(sourceEvent, "country", "\x00\x00"),
		"city":            stringVal(sourceEvent, "city"),
		"region":          stringVal(sourceEvent, "region"),
		"longitude":       sourceEvent["longitude"],
		"latitude":        sourceEvent["latitude"],
		"os":              stringVal(sourceEvent, "os"),
		"os_version":      stringVal(sourceEvent, "os_version"),
		"browser":         "",
		"browser_version": "",
		"device":          stringVal(sourceEvent, "device"),
		"brand":           stringVal(sourceEvent, "brand"),
		"model":           stringVal(sourceEvent, "model"),
		"imported_at":     nil,
	}
}

// Helpers

func stringVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func stringValOr(m map[string]any, key, def string) string {
	s := stringVal(m, key)
	if s == "" {
		return def
	}
	return s
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func formatTime(v any) string {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case string:
		return t
	default:
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
}

func parseTimestampMs(s string) float64 {
	s2 := s
	if len(s2) > 0 {
		s2 = s2[:min(len(s2), 35)] // truncate overly long strings
		// Try RFC3339Nano
		t, err := time.Parse(time.RFC3339Nano, s2)
		if err == nil {
			return float64(t.UnixMilli())
		}
		// Try without tz
		t, err = time.Parse("2006-01-02T15:04:05.999999999", s2)
		if err == nil {
			return float64(t.UnixMilli())
		}
	}
	return 0
}
