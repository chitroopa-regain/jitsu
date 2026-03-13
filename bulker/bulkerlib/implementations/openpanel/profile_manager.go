package openpanel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/redis/go-redis/v9"
)

const profileTTL = 24 * time.Hour

type ProfileManager struct {
	rdb       *redis.Client
	projectID string
}

func NewProfileManager(rdb *redis.Client, projectID string) *ProfileManager {
	return &ProfileManager{rdb: rdb, projectID: projectID}
}

func (pm *ProfileManager) profileKey(profileID string) string {
	return "profile:" + profileID
}

// ProcessIdentify processes an identify event, producing profile and alias rows.
func (pm *ProfileManager) ProcessIdentify(msg map[string]any, profileRows *[]map[string]any, aliasRows *[]map[string]any) {
	userID := firstNonEmpty(msg, "userId", "user_id")
	anonymousID := firstNonEmpty(msg, "anonymousId", "anonymous_id")
	profileID := userID
	if profileID == "" {
		profileID = anonymousID
	}
	if profileID == "" {
		return
	}

	traits := getTraits(msg)
	ctx := getMap(msg, "context")
	piAdj, _ := adjustTimestamp(msg)
	timestamp := piAdj.UTC().Format(time.RFC3339Nano)

	// Get existing profile from Redis cache
	var existingProps map[string]string
	cached, err := pm.rdb.Get(context.Background(), pm.profileKey(profileID)).Bytes()
	if err == nil {
		var existing map[string]any
		if json.Unmarshal(cached, &existing) == nil {
			if p, ok := existing["properties"].(map[string]any); ok {
				existingProps = make(map[string]string)
				for k, v := range p {
					existingProps[k] = fmt.Sprint(v)
				}
			}
		}
	}
	if existingProps == nil {
		existingProps = make(map[string]string)
	}

	// Device properties from context
	deviceProps := extractDeviceProps(ctx)

	// Merge: existing → device → traits (traits win)
	newProps := make(map[string]string)
	for k, v := range existingProps {
		newProps[k] = v
	}
	for k, v := range deviceProps {
		newProps[k] = v
	}

	standardFields := map[string]bool{
		"first_name": true, "last_name": true, "email": true, "avatar": true,
		"firstName": true, "lastName": true,
	}
	for k, v := range traits {
		if !standardFields[k] {
			newProps[k] = fmt.Sprint(v)
		}
	}

	firstName := getString(traits, "first_name", getString(traits, "firstName", ""))
	if firstName == "" {
		firstName = profileID
	}
	profileRow := map[string]any{
		"id":          profileID,
		"is_external": userID != "",
		"first_name":  firstName,
		"last_name":   getString(traits, "last_name", getString(traits, "lastName", "")),
		"email":       getString(traits, "email", ""),
		"avatar":      getString(traits, "avatar", ""),
		"properties":  newProps,
		"project_id":  pm.projectID,
		"created_at":  timestamp,
	}
	*profileRows = append(*profileRows, profileRow)

	// Cache in Redis
	data, err := json.Marshal(profileRow)
	if err != nil {
		logging.Errorf("[profile-mgr] marshal profile %s failed: %v", profileID, err)
	} else if err := pm.rdb.SetEx(context.Background(), pm.profileKey(profileID), data, profileTTL).Err(); err != nil {
		logging.Errorf("[profile-mgr] redis SETEX %s failed: %v", pm.profileKey(profileID), err)
	}

	// If both userId and anonymousId present, create an alias
	if userID != "" && anonymousID != "" && userID != anonymousID {
		*aliasRows = append(*aliasRows, map[string]any{
			"project_id": pm.projectID,
			"profile_id": userID,
			"alias":      anonymousID,
			"created_at": timestamp,
		})
	}
}

// EnsureProfile lazily creates a profile from a track/screen event if one doesn't exist.
// country comes from geo enrichment (already applied to the event).
func (pm *ProfileManager) EnsureProfile(msg map[string]any, country string, profileRows *[]map[string]any) {
	userID := firstNonEmpty(msg, "userId", "user_id")
	anonymousID := firstNonEmpty(msg, "anonymousId", "anonymous_id")
	profileID := userID
	if profileID == "" {
		profileID = anonymousID
	}
	if profileID == "" {
		return
	}

	// Fast path: profile already cached
	exists, _ := pm.rdb.Exists(context.Background(), pm.profileKey(profileID)).Result()
	if exists > 0 {
		return
	}

	ctx := getMap(msg, "context")
	epAdj, _ := adjustTimestamp(msg)
	timestamp := epAdj.UTC().Format(time.RFC3339Nano)

	deviceProps := extractDeviceProps(ctx)
	if country != "" && country != "\x00\x00" {
		deviceProps["country"] = country
	}

	profileRow := map[string]any{
		"id":          profileID,
		"is_external": userID != "",
		"first_name":  profileID,
		"last_name":   "",
		"email":       "",
		"avatar":      "",
		"properties":  deviceProps,
		"project_id":  pm.projectID,
		"created_at":  timestamp,
	}
	*profileRows = append(*profileRows, profileRow)

	data, err := json.Marshal(profileRow)
	if err != nil {
		logging.Errorf("[profile-mgr] marshal profile %s failed: %v", profileID, err)
	} else if err := pm.rdb.SetEx(context.Background(), pm.profileKey(profileID), data, profileTTL).Err(); err != nil {
		logging.Errorf("[profile-mgr] redis SETEX %s failed: %v", pm.profileKey(profileID), err)
	}
}

// EnsureProfilesBatch checks all profile_ids via Redis Pipeline and creates profiles
// for any that don't exist. 2 Redis round-trips total (Pipeline EXISTS + Pipeline SETEX).
// Returns error if Redis read pipeline fails (caller should abort batch so Kafka retries).
func (pm *ProfileManager) EnsureProfilesBatch(inputs []profileInput, enrichedEvents []map[string]any, profileRows *[]map[string]any, identifiedIDs map[string]bool) error {
	if len(inputs) == 0 {
		return nil
	}

	// 1. Collect unique profile_ids
	type profileIdx struct {
		input    profileInput
		eventIdx int // index into enrichedEvents for country
	}
	uniqueProfiles := make(map[string]profileIdx)
	for i, input := range inputs {
		if input.profileID == "" {
			continue
		}
		// Skip profiles already processed by ProcessIdentifyBatch
		if identifiedIDs[input.profileID] {
			continue
		}
		if _, exists := uniqueProfiles[input.profileID]; !exists {
			uniqueProfiles[input.profileID] = profileIdx{input: input, eventIdx: i}
		}
	}
	if len(uniqueProfiles) == 0 {
		return nil
	}

	// 2. Pipeline EXISTS: check all profile_ids in one round-trip
	ctx := context.Background()
	pipe := pm.rdb.Pipeline()
	existsCmds := make(map[string]*redis.IntCmd, len(uniqueProfiles))
	for pid := range uniqueProfiles {
		existsCmds[pid] = pipe.Exists(ctx, pm.profileKey(pid))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("[profile-mgr] pipeline EXISTS failed: %w", err)
	}

	// 3. Find missing profiles
	var missingProfiles []profileIdx
	for pid, cmd := range existsCmds {
		exists, _ := cmd.Result()
		if exists == 0 {
			missingProfiles = append(missingProfiles, uniqueProfiles[pid])
		}
	}
	if len(missingProfiles) == 0 {
		return nil
	}

	// 4. Create missing profiles and Pipeline SETEX in one round-trip
	writePipe := pm.rdb.Pipeline()
	for _, p := range missingProfiles {
		deviceProps := extractDeviceProps(p.input.context)
		if p.eventIdx < len(enrichedEvents) {
			if country, ok := enrichedEvents[p.eventIdx]["country"].(string); ok && country != "" && country != "\x00\x00" {
				deviceProps["country"] = country
			}
		}

		profileRow := map[string]any{
			"id":          p.input.profileID,
			"is_external": p.input.userID != "",
			"first_name":  p.input.profileID,
			"last_name":   "",
			"email":       "",
			"avatar":      "",
			"properties":  deviceProps,
			"project_id":  pm.projectID,
			"created_at":  p.input.timestamp,
		}
		*profileRows = append(*profileRows, profileRow)

		data, err := json.Marshal(profileRow)
		if err != nil {
			logging.Errorf("[profile-mgr] marshal profile %s failed: %v", p.input.profileID, err)
			continue
		}
		writePipe.SetEx(ctx, pm.profileKey(p.input.profileID), data, profileTTL)
	}
	if _, err := writePipe.Exec(ctx); err != nil && err != redis.Nil {
		logging.Errorf("[profile-mgr] pipeline SETEX failed: %v (profile cache may be stale)", err)
	}
	return nil
}

// ProcessIdentifyBatch processes all identify events via Redis Pipeline.
// 2 Redis round-trips total (Pipeline GET + Pipeline SETEX).
// Returns error if Redis read pipeline fails (caller should abort batch so Kafka retries).
func (pm *ProfileManager) ProcessIdentifyBatch(inputs []profileInput, profileRows *[]map[string]any, aliasRows *[]map[string]any) (map[string]bool, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	// 1. Collect unique profile_ids
	uniqueIDs := make(map[string]bool)
	for _, input := range inputs {
		if input.profileID != "" {
			uniqueIDs[input.profileID] = true
		}
	}
	if len(uniqueIDs) == 0 {
		return nil, nil
	}

	// 2. Pipeline GET: fetch existing profiles in one round-trip
	ctx := context.Background()
	pipe := pm.rdb.Pipeline()
	getCmds := make(map[string]*redis.StringCmd, len(uniqueIDs))
	for pid := range uniqueIDs {
		getCmds[pid] = pipe.Get(ctx, pm.profileKey(pid))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("[profile-mgr] pipeline GET failed: %w", err)
	}

	// 3. Parse existing profiles into in-memory map
	existingProfiles := make(map[string]map[string]string)
	for pid, cmd := range getCmds {
		raw, err := cmd.Bytes()
		if err != nil {
			continue
		}
		var existing map[string]any
		if json.Unmarshal(raw, &existing) == nil {
			if p, ok := existing["properties"].(map[string]any); ok {
				props := make(map[string]string)
				for k, v := range p {
					props[k] = fmt.Sprint(v)
				}
				existingProfiles[pid] = props
			}
		}
	}

	// 4. Process each identify event and collect profile rows
	writePipe := pm.rdb.Pipeline()
	for _, input := range inputs {
		if input.profileID == "" {
			continue
		}

		existingProps := existingProfiles[input.profileID]
		if existingProps == nil {
			existingProps = make(map[string]string)
		}

		deviceProps := extractDeviceProps(input.context)

		newProps := make(map[string]string)
		for k, v := range existingProps {
			newProps[k] = v
		}
		for k, v := range deviceProps {
			newProps[k] = v
		}

		standardFields := map[string]bool{
			"first_name": true, "last_name": true, "email": true, "avatar": true,
			"firstName": true, "lastName": true,
		}
		for k, v := range input.traits {
			if !standardFields[k] {
				newProps[k] = fmt.Sprint(v)
			}
		}

		firstName := getString(input.traits, "first_name", getString(input.traits, "firstName", ""))
		if firstName == "" {
			firstName = input.profileID
		}
		profileRow := map[string]any{
			"id":          input.profileID,
			"is_external": input.userID != "",
			"first_name":  firstName,
			"last_name":   getString(input.traits, "last_name", getString(input.traits, "lastName", "")),
			"email":       getString(input.traits, "email", ""),
			"avatar":      getString(input.traits, "avatar", ""),
			"properties":  newProps,
			"project_id":  pm.projectID,
			"created_at":  input.timestamp,
		}
		*profileRows = append(*profileRows, profileRow)

		// Update in-memory cache for subsequent identify events in same batch
		existingProfiles[input.profileID] = newProps

		data, err := json.Marshal(profileRow)
		if err != nil {
			logging.Errorf("[profile-mgr] marshal profile %s failed: %v", input.profileID, err)
			continue
		}
		writePipe.SetEx(ctx, pm.profileKey(input.profileID), data, profileTTL)

		// Alias
		if input.userID != "" && input.anonymousID != "" && input.userID != input.anonymousID {
			*aliasRows = append(*aliasRows, map[string]any{
				"project_id": pm.projectID,
				"profile_id": input.userID,
				"alias":      input.anonymousID,
				"created_at": input.timestamp,
			})
		}
	}

	if _, err := writePipe.Exec(ctx); err != nil && err != redis.Nil {
		logging.Errorf("[profile-mgr] pipeline SETEX failed: %v (profile cache may be stale)", err)
	}
	return uniqueIDs, nil
}

// ProcessAlias processes an alias event.
func (pm *ProfileManager) ProcessAlias(msg map[string]any, aliasRows *[]map[string]any) {
	userID := firstNonEmpty(msg, "userId", "user_id")
	previousID := firstNonEmpty(msg, "previousId", "previous_id", "anonymousId", "anonymous_id")

	if userID != "" && previousID != "" {
		paAdj, _ := adjustTimestamp(msg)
		*aliasRows = append(*aliasRows, map[string]any{
			"project_id": pm.projectID,
			"profile_id": userID,
			"alias":      previousID,
			"created_at": paAdj.UTC().Format(time.RFC3339Nano),
		})
	}
}

func extractDeviceProps(ctx map[string]any) map[string]string {
	props := make(map[string]string)
	if osName := getNested(ctx, "os.name"); osName != "" {
		props["os"] = osName
	}
	if brand := getNested(ctx, "device.manufacturer"); brand != "" {
		props["brand"] = brand
	}
	if model := getNested(ctx, "device.model"); model != "" {
		props["model"] = model
	}
	if devType := getNested(ctx, "device.type"); devType != "" {
		if strings.EqualFold(devType, "android") {
			props["device"] = "mobile"
		} else {
			props["device"] = devType
		}
	}
	return props
}
