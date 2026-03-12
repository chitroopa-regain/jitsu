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

	traits := getMap(msg, "traits")
	ctx := getMap(msg, "context")
	timestamp := getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano))

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
	timestamp := getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano))

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

// ProcessAlias processes an alias event.
func (pm *ProfileManager) ProcessAlias(msg map[string]any, aliasRows *[]map[string]any) {
	userID := firstNonEmpty(msg, "userId", "user_id")
	previousID := firstNonEmpty(msg, "previousId", "previous_id", "anonymousId", "anonymous_id")

	if userID != "" && previousID != "" {
		*aliasRows = append(*aliasRows, map[string]any{
			"project_id": pm.projectID,
			"profile_id": userID,
			"alias":      previousID,
			"created_at": getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano)),
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
