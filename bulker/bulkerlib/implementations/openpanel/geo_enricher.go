package openpanel

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const geoCacheTTL = 1 * time.Hour

type geoResult struct {
	country   string
	city      string
	region    string
	latitude  float32
	longitude float32
	expiresAt time.Time
}

type GeoEnricher struct {
	serviceURL string
	client     *http.Client
	mu         sync.RWMutex
	cache      map[string]geoResult
}

func NewGeoEnricher(serviceURL string) *GeoEnricher {
	return &GeoEnricher{
		serviceURL: serviceURL,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		cache: make(map[string]geoResult),
	}
}

func (g *GeoEnricher) Enrich(event map[string]any, ip string) {
	if ip == "" || g.serviceURL == "" {
		return
	}

	// Skip private/loopback IPs
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsUnspecified() {
		return
	}

	// Check in-memory cache
	g.mu.RLock()
	cached, found := g.cache[ip]
	g.mu.RUnlock()
	if found && time.Now().Before(cached.expiresAt) {
		applyGeoResult(event, cached)
		return
	}

	resp, err := g.client.Get(fmt.Sprintf("%s/api/geo-lookup/%s?exclude_whois=true", g.serviceURL, ip))
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}

	result := parseGeoResponse(data)
	result.expiresAt = time.Now().Add(geoCacheTTL)

	g.mu.Lock()
	g.cache[ip] = result
	g.mu.Unlock()

	applyGeoResult(event, result)
}

func parseGeoResponse(data map[string]any) geoResult {
	var r geoResult
	if country, ok := data["country"].(map[string]any); ok {
		if iso, ok := country["iso_code"].(string); ok && len(iso) >= 2 {
			r.country = iso[:2]
		}
	}
	if city, ok := data["city"].(map[string]any); ok {
		if name, ok := city["name"].(string); ok {
			r.city = name
		}
	}
	if subdivisions, ok := data["subdivisions"].([]any); ok && len(subdivisions) > 0 {
		if sub, ok := subdivisions[0].(map[string]any); ok {
			if name, ok := sub["name"].(string); ok {
				r.region = name
			}
		}
	}
	if location, ok := data["location"].(map[string]any); ok {
		if lat, ok := location["latitude"].(float64); ok {
			r.latitude = float32(lat)
		}
		if lon, ok := location["longitude"].(float64); ok {
			r.longitude = float32(lon)
		}
	}
	return r
}

func applyGeoResult(event map[string]any, r geoResult) {
	if r.country != "" {
		event["country"] = r.country
	}
	if r.city != "" {
		event["city"] = r.city
	}
	if r.region != "" {
		event["region"] = r.region
	}
	if r.latitude != 0 {
		event["latitude"] = r.latitude
	}
	if r.longitude != 0 {
		event["longitude"] = r.longitude
	}
}
