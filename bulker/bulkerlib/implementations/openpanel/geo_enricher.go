package openpanel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jitsucom/bulker/jitsubase/logging"
)

const geoCacheTTL = 1 * time.Hour
const cacheEvictInterval = 10 * time.Minute

type geoResult struct {
	country   string
	city      string
	region    string
	latitude  float32
	longitude float32
	hasCoords bool
	expiresAt time.Time
}

type GeoEnricher struct {
	serviceURL    string
	client        *http.Client
	mu            sync.RWMutex
	cache         map[string]geoResult
	lastEvictedAt time.Time
}

func NewGeoEnricher(serviceURL string) *GeoEnricher {
	if serviceURL == "" {
		logging.Errorf("[geo-enricher] geoServiceUrl is empty — geo enrichment will be disabled")
	} else {
		logging.Infof("[geo-enricher] initialized with service URL: %s", serviceURL)
	}
	return &GeoEnricher{
		serviceURL: serviceURL,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		cache: make(map[string]geoResult),
	}
}

func (g *GeoEnricher) Enrich(event map[string]any, ip string) {
	if g.serviceURL == "" {
		return
	}
	if ip == "" {
		logging.Warnf("[geo-enricher] event has no IP, skipping geo enrichment")
		return
	}

	// Skip private/loopback IPs
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsUnspecified() {
		logging.Debugf("[geo-enricher] skipping private/loopback IP: %s", ip)
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

	url := fmt.Sprintf("%s/api/geo-lookup/%s?exclude_whois=true", g.serviceURL, ip)
	resp, err := g.client.Get(url)
	if err != nil {
		logging.Errorf("[geo-enricher] failed to call %s: %v", url, err)
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	if resp.StatusCode != 200 {
		logging.Errorf("[geo-enricher] unexpected status %d from %s", resp.StatusCode, url)
		resp.Body.Close()
		return
	}
	defer resp.Body.Close()

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		logging.Errorf("[geo-enricher] failed to decode response for IP %s: %v", ip, err)
		return
	}

	result := parseGeoResponse(data)
	result.expiresAt = time.Now().Add(geoCacheTTL)

	g.mu.Lock()
	g.cache[ip] = result
	// Periodically evict expired entries to prevent unbounded growth
	now := time.Now()
	if now.Sub(g.lastEvictedAt) > cacheEvictInterval {
		for k, v := range g.cache {
			if now.After(v.expiresAt) {
				delete(g.cache, k)
			}
		}
		g.lastEvictedAt = now
	}
	g.mu.Unlock()

	applyGeoResult(event, result)
}

// EnrichBatch performs geo enrichment for all events in a single HTTP call
// to the Gunter batch endpoint. Unique uncached IPs are sent in one POST request.
// Returns error if Gunter is unreachable (caller should abort batch so Kafka retries).
// Individual IPs returning null is fine and not an error.
func (g *GeoEnricher) EnrichBatch(events []map[string]any, ips []string) error {
	if g.serviceURL == "" || len(events) == 0 {
		return nil
	}

	now := time.Now()

	// Collect unique IPs that need lookup, applying cache
	uncachedIPs := make([]string, 0)
	cachedResults := make(map[string]geoResult)
	seen := make(map[string]bool)

	g.mu.RLock()
	for _, ip := range ips {
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true

		// Skip private/loopback IPs
		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsUnspecified() {
			continue
		}

		// Check cache
		if cached, found := g.cache[ip]; found && now.Before(cached.expiresAt) {
			cachedResults[ip] = cached
		} else {
			uncachedIPs = append(uncachedIPs, ip)
		}
	}
	g.mu.RUnlock()

	// Fetch uncached IPs from Gunter batch endpoint
	freshResults := make(map[string]geoResult)
	if len(uncachedIPs) > 0 {
		// Chunk large batches to avoid huge payloads
		const chunkSize = 5000
		for i := 0; i < len(uncachedIPs); i += chunkSize {
			end := i + chunkSize
			if end > len(uncachedIPs) {
				end = len(uncachedIPs)
			}
			chunk := uncachedIPs[i:end]

			body, err := json.Marshal(map[string]any{"ips": chunk})
			if err != nil {
				return fmt.Errorf("[geo-enricher] failed to marshal batch request: %w", err)
			}

			url := fmt.Sprintf("%s/api/geo-lookup/batch?lang=en", g.serviceURL)
			resp, err := g.client.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				if resp != nil {
					resp.Body.Close()
				}
				return fmt.Errorf("[geo-enricher] Gunter unreachable: %w", err)
			}
			if resp.StatusCode != 200 {
				resp.Body.Close()
				return fmt.Errorf("[geo-enricher] Gunter returned status %d", resp.StatusCode)
			}

			var batchData map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&batchData); err != nil {
				resp.Body.Close()
				return fmt.Errorf("[geo-enricher] failed to decode Gunter response: %w", err)
			}
			resp.Body.Close()

			for _, ip := range chunk {
				ipData, ok := batchData[ip]
				if !ok || ipData == nil {
					continue
				}
				if ipMap, ok := ipData.(map[string]any); ok {
					result := parseGeoResponse(ipMap)
					result.expiresAt = now.Add(geoCacheTTL)
					freshResults[ip] = result
				}
			}
		}

		// Update cache with fresh results
		g.mu.Lock()
		for ip, result := range freshResults {
			g.cache[ip] = result
		}
		// Periodic eviction
		if now.Sub(g.lastEvictedAt) > cacheEvictInterval {
			for k, v := range g.cache {
				if now.After(v.expiresAt) {
					delete(g.cache, k)
				}
			}
			g.lastEvictedAt = now
		}
		g.mu.Unlock()
	}

	// Apply geo data to all events
	if len(events) != len(ips) {
		return fmt.Errorf("[geo-enricher] events/ips length mismatch: %d vs %d", len(events), len(ips))
	}
	for i, event := range events {
		ip := ips[i]
		if result, ok := cachedResults[ip]; ok {
			applyGeoResult(event, result)
		} else if result, ok := freshResults[ip]; ok {
			applyGeoResult(event, result)
		}
	}

	logging.Infof("[geo-enricher] batch: %d events, %d unique IPs, %d cached, %d fetched",
		len(events), len(seen), len(cachedResults), len(freshResults))
	return nil
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
		lat, hasLat := location["latitude"].(float64)
		lon, hasLon := location["longitude"].(float64)
		if hasLat && hasLon {
			r.latitude = float32(lat)
			r.longitude = float32(lon)
			r.hasCoords = true
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
	if r.hasCoords {
		event["latitude"] = r.latitude
		event["longitude"] = r.longitude
	}
}
