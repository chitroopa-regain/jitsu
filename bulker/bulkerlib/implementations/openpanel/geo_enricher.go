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

// ResolveBatch looks up geo data for a batch of IPs using cache + Gunter batch endpoint.
// Returns a map from IP to geoResult. Non-routable and empty IPs are silently skipped.
// Returns error if Gunter is unreachable (caller decides whether to abort or continue).
func (g *GeoEnricher) ResolveBatch(ips []string) (map[string]geoResult, error) {
	if g.serviceURL == "" || len(ips) == 0 {
		return nil, nil
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

		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsUnspecified() {
			continue
		}

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
		const chunkSize = 5000
		for i := 0; i < len(uncachedIPs); i += chunkSize {
			end := i + chunkSize
			if end > len(uncachedIPs) {
				end = len(uncachedIPs)
			}
			chunk := uncachedIPs[i:end]

			body, err := json.Marshal(map[string]any{"ips": chunk})
			if err != nil {
				return nil, fmt.Errorf("[geo-enricher] failed to marshal batch request: %w", err)
			}

			url := fmt.Sprintf("%s/api/geo-lookup/batch?lang=en", g.serviceURL)
			resp, err := g.client.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				if resp != nil {
					resp.Body.Close()
				}
				return nil, fmt.Errorf("[geo-enricher] Gunter unreachable: %w", err)
			}
			if resp.StatusCode != 200 {
				resp.Body.Close()
				return nil, fmt.Errorf("[geo-enricher] Gunter returned status %d", resp.StatusCode)
			}

			var batchData map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&batchData); err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("[geo-enricher] failed to decode Gunter response: %w", err)
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

	// Merge cached + fresh
	allResults := make(map[string]geoResult, len(cachedResults)+len(freshResults))
	for ip, r := range cachedResults {
		allResults[ip] = r
	}
	for ip, r := range freshResults {
		allResults[ip] = r
	}

	logging.Infof("[geo-enricher] resolve: %d IPs, %d unique, %d cached, %d fetched",
		len(ips), len(seen), len(cachedResults), len(freshResults))
	return allResults, nil
}

// EnrichBatch performs geo enrichment for all events in a single batch.
// Calls ResolveBatch for the lookup, then applies results to events in-place.
// Returns error if Gunter is unreachable (caller should abort batch so Kafka retries).
func (g *GeoEnricher) EnrichBatch(events []map[string]any, ips []string) error {
	if len(events) != len(ips) {
		return fmt.Errorf("[geo-enricher] events/ips length mismatch: %d vs %d", len(events), len(ips))
	}

	allResults, err := g.ResolveBatch(ips)
	if err != nil {
		return err
	}

	for i, ip := range ips {
		if result, ok := allResults[ip]; ok {
			applyGeoResult(events[i], result)
		}
	}
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
