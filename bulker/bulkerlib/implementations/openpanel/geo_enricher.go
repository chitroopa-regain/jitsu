package openpanel

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

type GeoEnricher struct {
	serviceURL string
	client     *http.Client
}

func NewGeoEnricher(serviceURL string) *GeoEnricher {
	return &GeoEnricher{
		serviceURL: serviceURL,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
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

	if country, ok := data["country"].(map[string]any); ok {
		if iso, ok := country["iso_code"].(string); ok && len(iso) >= 2 {
			event["country"] = iso[:2]
		}
	}
	if city, ok := data["city"].(map[string]any); ok {
		if name, ok := city["name"].(string); ok {
			event["city"] = name
		}
	}
	if subdivisions, ok := data["subdivisions"].([]any); ok && len(subdivisions) > 0 {
		if sub, ok := subdivisions[0].(map[string]any); ok {
			if name, ok := sub["name"].(string); ok {
				event["region"] = name
			}
		}
	}
	if location, ok := data["location"].(map[string]any); ok {
		if lat, ok := location["latitude"].(float64); ok {
			event["latitude"] = float32(lat)
		}
		if lon, ok := location["longitude"].(float64); ok {
			event["longitude"] = float32(lon)
		}
	}
}
