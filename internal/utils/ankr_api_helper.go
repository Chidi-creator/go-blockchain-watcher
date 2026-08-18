package utils

import (
	"strings"
)

// MaskAPIKey returns the URL with the API key portion masked for safe logging
func MaskAPIKey(url string) string {
	if !strings.Contains(url, "ankr.com") {
		return url
	}

	// Split URL on /
	parts := strings.Split(url, "/")

	// If URL ends with API key (last segment), mask it
	if len(parts) > 4 && parts[len(parts)-1] != "" {
		// Keep first 4 chars, mask middle, keep last 4 chars
		key := parts[len(parts)-1]
		if len(key) > 8 {
			parts[len(parts)-1] = key[:4] + "..." + key[len(key)-4:]
		} else if len(key) > 0 {
			parts[len(parts)-1] = "***"
		}
		return strings.Join(parts, "/")
	}

	return url
}

// AddAPIKeyToURL ensures the API key is properly added to Ankr URLs
// Returns the URL with the API key appended if needed
func AddAPIKeyToURL(url string, apiKey string) string {
	if apiKey == "" || !strings.Contains(url, "ankr.com") {
		return url
	}

	// Check if API key is already present
	if strings.Contains(url, apiKey) {
		return url
	}

	// Add API key to URL
	if strings.HasSuffix(url, "/") {
		return url + apiKey
	} else {
		return url + "/" + apiKey
	}
}

// ExtractAPIKeyFromURL extracts the API key from an Ankr URL if present
// Returns the API key and the base URL without the key
func ExtractAPIKeyFromURL(url string) (string, string) {
	// Check if URL potentially contains an API key
	if !strings.Contains(url, "ankr.com") || !strings.Contains(url, "/") {
		return "", url
	}

	parts := strings.Split(url, "/")
	if len(parts) > 4 && parts[len(parts)-1] != "" {
		// Last part is potentially the API key
		apiKey := parts[len(parts)-1]
		// Base URL without the API key
		baseURL := strings.Join(parts[:len(parts)-1], "/")
		return apiKey, baseURL
	}

	return "", url
}
