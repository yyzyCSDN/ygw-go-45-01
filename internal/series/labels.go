package series

import (
	"errors"
	"strings"
)

// ErrInvalidLabel is returned when a label key or value is malformed.
var ErrInvalidLabel = errors.New("invalid label")

// ValidName reports whether a metric name is acceptable.
func ValidName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '_' || r == '.' || r == '-' {
			continue
		}
		return false
	}
	return true
}

// SanitizeLabels validates and normalizes a label set. Keys must be non-empty
// and contain only letters, digits, underscores and dots.
func SanitizeLabels(labels map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if k == "" || len(k) > 64 {
			return nil, ErrInvalidLabel
		}
		for _, r := range k {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
				r == '_' || r == '.' {
				continue
			}
			return nil, ErrInvalidLabel
		}
		out[k] = strings.TrimSpace(v)
	}
	return out, nil
}
