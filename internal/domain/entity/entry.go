package entity

import (
	"time"
)

type LogEntry struct {
	Timestamp  time.Time      `json:"timestamp"`
	Level      string         `json:"level"`
	Service    string         `json:"service"`
	Message    string         `json:"message"`
	IPAddress  *string        `json:"ip_address,omitempty"`
	Method     *string        `json:"method,omitempty"`
	Path       *string        `json:"path,omitempty"`
	StatusCode *int           `json:"status_code,omitempty"`
	UserAgent  *string        `json:"user_agent,omitempty"`
	Others     map[string]any `json:"others,omitempty"`
}