package entity

import (
	"time"
)

type LogEntry struct {
    Timestamp  time.Time
    Level      string
    Service    string
    Message    string
    IPAddress  string
    Method     string
    Path       string
    StatusCode int
    UserAgent  string
    Metadata   map[string]interface{}
}