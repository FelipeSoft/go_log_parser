package entity

import (
	"time"
)

type RawEntry struct {
	StartAt time.Time
	Raw     string
}

type LogEntry struct {
    StartAt    time.Time
    Raw        string
    Timestamp  time.Time
    Level      string
    Service    string
    Message    string
    IPAddress  string
    Method     string
    Path       string
    StatusCode int
    UserAgent  string
    Metadata   map[string]any
}

func (ly *LogEntry) CalculateTotalProcessedTimeByMilliseconds() int64 {
    finishedAt := time.Since(ly.StartAt)
    return finishedAt.Milliseconds()
}
