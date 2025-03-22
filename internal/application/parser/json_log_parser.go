package parser

import (
    "encoding/json"
    "fmt"
    "regexp"
    "time"

    "github.com/etl_app_transform_service/internal/domain/entity"
)

type JsonLogParser struct {
    regex *regexp.Regexp
}

func NewJsonLogParser(regex *regexp.Regexp) entity.LogParser {
    return &JsonLogParser{regex: regex}
}

func (p *JsonLogParser) Parse(line string) (entity.LogEntry, error) {
    var jsonLog map[string]interface{}
    if err := json.Unmarshal([]byte(line), &jsonLog); err != nil {
        return entity.LogEntry{}, fmt.Errorf("JSON parsing error: %w (line: %s)", err, line)
    }

    timestampStr, ok := jsonLog["timestamp"].(string)
    if !ok {
        return entity.LogEntry{}, fmt.Errorf("missing or invalid 'timestamp' field in log line: %s", line)
    }

    timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
    if err != nil {
        return entity.LogEntry{}, fmt.Errorf("error parsing timestamp (%s): %w", timestampStr, err)
    }

    return entity.LogEntry{
        Timestamp: timestamp,
        Level:     getString(jsonLog, "level"),
        Service:   getString(jsonLog, "service"),
        Message:   getString(jsonLog, "message"),
        Metadata:  jsonLog,
    }, nil
}

func (p *JsonLogParser) PreservesRaw() bool {
    return true
}

func getString(m map[string]interface{}, key string) string {
    if val, ok := m[key].(string); ok {
        return val
    }
    return ""
}