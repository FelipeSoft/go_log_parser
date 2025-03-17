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

func NewJsonLogParser(regex *regexp.Regexp) *JsonLogParser {
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

	level, _ := jsonLog["level"].(string)
	service, _ := jsonLog["service"].(string)
	message, _ := jsonLog["message"].(string)

	return entity.LogEntry{
		Timestamp: timestamp,
		Level:     level,
		Service:   service,
		Message:   message,
		Others:    jsonLog,
	}, nil
}