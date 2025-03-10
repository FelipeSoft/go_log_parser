package parser

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/entity"
)

type JsonLogParser struct {
	regex *regexp.Regexp
}

func NewJsonLogParser(regex *regexp.Regexp) *JsonLogParser {
	return &JsonLogParser{
		regex: regex,
	}
}

func (p *JsonLogParser) Parse(line string, lineCount int) (*entity.LogEntry, error) {
	var jsonLog map[string]interface{}
	err := json.Unmarshal([]byte(line), &jsonLog)
	if err != nil {
		return nil, fmt.Errorf("using the JsonLogParser does not match with the format provided on the line %d of the log.", lineCount)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, jsonLog["timestamp"].(string))
	if err != nil {
		return nil, fmt.Errorf("using the HttpLogParser an error occurred on the line %d of the log in the parsing time moment.", lineCount)
	}

	entry := &entity.LogEntry{
		Timestamp: timestamp,
		Level:     jsonLog["level"].(string),
		Service:   jsonLog["service"].(string),
		Message:   jsonLog["message"].(string),
		Others:    jsonLog,
	}

	return entry, nil
}
