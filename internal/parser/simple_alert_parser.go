package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/entity"
)

type SimpleAlertParser struct {
	regex *regexp.Regexp
}

func NewSimpleAlertParser(regex *regexp.Regexp) *SimpleAlertParser {
	return &SimpleAlertParser{regex: regex}
}

func (p *SimpleAlertParser) Parse(line string, lineCount int) (*entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("using the SimpleAlertParser does not match with the format provided on the line %d of the log.", lineCount)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, matches[1])
	if err != nil {
		return nil, fmt.Errorf("using the HttpLogParser an error occurred on the line %d of the log in the parsing time moment.", lineCount)
	}

	entry := &entity.LogEntry{
		Timestamp: timestamp,
		Level:     matches[2],
		Service:   matches[3],
		Message:   matches[4],
	}

	return entry, nil
}
