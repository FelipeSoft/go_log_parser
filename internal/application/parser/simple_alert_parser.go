package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type SimpleAlertParser struct {
	regex *regexp.Regexp
}

func NewSimpleAlertParser(regex *regexp.Regexp) entity.LogParser {
	return &SimpleAlertParser{regex: regex}
}

func (p *SimpleAlertParser) PreservesRaw() bool {
    return false
}

func (p *SimpleAlertParser) Parse(line string) (entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return entity.LogEntry{}, fmt.Errorf("using the SimpleAlertParser does not match with the format provided on the line %s of the log", line)
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", matches[1])
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("using the HttpLogParser an error occurred on the line %s of the log in the parsing time moment", line)
	}

	entry := entity.LogEntry{
		Timestamp: timestamp,
		Level:     matches[2],
		Service:   matches[3],
		Message:   matches[4],
	}

	return entry, nil
}
