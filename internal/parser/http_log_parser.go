package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/etl_app_transform_service/internal/entity"
)

type HttpLogParser struct {
	regex *regexp.Regexp
}

func NewHttpLogParser(regex *regexp.Regexp) *HttpLogParser {
	return &HttpLogParser{regex: regex}
}

func (p *HttpLogParser) Parse(line string, lineCount int) (*entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("using the HttpLogParser does not match with the format provided on the line %d of the log.", lineCount)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, matches[2])
	if err != nil {
		return nil, fmt.Errorf("using the HttpLogParser an error occurred on the line %d of the log in the parsing time moment.", lineCount)
	}

	integer, err := strconv.Atoi(matches[5])

	entry := &entity.LogEntry{
		Timestamp:  timestamp,
		Level:      "INFO",
		Service:    matches[3],
		Message:    matches[4],
		IPAddress:  &matches[1],
		Method:     &matches[3],
		Path:       &matches[4],
		StatusCode: &integer,
		UserAgent:  &matches[6],
	}

	return entry, nil
}
