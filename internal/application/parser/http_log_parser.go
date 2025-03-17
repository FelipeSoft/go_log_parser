package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type HttpLogParser struct {
	regex *regexp.Regexp
}

func NewHttpLogParser(regex *regexp.Regexp) *HttpLogParser {
	return &HttpLogParser{regex: regex}
}

func (p *HttpLogParser) Parse(line string) (entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return entity.LogEntry{}, fmt.Errorf("using the HttpLogParser does not match with the format provided on the line %s of the log", line)
	}

	timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", matches[2])
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("using the HttpLogParser an error occurred on the line %s of the log in the parsing time moment", line)
	}

	integer, err := strconv.Atoi(matches[5])

	if err != nil {
		return entity.LogEntry{}, err
	}

	entry := entity.LogEntry{
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
