package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type BracketsStructuredParser struct {
	regex *regexp.Regexp
}

func NewBracketsStructuredParser(regex *regexp.Regexp) *BracketsStructuredParser {
	return &BracketsStructuredParser{regex: regex}
}

// 192.168.156.97 - - [09/Mar/2025:20:43:37 +0000] "GET /api/v1/reports HTTP/1.1" 200 2048 "-" "Mozilla/5.0 (Linux; Android 11)"
func (p *BracketsStructuredParser) Parse(line string) (entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return entity.LogEntry{}, fmt.Errorf("using the BracketsStructuredParser does not match with the format provided on the line %s of the log", line)
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", matches[1])
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("using the BracketsStructuredParser an error occurred on the line %s of the log in the parsing time moment", line)
	}

	entry := entity.LogEntry{
		Level:     matches[1],
		Timestamp: timestamp,
		IPAddress: &matches[3],
		Message:   matches[4],
	}

	return entry, nil
}
