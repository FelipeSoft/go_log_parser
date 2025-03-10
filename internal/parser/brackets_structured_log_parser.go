package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/entity"
)

type BracketsStructuredParser struct {
	regex *regexp.Regexp
}

func NewBracketsStructuredParser(regex *regexp.Regexp) *BracketsStructuredParser {
	return &BracketsStructuredParser{regex: regex}
}

func (p *BracketsStructuredParser) Parse(line string, lineCount int) (*entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("using the BracketsStructuredParser does not match with the format provided on the line %d of the log.", lineCount)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, matches[2])
	if err != nil {
		return nil, fmt.Errorf("using the BracketsStructuredParser an error occurred on the line %d of the log in the parsing time moment.", lineCount)
	}

	entry := &entity.LogEntry{
		Level:     matches[1],
		Timestamp: timestamp,
		IPAddress: &matches[3],
		Message:   matches[4],
	}

	return entry, nil
}
