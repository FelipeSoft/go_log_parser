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

func (p *BracketsStructuredParser) PreservesRaw() bool {
    return false
}

func (p *BracketsStructuredParser) Parse(line string) (entity.LogEntry, error) {
    matches := p.regex.FindStringSubmatch(line)
    if len(matches) != 5 {
        return entity.LogEntry{}, fmt.Errorf("invalid format")
    }

    timestamp, err := time.Parse("2006-01-02 15:04:05", matches[1])
    if err != nil {
        return entity.LogEntry{}, err
    }

    return entity.LogEntry{
        Timestamp: timestamp,
        Level:     matches[2],
        Service:   matches[3],
        Message:   matches[4],
    }, nil
}
