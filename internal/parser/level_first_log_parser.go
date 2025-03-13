package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/entity"
)

type LevelFirstLogParser struct {
	regex *regexp.Regexp
}

func NewLevelFirstParser(regex *regexp.Regexp) *LevelFirstLogParser {
	return &LevelFirstLogParser{regex: regex}
}

func (p *LevelFirstLogParser) Parse(line string, lineCount int) (*entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("using the LevelFirstLogParser does not match with the format provided on the line %d of the log", lineCount)
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", matches[2])
	if err != nil {
		return nil, fmt.Errorf("using the LevelFirstLogParser an error occurred on the line %d of the log in the parsing time moment", lineCount)
	}

	entry := &entity.LogEntry{
		Timestamp:  timestamp,
		Level:      matches[1],
		Service:    matches[3],
		Message:    matches[4],
	}

	return entry, nil
}
