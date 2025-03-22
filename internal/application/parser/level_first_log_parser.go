package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type LevelFirstLogParser struct {
	regex *regexp.Regexp
}

func NewLevelFirstParser(regex *regexp.Regexp) entity.LogParser {
	return &LevelFirstLogParser{regex: regex}
}

func (p *LevelFirstLogParser) PreservesRaw() bool {
    return false
}

func (p *LevelFirstLogParser) Parse(line string) (entity.LogEntry, error) {
	matches := p.regex.FindStringSubmatch(line)
	if matches == nil {
		return entity.LogEntry{}, fmt.Errorf("using the LevelFirstLogParser does not match with the format provided on the line %s of the log", line)
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", matches[2])
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("using the LevelFirstLogParser an error occurred on the line %s of the log in the parsing time moment", line)
	}

	entry := entity.LogEntry{
		Timestamp:  timestamp,
		Level:      matches[1],
		Service:    matches[3],
		Message:    matches[4],
	}

	return entry, nil
}
