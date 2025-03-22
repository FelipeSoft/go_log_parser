package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type DeadlockParser struct {
	pattern *regexp.Regexp
}

func NewDeadlockParser(re *regexp.Regexp) entity.LogParser {
	return &DeadlockParser{pattern: re}
}

func (p *DeadlockParser) PreservesRaw() bool {
    return false
}

func (p *DeadlockParser) Parse(line string) (entity.LogEntry, error) {
	matches := p.pattern.FindStringSubmatch(line)
	if len(matches) != 4 {
		return entity.LogEntry{}, fmt.Errorf("invalid deadlock format")
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", matches[1])
	if err != nil {
		return entity.LogEntry{}, err
	}

	return entity.LogEntry{
		Timestamp: timestamp,
		Level:     "ERROR",
		Service:   matches[2],
		Message:   fmt.Sprintf(`Deadlock detected on table "%s"`, matches[3]),
	}, nil
}
