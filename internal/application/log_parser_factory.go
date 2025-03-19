package application

import (
	"fmt"
	"github.com/etl_app_transform_service/internal/domain/entity"
)

type LogParserFactory struct {
	patterns []ParserPattern
}

func NewLogParserFactory(patterns []ParserPattern) *LogParserFactory {
	return &LogParserFactory{
		patterns: patterns,
	}
}

func (f *LogParserFactory) GetParser(line string) (entity.LogParser, error) {
	for _, p := range f.patterns {
		if p.Regex.MatchString(line) {
			return p.Parser, nil
		}
	}
	return nil, fmt.Errorf("could not find a parser for line %s", line)
}