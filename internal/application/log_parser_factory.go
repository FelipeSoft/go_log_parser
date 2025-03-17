package application

import (
	"fmt"
	"regexp"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type LogParserFactory struct {
	parsers map[*regexp.Regexp]entity.LogParser
}

func NewLogParserFactory(parsers map[*regexp.Regexp]entity.LogParser) *LogParserFactory {
	return &LogParserFactory{
		parsers: parsers,
	}
}

func (f *LogParserFactory) GetParser(line string) (entity.LogParser, error) {
	for regex, parser := range f.parsers {
		if regex.MatchString(line) {
			return parser, nil
		}
	}
	return nil, fmt.Errorf("could not find a parser for line %s", line)
}
