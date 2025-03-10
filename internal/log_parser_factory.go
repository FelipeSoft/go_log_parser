package internal

import (
	"regexp"

	"github.com/etl_app_transform_service/internal/entity"
)

type LogParserFactory struct {
	parsers map[*regexp.Regexp]entity.LogParser
}

func NewLogParserFactory(parsers map[*regexp.Regexp]entity.LogParser) *LogParserFactory {
	return &LogParserFactory{
		parsers: parsers,
	}
}

func (f *LogParserFactory) GetParser(line string) entity.LogParser {
	for regex, parser := range f.parsers {
		if regex.MatchString(line) {
			return parser
		}
	}
	return nil
}
