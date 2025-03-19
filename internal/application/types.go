package application

import (
	"regexp"
	"github.com/etl_app_transform_service/internal/domain/entity"
)

type ParserPattern struct {
	Regex  *regexp.Regexp
	Parser entity.LogParser
}