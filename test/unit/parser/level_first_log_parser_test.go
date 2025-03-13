package parser_test

import (
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/parser"
)

func Test_LevelFirstLogParser(t *testing.T) {
	testCases := []string{
		`ERROR 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
		`WARN 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
		`INFO 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
	}

	levelFirstParser := parser.NewLevelFirstParser(regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`))
	for idx, tc := range testCases {
		output, err := levelFirstParser.Parse(tc, idx + 1)
		if err != nil {
			t.Fatal(err.Error())
		}
		if output == nil {
			t.Fatal("output can not to be nil")
		}
	}
}
