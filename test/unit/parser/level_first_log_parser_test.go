package parser_test

import (
	"log"
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/application/parser"
)

func Test_LevelFirstLogParser(t *testing.T) {
	testCases := []string{
		`ERROR 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
		`WARN 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
		`INFO 2025-03-09 11:50:41 api-gateway Internal Server Error on /api/v1/healthcheck`,
	}

	levelFirstParser := parser.NewLevelFirstParser(regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`))
	for _, tc := range testCases {
		output, err := levelFirstParser.Parse(tc)
		if err != nil {
			t.Fatal(err.Error())
		}
		log.Print(output)
	}
}
