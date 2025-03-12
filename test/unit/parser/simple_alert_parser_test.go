package parser_test

import (
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/parser"
)

func Test_SimpleAlertParserTest(t *testing.T) {
	testCases := []string{
		"2025-03-09 16:50:43 WARN storage-service Backup storage at 95% capacity",
		"2025-03-09 18:50:58 WARN storage-service Backup storage at 94% capacity",
		"2025-03-09 15:21:49 WARN storage-service Backup storage at 98% capacity",
	}

	simpleAlertParser := parser.NewSimpleAlertParser(regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`))
	for idx, tc := range testCases {
		output, err := simpleAlertParser.Parse(tc, idx + 1)
		if err != nil {
			t.Fatal(err)
		}
		if output == nil {
			t.Fatal("output can not be nil")
		}
	}
}
