package parser_test

import (
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/parser"
)

func Test_BracketsStructuredParser(t *testing.T) {
	testCases := []string{
		`2025-03-09 14:04:18 [ERROR] [db-connection] Deadlock detected on table "users"`,
		`2025-03-09 03:34:30 [INFO] [auth-service] User 14408 logged in from IP 192.168.181.195`,
		`2025-03-09 01:33:47 [INFO] [auth-service] User 16323 logged in from IP 192.168.244.40`,
	}

	bracketsStructuredLogParser := parser.NewBracketsStructuredParser(regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[(.+?)\] (.+)$`))

	for idx, tc := range testCases {
		output, err := bracketsStructuredLogParser.Parse(tc, idx)
		if err != nil {
			t.Fatal(err)
		}
		if output == nil {
			t.Fatal("output can not to be nil")
		}
	}
}
