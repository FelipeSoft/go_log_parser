package parser_test

import (
	"log"
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/application/parser"
)

func Test_StructuredLogParser(t *testing.T) {
	testCases := []string{
		`2025-03-09 16:39:29 [INFO] [auth-service] User 18951 logged in from IP 192.168.81.149`,
		`2025-03-09 01:06:37 [ERROR] [db-connection] Deadlock detected on table "payments"`,
		`2025-03-09 10:59:19 [WARN] [db-connection] Deadlock detected on table "sessions"`,
	}

	structuredLogParser := parser.NewDefaultStructuredParser(regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`))
	for _, tc := range testCases {
		output, err := structuredLogParser.Parse(tc)
		if err != nil {
			t.Fatal(err)
		}
		log.Print(output)
	}
}