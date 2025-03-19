package parser_test

import (
	"log"
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/application/parser"
)

func Test_HttpLogParser(t *testing.T) {
	testCases := []string{
		`192.168.228.118 - - [09/Mar/2025:10:47:18 +0000] "POST /api/v1/register HTTP/1.1" 201 512 "-" "Mozilla/5.0 (Windows NT 10.0; Win64)"`,
		`192.168.40.245 - - [09/Mar/2025:20:14:00 +0000] "GET /api/v1/reports HTTP/1.1" 200 2048 "-" "Mozilla/5.0 (Linux; Android 11)"`,
		`192.168.239.48 - - [09/Mar/2025:15:03:29 +0000] "DELETE /api/v1/reports/341 HTTP/1.1" 200 2048 "-" "Mozilla/5.0 (Linux; Android 11)"`,
	}

	reHttp := regexp.MustCompile(`^(\S+) - - \[(.*?)\] "(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD) (.+?) HTTP/\d\.\d" (\d{3}) (\d+) "(.*?)" "(.*?)"$`)
	httpLogParser := parser.NewHttpLogParser(reHttp)

	for _, tc := range testCases {
		output, err := httpLogParser.Parse(tc)
		if err != nil {
			t.Fatalf("Falha ao analisar a linha: %v", err)
		}
		log.Print(output)
	}
}
