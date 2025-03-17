package parser_test

import (
	"log"
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal/application/parser"
)

func Test_JsonLogParser(t *testing.T) {
	testCases := []string{
		`{"timestamp": "2025-03-09T17:28:05.657131Z", "level": "ERROR", "service": "auth-service", "message": "Invalid JWT token", "user_id": 11672}`,
		`{"timestamp": "2025-03-09T06:27:24.657152Z", "level": "INFO", "service": "payment-service", "message": "Transaction processed", "transaction_id": "TXN53186", "amount": 496.85}`,
		`{"timestamp": "2025-03-09T18:42:21.657330Z", "level": "ERROR", "service": "auth-service", "message": "Invalid JWT token", "user_id": 18034}`,
	}

	jsonLogParser := parser.NewJsonLogParser(regexp.MustCompile(`^\{.*\}$`))
	for _, tc := range testCases {
		output, err := jsonLogParser.Parse(tc)
		if err != nil {
			t.Fatal(err)
		}
		log.Print(output)
	}
}