package unit_test

import (
	"os"
	"regexp"
	"testing"

	"github.com/etl_app_transform_service/internal"
	"github.com/etl_app_transform_service/internal/entity"
	"github.com/etl_app_transform_service/internal/parser"
)

func Test_LocalDynamicLogParser(t *testing.T) {
	reDefaultStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
	reJson := regexp.MustCompile(`^\{.*\}$`)
	reSimpleAlert := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`)
	reHttp := regexp.MustCompile(`^(\S+) - - \[(.+?)\] "(\w+) (.+?) HTTP\/\d\.\d" (\d+) (\d+|-) ".*?" "(.*?)"$`)
	reBracketsStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
	reLevelFirst := regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`)

	logFormatFactory := internal.NewLogParserFactory(map[*regexp.Regexp]entity.LogParser{
		reDefaultStructured:  parser.NewDefaultStructuredParser(reDefaultStructured),
		reJson:               parser.NewJsonLogParser(reJson),
		reSimpleAlert:        parser.NewSimpleAlertParser(reSimpleAlert),
		reHttp:               parser.NewHttpLogParser(reHttp),
		reBracketsStructured: parser.NewBracketsStructuredParser(reBracketsStructured),
		reLevelFirst:         parser.NewLevelFirstParser(reLevelFirst),
	})
	logParser := internal.NewLogParser(logFormatFactory)

	output, err := logParser.ParseLocalLogFile("./../mock/mock_server_log.txt")
	if err != nil {
		t.Fatalf("could not parse server log file: %s", err.Error())
	}

	_, err = os.ReadFile("./../mock/mock_server_log.txt")
	if err != nil {
		t.Fatalf("could not read the mock server log file: %s", err.Error())
	}

	if output == nil {
		t.Fatal("output could not be nil")
	}
}
