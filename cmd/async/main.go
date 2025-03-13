package main

import (
	"fmt"
	"log"
	"regexp"

	"github.com/etl_app_transform_service/internal"
	"github.com/etl_app_transform_service/internal/entity"
	"github.com/etl_app_transform_service/internal/parser"
	"github.com/joho/godotenv"
)

func main() {
	// workers := 5

	err := godotenv.Load("./../../.env")
	if err != nil {
		log.Fatal("could not load the enviroment variables file")
	}

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

	output, err := logParser.ParseLocalLogFile("C:/Users/felip/Filipinho/etl_app/transform_service/assets/server_log.txt")
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println(output)

	// for w := range workers {
	// 	go func(worker int) {

	// 	}(w)
	// }
}
