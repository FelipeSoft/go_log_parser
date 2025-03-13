package internal

import (
	"bufio"
	"fmt"
	"os"

	"github.com/etl_app_transform_service/internal/entity"
)

type LocalLogParser struct {
	logParserFactory *LogParserFactory
}

func NewLogParser(logParserFactory *LogParserFactory) *LocalLogParser {
	return &LocalLogParser{
		logParserFactory: logParserFactory,
	}
}

func (lp *LocalLogParser) ParseLocalLogFile(filepath string) ([]*entity.LogEntry, error) {
	var output []*entity.LogEntry

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 1

	for scanner.Scan() {
		line := scanner.Text()
		parser := lp.logParserFactory.GetParser(line)
		logEntry, err := parser.Parse(line, lineCount)
		if err != nil {
			return nil, fmt.Errorf("could not parse the log line: %s; error found on line: %s", err.Error(), line)
		}
		output = append(output, logEntry)
		lineCount++
	}

	return output, nil
}
