package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type HttpLogParser struct {
	regex *regexp.Regexp
}

func NewHttpLogParser(regex *regexp.Regexp) *HttpLogParser {
	return &HttpLogParser{regex: regex}
}

func (p *HttpLogParser) PreservesRaw() bool {
    return false
}

func (p *HttpLogParser) Parse(line string) (entity.LogEntry, error) {
    matches := p.regex.FindStringSubmatch(line)
    if len(matches) != 9 {
        return entity.LogEntry{}, fmt.Errorf("invalid HTTP log error")
    }

    timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", matches[2])
    if err != nil {
        return entity.LogEntry{}, err
    }

    statusCode, _ := strconv.Atoi(matches[5])
    responseSize, _ := strconv.Atoi(matches[6])

    return entity.LogEntry{
        Timestamp:  timestamp,
        Level:      "INFO",
        Service:    "api-gateway",
        Message:    fmt.Sprintf("%s %s", matches[3], matches[4]),
        IPAddress:  matches[1],
        Method:     matches[3],
        Path:       matches[4],
        StatusCode: statusCode,
        UserAgent:  matches[8],
        Metadata: map[string]interface{}{
            "response_size": responseSize,
        },
    }, nil
}
