package entity

type LogParser interface {
	Parse(line string, lineCount int) (*LogEntry, error)
}
