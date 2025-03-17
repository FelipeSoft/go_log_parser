package entity

type LogParser interface {
	Parse(line string) (LogEntry, error)
}

type TransformRepository interface {
	Transform(entry []LogEntry) error
}
