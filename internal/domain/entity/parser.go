package entity

type LogParser interface {
	Parse(line string) (LogEntry, error)
	PreservesRaw() bool
}

type TransformRepository interface {
	Transform(entry []LogEntry) error
}
