package entity

type MessageConsumer interface {
	Messages() <-chan string
	Close()
}
