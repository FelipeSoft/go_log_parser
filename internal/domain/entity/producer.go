package entity

type MessageProducer interface {
    Send(message string) error
    Close()
}