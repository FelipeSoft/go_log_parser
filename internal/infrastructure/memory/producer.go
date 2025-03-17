package memory

type InMemoryProducer struct {
    Ch chan<- string
}

func NewInMemoryProducer(ch chan<- string) *InMemoryProducer {
    return &InMemoryProducer{Ch: ch}
}

func (p *InMemoryProducer) Send(message string) error {
    p.Ch <- message
    return nil
}

func (p *InMemoryProducer) Close() {}