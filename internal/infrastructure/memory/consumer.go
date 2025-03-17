package memory

type InMemoryConsumer struct {
	Ch <-chan string
}

func NewInMemoryConsumer(ch <-chan string) *InMemoryConsumer {
	return &InMemoryConsumer{Ch: ch}
}

func (c *InMemoryConsumer) Messages() <-chan string {
	return c.Ch
}

func (c *InMemoryConsumer) Close() {}