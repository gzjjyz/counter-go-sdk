package counter

type Producer interface {
	Add(topic string, buf []byte) error
	Close() error
}
