package exchanges

import "fmt"

func New(name string, ch chan<- string) (Exchange, error) {
	switch name {
	case "bybit":
		return &Bybit{Channel: ch}, nil
	default:
		return nil, fmt.Errorf("unknown exchange: %s", name)
	}
}
