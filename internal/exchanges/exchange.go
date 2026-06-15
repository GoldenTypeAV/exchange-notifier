package exchanges

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"
)

type Exchange interface {
	CheckCredentials() error
	ConnectPublicWS(ctx context.Context) (*websocket.Conn, error)
	ConnectPrivateWS(ctx context.Context) (*websocket.Conn, error)

	SubscribePrivate(
		ctx context.Context,
		conn *websocket.Conn,
		topics PrivateTopics,
	) error

	HandleMessage(msg json.RawMessage)
}

type PrivateTopics struct {
	Orders     bool
	Positions  bool
	Wallet     bool
	Executions bool
}
