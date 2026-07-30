package chat

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client ChatServiceClient
}

func NewClient(address string) (*Client, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		client: NewChatServiceClient(conn),
	}, nil
}

func (c *Client) OpenStream() (grpc.BidiStreamingClient[ChatMessage, ChatMessage], error) {
	return c.client.Chat(context.Background())
}

func (c *Client) Close() error {
	return c.conn.Close()
}
