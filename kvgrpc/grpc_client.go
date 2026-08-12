package kvgrpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client KVServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("gRPC dial failed: %w", err)
	}
	return &Client{
		conn:   conn,
		client: NewKVServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Put(key, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.client.Put(ctx, &PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("server returned failure")
	}
	return nil
}

func (c *Client) Get(key []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.client.Get(ctx, &GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("key not found")
	}
	return resp.Value, nil
}

func (c *Client) Delete(key []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.client.Delete(ctx, &DeleteRequest{Key: key})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("server returned failure")
	}
	return nil
}
