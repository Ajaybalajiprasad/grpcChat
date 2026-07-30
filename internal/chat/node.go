package chat

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MessageSender interface {
	Send(*ChatMessage) error
}

type Node struct {
	UnimplementedChatServiceServer
	Username string

	mu       sync.Mutex
	senders  map[string]MessageSender
	seenMsgs map[string]time.Time
}

func NewNode(username string) *Node {
	return &Node{
		Username: username,
		senders:  make(map[string]MessageSender),
		seenMsgs: make(map[string]time.Time),
	}
}

func (n *Node) AddSender(id string, s MessageSender) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.senders[id] = s
}

func (n *Node) RemoveSender(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.senders, id)
}

func (n *Node) Broadcast(msg *ChatMessage, sourceID string) bool {
	n.mu.Lock()

	// Periodic cleanup of old message IDs
	now := time.Now()
	if len(n.seenMsgs) > 1000 {
		for id, t := range n.seenMsgs {
			if now.Sub(t) > 5*time.Minute {
				delete(n.seenMsgs, id)
			}
		}
	}

	if msg.Id == "" {
		msg.Id = fmt.Sprintf("%s-%d-%d", msg.Username, msg.Timestamp, rand.Int63())
	}

	if _, seen := n.seenMsgs[msg.Id]; seen {
		n.mu.Unlock()
		return false
	}
	n.seenMsgs[msg.Id] = now

	type target struct {
		id string
		s  MessageSender
	}
	var targets []target
	for id, s := range n.senders {
		if id == sourceID {
			continue
		}
		targets = append(targets, target{id: id, s: s})
	}
	n.mu.Unlock()

	for _, t := range targets {
		go func(targetID string, s MessageSender) {
			if err := s.Send(msg); err != nil {
				n.RemoveSender(targetID)
			}
		}(t.id, t.s)
	}

	return true
}

// Chat handles incoming gRPC streaming requests from remote peers
func (n *Node) Chat(stream grpc.BidiStreamingServer[ChatMessage, ChatMessage]) error {
	streamID := fmt.Sprintf("in-%p", stream)
	n.AddSender(streamID, stream)
	defer func() {
		n.RemoveSender(streamID)
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if n.Broadcast(msg, streamID) {
			fmt.Printf("[%s] %s\n", msg.Username, msg.Message)
		}
	}
}

// ConnectToPeer establishes a gRPC client stream to a target peer address and manages reconnection
func (n *Node) ConnectToPeer(address string) {
	go func() {
		for {
			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			client := NewChatServiceClient(conn)
			stream, err := client.Chat(context.Background())
			if err != nil {
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			streamID := fmt.Sprintf("out-%s-%p", address, stream)
			log.Println("Connected to peer:", address)
			n.AddSender(streamID, stream)

			for {
				msg, err := stream.Recv()
				if err != nil {
					log.Printf("Peer %s disconnected\n", address)
					n.RemoveSender(streamID)
					stream.CloseSend()
					conn.Close()
					break
				}

				if n.Broadcast(msg, streamID) {
					fmt.Printf("[%s] %s\n", msg.Username, msg.Message)
				}
			}

			time.Sleep(2 * time.Second)
		}
	}()
}
