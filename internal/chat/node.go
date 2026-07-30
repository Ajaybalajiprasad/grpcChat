package chat

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerInfo struct {
	ID        string
	Username  string
	Address   string
	Direction string // "IN" or "OUT"
	Connected time.Time
}

type MessageSender interface {
	Send(*ChatMessage) error
}

type Node struct {
	UnimplementedChatServiceServer
	Username   string
	ListenAddr string

	mu         sync.RWMutex
	senders    map[string]MessageSender
	peerMeta   map[string]PeerInfo
	knownAddrs map[string]bool
	seenMsgs   map[string]time.Time
}

func NewNode(username string, listenAddr string) *Node {
	n := &Node{
		Username:   username,
		ListenAddr: listenAddr,
		senders:    make(map[string]MessageSender),
		peerMeta:   make(map[string]PeerInfo),
		knownAddrs: make(map[string]bool),
		seenMsgs:   make(map[string]time.Time),
	}

	if listenAddr != "" {
		n.knownAddrs[listenAddr] = true
	}

	// Start background Peer Exchange (PEX) ticker
	go n.startPeerExchangeLoop()

	return n
}

func (n *Node) AddSender(id string, s MessageSender, meta PeerInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.senders[id] = s
	n.peerMeta[id] = meta
	if meta.Address != "" {
		n.knownAddrs[meta.Address] = true
	}
}

func (n *Node) RemoveSender(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.senders, id)
	delete(n.peerMeta, id)
}

func (n *Node) GetKnownPeersList() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var peers []string
	for addr := range n.knownAddrs {
		peers = append(peers, addr)
	}
	return peers
}

func (n *Node) startPeerExchangeLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		peers := n.GetKnownPeersList()
		if len(peers) == 0 {
			continue
		}

		pexMsg := &ChatMessage{
			Id:         fmt.Sprintf("pex-%s-%d", n.Username, time.Now().UnixNano()),
			Username:   n.Username,
			Timestamp:  time.Now().Unix(),
			Type:       MessageType_PEER_EXCHANGE,
			KnownPeers: peers,
			ListenAddr: n.ListenAddr,
		}

		n.Broadcast(pexMsg, "local-pex")
	}
}

func (n *Node) handlePeerExchange(msg *ChatMessage) {
	for _, addr := range msg.KnownPeers {
		addr = strings.TrimSpace(addr)
		if addr == "" || addr == n.ListenAddr {
			continue
		}

		n.mu.RLock()
		alreadyKnown := n.knownAddrs[addr]
		n.mu.RUnlock()

		if !alreadyKnown {
			n.mu.Lock()
			n.knownAddrs[addr] = true
			n.mu.Unlock()

			log.Printf("[PEX] Discovered new peer: %s (auto-connecting...)\n", addr)
			n.ConnectToPeer(addr)
		}
	}
}

func (n *Node) Broadcast(msg *ChatMessage, sourceID string) bool {
	n.mu.Lock()

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

func (n *Node) Chat(stream grpc.BidiStreamingServer[ChatMessage, ChatMessage]) error {
	streamID := fmt.Sprintf("in-%p", stream)
	n.AddSender(streamID, stream, PeerInfo{
		ID:        streamID,
		Username:  "Unknown Peer",
		Address:   "",
		Direction: "IN",
		Connected: time.Now(),
	})
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

		if msg.Username != "" {
			n.mu.Lock()
			if meta, exists := n.peerMeta[streamID]; exists {
				meta.Username = msg.Username
				if msg.ListenAddr != "" {
					meta.Address = msg.ListenAddr
					n.knownAddrs[msg.ListenAddr] = true
				}
				n.peerMeta[streamID] = meta
			}
			n.mu.Unlock()
		}

		if msg.Type == MessageType_PEER_EXCHANGE {
			n.handlePeerExchange(msg)
			n.Broadcast(msg, streamID)
			continue
		}

		if n.Broadcast(msg, streamID) {
			fmt.Printf("[%s] %s\n", msg.Username, msg.Message)
		}
	}
}

func (n *Node) ConnectToPeer(address string) {
	go func() {
		for {
			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				time.Sleep(3 * time.Second)
				continue
			}

			client := NewChatServiceClient(conn)
			stream, err := client.Chat(context.Background())
			if err != nil {
				conn.Close()
				time.Sleep(3 * time.Second)
				continue
			}

			streamID := fmt.Sprintf("out-%s-%p", address, stream)
			log.Println("Connected to peer:", address)

			n.AddSender(streamID, stream, PeerInfo{
				ID:        streamID,
				Username:  "Peer (" + address + ")",
				Address:   address,
				Direction: "OUT",
				Connected: time.Now(),
			})

			stream.Send(&ChatMessage{
				Id:         fmt.Sprintf("handshake-%s", n.Username),
				Username:   n.Username,
				Timestamp:  time.Now().Unix(),
				Type:       MessageType_PEER_EXCHANGE,
				KnownPeers: n.GetKnownPeersList(),
				ListenAddr: n.ListenAddr,
			})

			for {
				msg, err := stream.Recv()
				if err != nil {
					log.Printf("Peer %s disconnected\n", address)
					n.RemoveSender(streamID)
					stream.CloseSend()
					conn.Close()
					break
				}

				if msg.Username != "" {
					n.mu.Lock()
					if meta, exists := n.peerMeta[streamID]; exists {
						meta.Username = msg.Username
						n.peerMeta[streamID] = meta
					}
					n.mu.Unlock()
				}

				if msg.Type == MessageType_PEER_EXCHANGE {
					n.handlePeerExchange(msg)
					n.Broadcast(msg, streamID)
					continue
				}

				if n.Broadcast(msg, streamID) {
					fmt.Printf("[%s] %s\n", msg.Username, msg.Message)
				}
			}

			time.Sleep(3 * time.Second)
		}
	}()
}

// PrintTopology renders a live ASCII map of the node's P2P network connections
func (n *Node) PrintTopology() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println()
	fmt.Println("==========================================================")
	fmt.Println("                 NETWORK TOPOLOGY GRAPH                   ")
	fmt.Println("==========================================================")
	fmt.Printf(" Local Node Username : %s\n", n.Username)
	fmt.Printf(" Listening Address   : %s\n", n.ListenAddr)
	fmt.Printf(" Total Discovered IP : %d\n", len(n.knownAddrs))
	fmt.Printf(" Active Connections  : %d\n", len(n.peerMeta))
	fmt.Println("----------------------------------------------------------")

	if len(n.peerMeta) == 0 {
		fmt.Println(" (No active connections - running as standalone node)")
	} else {
		i := 0
		total := len(n.peerMeta)
		for _, meta := range n.peerMeta {
			i++
			prefix := " ├──"
			if i == total {
				prefix = " └──"
			}
			addrStr := meta.Address
			if addrStr == "" {
				addrStr = "remote"
			}
			fmt.Printf("%s [%-3s] %-15s (Address: %s, Connected: %s ago)\n",
				prefix,
				meta.Direction,
				meta.Username,
				addrStr,
				time.Since(meta.Connected).Round(time.Second),
			)
		}
	}
	fmt.Println("==========================================================")
	fmt.Println()
}
