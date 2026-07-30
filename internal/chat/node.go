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

func NormalizeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	if strings.HasPrefix(addr, "localhost:") {
		return "127.0.0.1:" + strings.TrimPrefix(addr, "localhost:")
	}
	return addr
}

type Node struct {
	UnimplementedChatServiceServer
	Username   string
	ListenAddr string

	mu             sync.RWMutex
	senders        map[string]MessageSender
	peerMeta       map[string]PeerInfo
	knownAddrs     map[string]bool
	activeOutConns map[string]bool
	seenMsgs       map[string]time.Time
}

func NewNode(username string, listenAddr string) *Node {
	normListen := NormalizeAddr(listenAddr)
	n := &Node{
		Username:       username,
		ListenAddr:     normListen,
		senders:        make(map[string]MessageSender),
		peerMeta:       make(map[string]PeerInfo),
		knownAddrs:     make(map[string]bool),
		activeOutConns: make(map[string]bool),
		seenMsgs:       make(map[string]time.Time),
	}

	if normListen != "" {
		n.knownAddrs[normListen] = true
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
		norm := NormalizeAddr(meta.Address)
		n.knownAddrs[norm] = true
	}
}

func (n *Node) RemoveSender(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if meta, exists := n.peerMeta[id]; exists {
		if meta.Address != "" {
			delete(n.activeOutConns, NormalizeAddr(meta.Address))
		}
	}
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
		norm := NormalizeAddr(addr)
		if norm == "" || norm == n.ListenAddr {
			continue
		}

		n.mu.RLock()
		isKnown := n.knownAddrs[norm]
		isConnecting := n.activeOutConns[norm]
		n.mu.RUnlock()

		if !isKnown {
			n.mu.Lock()
			n.knownAddrs[norm] = true
			n.mu.Unlock()
		}

		if !isConnecting {
			log.Printf("[PEX] Discovered new peer: %s (auto-connecting...)\n", norm)
			n.ConnectToPeer(norm)
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
		Username:  "Connecting Peer...",
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
					norm := NormalizeAddr(msg.ListenAddr)
					meta.Address = norm
					n.knownAddrs[norm] = true
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
	normAddr := NormalizeAddr(address)
	if normAddr == "" || normAddr == n.ListenAddr {
		return // Do NOT connect to self
	}

	n.mu.Lock()
	if n.activeOutConns[normAddr] {
		n.mu.Unlock()
		return // Already connected or connecting
	}
	n.activeOutConns[normAddr] = true
	n.mu.Unlock()

	go func() {
		defer func() {
			n.mu.Lock()
			delete(n.activeOutConns, normAddr)
			n.mu.Unlock()
		}()

		for {
			conn, err := grpc.NewClient(
				normAddr,
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

			streamID := fmt.Sprintf("out-%s-%p", normAddr, stream)
			log.Println("Connected to peer:", normAddr)

			n.AddSender(streamID, stream, PeerInfo{
				ID:        streamID,
				Username:  "Peer (" + normAddr + ")",
				Address:   normAddr,
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
					log.Printf("Peer %s disconnected\n", normAddr)
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

	// Deduplicate entries by Username & Address for clean topology display
	type cleanEntry struct {
		Username  string
		Address   string
		Direction string
		Connected time.Time
	}
	seenKeys := make(map[string]bool)
	var entries []cleanEntry

	for _, meta := range n.peerMeta {
		if meta.Username == n.Username {
			continue // Skip self if self handshake received
		}
		key := fmt.Sprintf("%s-%s", meta.Username, meta.Address)
		if seenKeys[key] {
			continue
		}
		seenKeys[key] = true
		entries = append(entries, cleanEntry{
			Username:  meta.Username,
			Address:   meta.Address,
			Direction: meta.Direction,
			Connected: meta.Connected,
		})
	}

	fmt.Println()
	fmt.Println("==========================================================")
	fmt.Println("                 NETWORK TOPOLOGY GRAPH                   ")
	fmt.Println("==========================================================")
	fmt.Printf(" Local Node Username : %s\n", n.Username)
	fmt.Printf(" Listening Address   : %s\n", n.ListenAddr)
	fmt.Printf(" Total Discovered IP : %d\n", len(n.knownAddrs))
	fmt.Printf(" Active Remote Peers : %d\n", len(entries))
	fmt.Println("----------------------------------------------------------")

	if len(entries) == 0 {
		fmt.Println(" (No active connections - running as standalone node)")
	} else {
		total := len(entries)
		for i, entry := range entries {
			prefix := " ├──"
			if i == total-1 {
				prefix = " └──"
			}
			addrStr := entry.Address
			if addrStr == "" {
				addrStr = "remote"
			}
			fmt.Printf("%s [%-3s] %-15s (Address: %s, Connected: %s ago)\n",
				prefix,
				entry.Direction,
				entry.Username,
				addrStr,
				time.Since(entry.Connected).Round(time.Second),
			)
		}
	}
	fmt.Println("==========================================================")
	fmt.Println()
}
