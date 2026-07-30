package chat

import (
	"context"
	"fmt"
	"io"
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

	MsgChan chan *ChatMessage
	LogChan chan string
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
		MsgChan:        make(chan *ChatMessage, 100),
		LogChan:        make(chan string, 100),
	}

	if normListen != "" {
		n.knownAddrs[normListen] = true
	}

	// Start background Peer Exchange (PEX) ticker
	go n.startPeerExchangeLoop()

	return n
}

func (n *Node) log(msg string) {
	select {
	case n.LogChan <- msg:
	default:
	}
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

			n.log(fmt.Sprintf("[PEX] Discovered new peer: %s (auto-connecting...)", norm))
			n.ConnectToPeer(norm)
		}

		if !isConnecting {
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
			select {
			case n.MsgChan <- msg:
			default:
			}
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
			n.log(fmt.Sprintf("Connected to peer: %s", normAddr))

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
					n.log(fmt.Sprintf("Peer %s disconnected", normAddr))
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
					select {
					case n.MsgChan <- msg:
					default:
					}
				}
			}

			time.Sleep(3 * time.Second)
		}
	}()
}

// GetTopologyString returns a string representation of the network mapping
func (n *Node) GetTopologyString() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("\n==========================================================\n")
	sb.WriteString("                 NETWORK TOPOLOGY GRAPH                   \n")
	sb.WriteString("==========================================================\n")
	sb.WriteString(fmt.Sprintf(" Local Node Username : %s\n", n.Username))
	sb.WriteString(fmt.Sprintf(" Listening Address   : %s\n", n.ListenAddr))
	sb.WriteString(fmt.Sprintf(" Total Discovered IP : %d\n", len(n.knownAddrs)))
	sb.WriteString(fmt.Sprintf(" Active Connections  : %d\n", len(n.peerMeta)))
	sb.WriteString("----------------------------------------------------------\n")

	if len(n.peerMeta) == 0 {
		sb.WriteString(" (No active connections - running as standalone node)\n")
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
			sb.WriteString(fmt.Sprintf("%s [%-3s] %-15s (Address: %s, Connected: %s ago)\n",
				prefix,
				meta.Direction,
				meta.Username,
				addrStr,
				time.Since(meta.Connected).Round(time.Second),
			))
		}
	}
	sb.WriteString("==========================================================\n\n")
	return sb.String()
}
