package chat

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestP2PMeshBroadcast(t *testing.T) {
	nodeA := NewNode("Sakthivasan", "127.0.0.1:50051")
	nodeB := NewNode("Ajay", "127.0.0.1:50052")
	nodeC := NewNode("Kathir", "127.0.0.1:50053")

	lisA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lisA.Close()

	lisB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lisB.Close()

	srvA := grpc.NewServer()
	RegisterChatServiceServer(srvA, nodeA)
	go srvA.Serve(lisA)
	defer srvA.Stop()

	srvB := grpc.NewServer()
	RegisterChatServiceServer(srvB, nodeB)
	go srvB.Serve(lisB)
	defer srvB.Stop()

	// Connect Node B (Ajay) -> Node A (Sakthivasan)
	nodeB.ConnectToPeer(lisA.Addr().String())
	// Connect Node C (Kathir) -> Node A (Sakthivasan)
	nodeC.ConnectToPeer(lisA.Addr().String())

	time.Sleep(300 * time.Millisecond)

	// 1. Sakthivasan (Node A) broadcasts a message
	msgA := &ChatMessage{
		Id:        "msg-a-1",
		Username:  "Sakthivasan",
		Message:   "Hello from Sakthivasan",
		Timestamp: time.Now().Unix(),
		Type:      MessageType_CHAT,
	}

	if !nodeA.Broadcast(msgA, "local-cli") {
		t.Fatalf("Expected Broadcast to succeed on node A")
	}

	time.Sleep(300 * time.Millisecond)

	nodeB.mu.RLock()
	_, seenB := nodeB.seenMsgs["msg-a-1"]
	nodeB.mu.RUnlock()

	nodeC.mu.RLock()
	_, seenC := nodeC.seenMsgs["msg-a-1"]
	nodeC.mu.RUnlock()

	if !seenB {
		t.Errorf("Node B (Ajay) did not receive Sakthivasan's message!")
	}
	if !seenC {
		t.Errorf("Node C (Kathir) did not receive Sakthivasan's message!")
	}

	// 2. Ajay (Node B) broadcasts a message
	msgB := &ChatMessage{
		Id:        "msg-b-1",
		Username:  "Ajay",
		Message:   "Hello from Ajay",
		Timestamp: time.Now().Unix(),
		Type:      MessageType_CHAT,
	}

	if !nodeB.Broadcast(msgB, "local-cli") {
		t.Fatalf("Expected Broadcast to succeed on node B")
	}

	time.Sleep(300 * time.Millisecond)

	nodeA.mu.RLock()
	_, seenAfromB := nodeA.seenMsgs["msg-b-1"]
	nodeA.mu.RUnlock()

	nodeC.mu.RLock()
	_, seenCfromB := nodeC.seenMsgs["msg-b-1"]
	nodeC.mu.RUnlock()

	if !seenAfromB {
		t.Errorf("Node A (Sakthivasan) did not receive Ajay's message!")
	}
	if !seenCfromB {
		t.Errorf("Node C (Kathir) did not receive Ajay's message forwarded via Sakthivasan!")
	}
}
