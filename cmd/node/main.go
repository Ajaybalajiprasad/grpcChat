package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"grpcchat/internal/chat"

	"google.golang.org/grpc"
)

func main() {
	username := flag.String("username", "anonymous", "Your username")
	listen := flag.String("listen", ":50051", "Address to listen on")
	peerStr := flag.String("peer", "", "Peer address or comma-separated addresses (e.g. 10.42.0.250:50051)")

	flag.Parse()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	node := chat.NewNode(*username)

	grpcServer := grpc.NewServer()
	chat.RegisterChatServiceServer(grpcServer, node)

	go func() {
		log.Println("gRPC server listening on", *listen)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	if *peerStr != "" {
		peers := strings.Split(*peerStr, ",")
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				node.ConnectToPeer(peer)
			}
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			break
		}

		text := scanner.Text()
		if strings.TrimSpace(text) == "" {
			continue
		}

		msg := &chat.ChatMessage{
			Id:        fmt.Sprintf("%s-%d-%d", *username, time.Now().UnixNano(), rand.Int63()),
			Username:  *username,
			Message:   text,
			Timestamp: time.Now().Unix(),
		}

		node.Broadcast(msg, "local-cli")
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
}
