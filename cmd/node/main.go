package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"grpcchat/internal/chat"

	"google.golang.org/grpc"
)

func main() {

	username := flag.String("username", "anonymous", "Your username")
	listen := flag.String("listen", ":50051", "Address to listen on")
	peer := flag.String("peer", "", "Peer address")

	flag.Parse()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	chat.RegisterChatServiceServer(grpcServer, &chat.Server{})

	go func() {
		log.Println("gRPC server listening on", *listen)

		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(time.Second)

	address := *peer
	if address == "" {
		address = "localhost:50051"
	}

	var (
		client *chat.Client
		stream grpc.BidiStreamingClient[chat.ChatMessage, chat.ChatMessage]
	)

	for {

		client, err = chat.NewClient(address)
		if err != nil {
			log.Println("Unable to create client:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		stream, err = client.OpenStream()
		if err != nil {
			log.Println("Peer unavailable, retrying in 2 seconds...")
			client.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("Connected to peer:", address)
		break
	}

	defer client.Close()

	go func() {
		for {
			reply, err := stream.Recv()
			if err != nil {
				log.Println("Receive error:", err)
				return
			}

			log.Printf("[%s] %s\n", reply.Username, reply.Message)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		text := scanner.Text()

		err := stream.Send(&chat.ChatMessage{
			Username:  *username,
			Message:   text,
			Timestamp: time.Now().Unix(),
		})

		if err != nil {
			log.Println("Send failed:", err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
}
