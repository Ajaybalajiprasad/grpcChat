package chat

import (
	"fmt"
	"io"

	"google.golang.org/grpc"
)

type Server struct {
	UnimplementedChatServiceServer
  clients []grpc.BidiStreamingServer[ChatMessage, ChatMessage]
}

func (s *Server) Chat(stream grpc.BidiStreamingServer[ChatMessage, ChatMessage]) error {

	fmt.Println("Client connected")
  s.clients = append(s.clients, stream)

	for {

		msg, err := stream.Recv()

		if err == io.EOF {
			fmt.Println("Client disconnected")
			return nil
		}

		if err != nil {
			return err
		}

		fmt.Printf("[%s] %s\n", msg.Username, msg.Message)

    for _, client := range s.clients {

      if client == stream {
        continue
      }
    	err := client.Send(&ChatMessage{
		    Username:  msg.Username,
		    Message:   msg.Message,
		    Timestamp: msg.Timestamp,
	  })

	  if err != nil {
		  fmt.Println("Send failed:", err)
	  }
  }

	}
}
