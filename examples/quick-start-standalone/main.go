package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/deventlab/d-engine/proto/client"
	error_pb "github.com/deventlab/d-engine/proto/error"
)

func main() {
	// Start with any node
	addresses := []string{"127.0.0.1:9081", "127.0.0.1:9082", "127.0.0.1:9083"}
	currentAddr := addresses[0]

	var client pb.RaftClientServiceClient
	var conn *grpc.ClientConn
	var err error

	// Try to write, follow leader hints if needed
	maxRedirects := 3
	for i := 0; i < maxRedirects; i++ {
		conn, err = grpc.NewClient(currentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to %s: %v", currentAddr, err)
		}
		client = pb.NewRaftClientServiceClient(conn)

		// Try a write
		writeResp, err := client.HandleClientWrite(context.Background(), &pb.ClientWriteRequest{
			Commands: []*pb.WriteCommand{{
				Operation: &pb.WriteCommand_Insert_{
					Insert: &pb.WriteCommand_Insert{
						Key:   []byte("hello"),
						Value: []byte("world"),
					},
				},
			}},
		})

		if err != nil {
			log.Fatalf("Write RPC failed: %v", err)
		}

		if writeResp.Error == error_pb.ErrorCode_SUCCESS {
			fmt.Printf("Write success (leader at %s)\n", currentAddr)
			break
		}

		// Check if we got NOT_LEADER error with leader hint
		if writeResp.Error == error_pb.ErrorCode_NOT_LEADER && writeResp.Metadata != nil {
			leaderAddr := writeResp.Metadata.LeaderAddress
			if leaderAddr != nil && *leaderAddr != "" {
				fmt.Printf("Not leader, redirecting to %s\n", *leaderAddr)
				conn.Close()
				currentAddr = *leaderAddr
				continue
			}
		}

		log.Fatalf("Write failed: error=%v, no leader hint available", writeResp.Error)
	}
	defer conn.Close()

	// Read with LinearizableRead (guarantees reading what we just wrote)
	policy := pb.ReadConsistencyPolicy_READ_CONSISTENCY_POLICY_LINEARIZABLE_READ
	resp, err := client.HandleClientRead(context.Background(), &pb.ClientReadRequest{
		Keys:              [][]byte{[]byte("hello")},
		ConsistencyPolicy: &policy,
	})
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}

	if len(resp.GetReadData().GetResults()) == 0 {
		log.Fatal("Key not found")
	}

	fmt.Printf("Value: %s\n", resp.GetReadData().Results[0].Value)
}
