package indexer

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/server/indexer"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func NewRunIndexerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run-indexer",
		Short: "Run the OpenFGA Indexer server",
		Long:  "Run the OpenFGA Indexer server.",
		Run:   run,
		Args:  cobra.NoArgs,
	}

	return cmd
}

func run(_ *cobra.Command, _ []string) {

	openfgaClientOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(":8081", openfgaClientOpts...)
	if err != nil {
		log.Fatalf("failed to connect to OpenFGA service: %v", err)
	}
	defer conn.Close()

	server := indexer.NewIndexerServerWithsOpts(
		indexer.WithOpenFGAClient(openfgav1.NewOpenFGAServiceClient(conn)),
	)

	serverOpts := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(serverOpts...)
	openfgav1.RegisterIndexerServiceServer(grpcServer, server)

	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("failed to start grpc listener: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				log.Fatalf("failed to start grpc server: %v", err)
			}

			log.Println("grpc server shut down..")
		}
	}()
	log.Println(fmt.Sprintf("grpc server listening on '%s'...", ":50053"))

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
	}

	// todo: graceful shutdown
}
