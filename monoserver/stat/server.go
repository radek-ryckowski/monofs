// stat backend server for monofs
package stat

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/radek-ryckowski/monofs/proto"
	"go.uber.org/zap"
)

type Server struct {
	pb.UnimplementedMonofsStatServer
	log *zap.Logger
}

// New is a constructor for Server

func New() *Server {
	return &Server{}
}

// Stat is a RPC for stat
func (s *Server) Stat(ctx context.Context, in *pb.StatRequest) (*pb.StatResponse, error) {
	blockSize := uint32(4096)
	return &pb.StatResponse{
		Id:              in.Fs,
		BlockSize:       blockSize,
		Blocks:          1024 * 1024 * uint64(blockSize),
		BlocksFree:      1024 * 1024 * uint64(blockSize),
		BlocksAvailable: 1024 * 1024 * uint64(blockSize),
	}, nil
}

// start server on specific address
func (s *Server) Start(address, certDir string, log *zap.SugaredLogger) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
		return err
	}

	var grpcServer *grpc.Server
	testrun := os.Getenv("MONOFS_DEV_RUN")
	if len(testrun) > 0 {
		log.Infof("running insecure server reason: %s", testrun)
		grpcServer = grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	} else {
		if len(certDir) == 0 {
			return fmt.Errorf("certDir is not set")
		}

		caPem, err := os.ReadFile(fmt.Sprintf("%s/ca-cert.pem", certDir))
		if err != nil {
			return err
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPem) {
			return err
		}
		serverCertPath := fmt.Sprintf("%s/server-cert.pem", certDir)
		serverKeyPath := fmt.Sprintf("%s/server-key.pem", certDir)
		serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
		if err != nil {
			return err
		}
		config := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientCAs:    certPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}

		tlsCredential := credentials.NewTLS(config)
		grpcServer = grpc.NewServer(grpc.Creds(tlsCredential))
	}

	pb.RegisterMonofsStatServer(grpcServer, s)

	log.Infof("starting server on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
		return err
	}

	return nil
}
