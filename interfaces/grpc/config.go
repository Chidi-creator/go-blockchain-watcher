package grpc

import (
	"fmt"
	"net"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// GrpcServer represents the gRPC server
type GrpcServer struct {
	server   *grpc.Server
	listener net.Listener
	logger   logger.Logger
	port     int
}

// GrpcServerOptions contains options for configuring the gRPC server
type GrpcServerOptions struct {
	Port             int
	TLSCertFile      string
	TLSKeyFile       string
	EnableTLS        bool
	EnableReflection bool
}

// NewGrpcServer creates a new gRPC server
func NewGrpcServer(logger logger.Logger, options GrpcServerOptions) (*GrpcServer, error) {
	// Setup listener
	addr := fmt.Sprintf(":%d", options.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Configure keepalive parameters
	keepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     time.Minute * 5,
		MaxConnectionAge:      time.Hour * 1,
		MaxConnectionAgeGrace: time.Minute * 5,
		Time:                  time.Minute * 1,
		Timeout:               time.Second * 20,
	}

	// Server options
	var opts []grpc.ServerOption
	opts = append(opts, grpc.KeepaliveParams(keepaliveParams))

	// Add TLS if enabled
	if options.EnableTLS {
		creds, err := credentials.NewServerTLSFromFile(options.TLSCertFile, options.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Create gRPC server
	server := grpc.NewServer(opts...)

	// Enable reflection if needed (useful for development and debugging)
	if options.EnableReflection {
		reflection.Register(server)
	}

	return &GrpcServer{
		server:   server,
		listener: listener,
		logger:   logger,
		port:     options.Port,
	}, nil
}

// RegisterService registers service implementations with the gRPC server
func (s *GrpcServer) RegisterService(registerFn func(server *grpc.Server)) {
	registerFn(s.server)
}

// Start starts the gRPC server
func (s *GrpcServer) Start() error {
	s.logger.Info("Starting gRPC server", "port", s.port)
	return s.server.Serve(s.listener)
}

// Stop gracefully stops the gRPC server
func (s *GrpcServer) Stop() {
	s.logger.Info("Stopping gRPC server")
	s.server.GracefulStop()
}

// GetPort returns the port the server is listening on
func (s *GrpcServer) GetPort() int {
	return s.port
}
