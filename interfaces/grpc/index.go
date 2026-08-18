package grpc

import (
	"bitbucket.org/zapspace/zap-go-server/interfaces/grpc/handlers"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	pb "bitbucket.org/zapspace/zap-go-server/protoBuilds/wallet"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	"google.golang.org/grpc"
)

// NewImportHandler creates a new gRPC import handler
func NewImportHandler(
	accountUseCase *usecases.AccountUseCase,
	eventService *services.EventService,
	supportedCurrencyRepo repositories.SupportedCurrencyRepository,
	logger logger.Logger,
) *handlers.GrpcImportHandler {
	return handlers.NewGrpcImportHandler(accountUseCase, eventService, supportedCurrencyRepo, logger)
}

// RegisterImportServiceServer registers the ImportService server with the gRPC server
// Now using the auto-generated protobuf registration function
func RegisterImportServiceServer(s *grpc.Server, srv interface{}) {
	if s == nil {
		return // Prevent nil pointer dereference
	}

	importHandler, ok := srv.(*handlers.GrpcImportHandler)
	if !ok {
		return // Wrong type, just return silently rather than panic
	}

	// Register using the generated code
	pb.RegisterAccountImportServiceServer(s, importHandler)
}
