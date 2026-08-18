package secrets

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type SecretsManager interface {
	FetchAndWriteSecrets(secretID string, envPath string) error
}

type AWSSecretsManager struct {
	client *secretsmanager.Client
}

func NewAWSSecretsManager(region string) (*AWSSecretsManager, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := secretsmanager.NewFromConfig(cfg)
	return &AWSSecretsManager{client: client}, nil
}

func (sm *AWSSecretsManager) FetchAndWriteSecrets(secretID string, envPath string) error {
	fmt.Printf("Fetching secrets from AWS Secrets Manager (ID: %s)...\n", secretID)

	input := &secretsmanager.GetSecretValueInput{
		SecretId:     &secretID,
		VersionStage: aws.String("AWSCURRENT"),
	}

	result, err := sm.client.GetSecretValue(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to get secret value: %w", err)
	}

	var secretString string
	if result.SecretString != nil {
		secretString = *result.SecretString
	} else {
		secretString = string(result.SecretBinary)
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(envPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for .env file: %w", err)
	}

	fmt.Printf("Writing secrets to .env file at %s...\n", envPath)
	if err := os.WriteFile(envPath, []byte(secretString), 0644); err != nil {
		return fmt.Errorf("failed to write .env file: %w", err)
	}

	fmt.Println("Secrets have been successfully written to .env file")
	return nil
}
