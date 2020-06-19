package godynamodb

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Test is a DynamoDB integration test that runs using a local Docker container.
type Test struct {
	// Table is the name of the table to create.
	Table string

	inCircleCI bool
	endpoint   string
}

// TestMain runs the tests.
//
// Using this function we can ensure the underlying resources are freed (check https://github.com/golang/go/issues/23404)
func (t *Test) TestMain(m *testing.M, setupDB func(svc *dynamodb.Client) error) int {
	t.inCircleCI = os.Getenv("CIRCLECI") == "true"
	switch t.inCircleCI {
	case false:
		// run dynamodb container
		port, err := getFreePort()
		if err != nil {
			log.Printf("couldn't get a free port: %s", err)
			return 1
		}
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			Image:       "amazon/dynamodb-local",
			Entrypoint:  []string{"java", "-jar", "DynamoDBLocal.jar", "-inMemory", "-port", strconv.Itoa(port)},
			WaitingFor:  wait.NewLogStrategy("CorsParams"),
			AutoRemove:  true,
			NetworkMode: "host",
		}
		dynamoDBLocal, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			log.Printf("couldn't start container: %s", err)
			return 1
		}
		defer dynamoDBLocal.Terminate(ctx)
		t.endpoint = "localhost:" + strconv.Itoa(port)
		log.Printf("endpoint: %s", t.endpoint)
	default:
		// CircleCI spins up the container for us
		t.endpoint = "localhost:8000"
		log.Printf("endpoint: %s", t.endpoint)
	}

	// setup database
	svc, err := t.GetClient()
	if err != nil {
		log.Printf("could not create dynamodb client: %s", err)
		return 1
	}
	err = setupDB(svc)
	if err != nil {
		log.Printf("could not setup dynamodb: %s", err)
		return 1
	}

	// run tests
	return m.Run()
}

// GetClient returns a DynamoDB Client configured.
func (t *Test) GetClient() (*dynamodb.Client, error) {
	cfg, err := external.LoadDefaultAWSConfig(external.WithCredentialsProvider{
		CredentialsProvider: aws.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "KEY",
				SecretAccessKey: "SECRET",
				SessionToken:    "SESSION",
				Source:          "fake credentials",
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}
	cfg.Region = "us-east-1"
	cfg.EndpointResolver = aws.ResolveWithEndpointURL("http://" + t.endpoint)
	cfg.HTTPClient = &http.Client{
		Timeout: 3 * time.Second,
	}
	cfg.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
		options.MaxAttempts = 1
	})
	return dynamodb.New(cfg), nil
}

func (t *Test) waitForDynamoDB() error {
	svc, err := t.GetClient()
	if err != nil {
		return err
	}
	request := svc.DescribeTableRequest(&dynamodb.DescribeTableInput{
		TableName: aws.String(t.Table),
	})
	_, err = request.Send(context.Background())
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// ignore only an ResourceNotFoundException error
			if awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return nil
			}
		}
		return err
	}
	return nil
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp4", ":0")
	if err != nil {
		return 0, fmt.Errorf("couldn't get a free tcp4 port: %s", err)
	}
	log.Printf("%s", l.Addr())
	sep := strings.LastIndex(l.Addr().String(), ":")
	if sep == -1 {
		return 0, fmt.Errorf("invalid address, ':' separator not found: %s", l.Addr())
	}
	port := l.Addr().String()[sep+1:]
	err = l.Close()
	if err != nil {
		return 0, fmt.Errorf("error closing listener: %s", err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return 0, fmt.Errorf("invalid port %s: %s", port, err)
	}
	return n, nil
}
