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
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Test is a DynamoDB integration test that runs using a local Docker container.
type Test struct {
	inCircleCI bool
	endpoint   string
}

// TestMain runs the tests. First, creates a container using the image amazon/dynamodb-local, with "host" as network
// and listening to a random port. Then, calls setupDB to initialize the database and finally calls m.Run() to execute
// the tests of the pkg.
// As special case, if the environment variable CIRCLECI exists, the container isn't created and is assumed to be running
// and listening at http://localhost:8000. Check .circleci/config.yml to see an example of how to run the integration tests
// in CircleCI.
//
// Note: Using this function we can ensure the underlying resources are freed (check https://github.com/golang/go/issues/23404)
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

// GetClient returns a DynamoDB Client configured to access the running dynamodb-local container.
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
		Timeout: time.Second,
	}
	cfg.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
		options.MaxAttempts = 3
	})
	return dynamodb.New(cfg), nil
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

// GetItem returns an item from a table, or nil if it isn't found.
func GetItem(t *testing.T, svc dynamodbiface.ClientAPI, table string, key map[string]string) map[string]dynamodb.AttributeValue {
	itemKey := make(map[string]dynamodb.AttributeValue, len(key))
	for k, v := range key {
		itemKey[k] = dynamodb.AttributeValue{S: aws.String(v)}
	}
	request := svc.GetItemRequest(&dynamodb.GetItemInput{
		Key:                    itemKey,
		ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityNone,
		TableName:              aws.String(table),
	})
	ctx := context.Background()
	response, err := request.Send(ctx)
	require.Nil(t, err)
	return response.Item
}
