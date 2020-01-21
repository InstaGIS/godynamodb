package godynamodb

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
	"github.com/ory/dockertest"
)

// Test is a DynamoDB integration test that runs using a local Docker container.
type Test struct {
	// Table is the name of the table to create.
	Table string

	endpoint string
}

// TestMain runs the tests.
//
// Using this function we can ensure the underlying resources are freed (check https://github.com/golang/go/issues/23404)
func (t *Test) TestMain(m *testing.M, setupDB func(svc *dynamodb.Client) error) int {
	// run dynamodb container
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("couldn't connect to docker: %s", err)
		return 1
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		ExposedPorts: []string{"8000"},
		Repository:   "amazon/dynamodb-local",
	})
	if err != nil {
		log.Printf("couldn't start container: %s", err)
		return 1
	}
	defer pool.Purge(resource)

	// wait for dynamodb to be ready
	t.endpoint = resource.GetHostPort("8000/tcp")
	log.Printf("endpoint: %s", t.endpoint)
	// FIXME: debug info, remove later
	spew.Dump(resource.Container.NetworkSettings)
	log.Printf("DOCKER_HOST %s", os.Getenv("DOCKER_HOST"))
	err = pool.Retry(t.waitForDynamoDB)
	if err != nil {
		log.Printf("could not connect to dynamodb: %s", err)
		return 1
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
	cfg, err := external.LoadDefaultAWSConfig(external.WithCredentialsValue{
		AccessKeyID:     "KEY",
		SecretAccessKey: "SECRET",
		SessionToken:    "SESSION",
		Source:          "fake credentials",
	})
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}
	cfg.Region = endpoints.UsEast1RegionID
	cfg.EndpointResolver = aws.ResolveWithEndpointURL("http://" + t.endpoint)
	cfg.HTTPClient = &http.Client{
		Timeout: 3 * time.Second,
	}
	cfg.Retryer = aws.NewDefaultRetryer(func(d *aws.DefaultRetryer) {
		d.NumMaxRetries = 1
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
