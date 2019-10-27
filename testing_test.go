// +build itest

package godynamodb_test

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/InstaGIS/godynamodb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

// test instance
var test = &godynamodb.Test{
	Table: "test-table",
}

// TestMain launches package tests
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		log.Printf("skipping integration tests")
		os.Exit(0)
	}
	os.Exit(test.TestMain(m, setupDB))
}

// setupDB creates a table in DynamoDB, inserting some testing data.
func setupDB(svc *dynamodb.Client) error {
	// create table
	createRequest := svc.CreateTableRequest(&dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: "S",
			},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       dynamodb.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(test.Table),
	})
	_, err := createRequest.Send(context.Background())
	if err != nil {
		return err
	}

	// add test data
	request := svc.PutItemRequest(&dynamodb.PutItemInput{
		Item: map[string]dynamodb.AttributeValue{
			"id":   {S: aws.String("abc123")},
			"name": {S: aws.String("John")},
		},
		TableName: aws.String(test.Table),
	})
	_, err = request.Send(context.Background())
	if err != nil {
		return err
	}

	return nil
}

// TestTest_Sample is your integration test
func TestTest_Sample(t *testing.T) {
	t.Parallel()

	// get DynamoDB client and do something
	svc, err := test.GetClient()
	require.Nil(t, err)
	request := svc.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"id": {S: aws.String("abc123")},
		},
		TableName: aws.String(test.Table),
	})
	response, err := request.Send(context.Background())
	require.Nil(t, err)
	assert.Equal(t, "abc123", *(response.Item["id"].S))
	assert.Equal(t, "John", *(response.Item["name"].S))
}
