// +build itest

package godynamodb_test

import (
	"context"
	"os"
	"testing"

	"github.com/InstaGIS/godynamodb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	test  = &godynamodb.Test{} // test instance
	table = "test-table"       // testing table
)

// TestMain launches the package tests
func TestMain(m *testing.M) {
	code := test.TestMain(m, func(svc *dynamodb.Client) error { // this function setups the database
		// create testing table
		createRequest := svc.CreateTableRequest(&dynamodb.CreateTableInput{
			AttributeDefinitions: []dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("PK"),
					AttributeType: "S",
				},
			},
			KeySchema: []dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("PK"),
					KeyType:       dynamodb.KeyTypeHash,
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			TableName: aws.String(table),
		})
		_, err := createRequest.Send(context.Background())
		if err != nil {
			return err
		}
		// add testing data
		request := svc.PutItemRequest(&dynamodb.PutItemInput{
			Item: map[string]dynamodb.AttributeValue{
				"PK":   {S: aws.String("abc123")},
				"name": {S: aws.String("John")},
			},
			TableName: aws.String(table),
		})
		_, err = request.Send(context.Background())
		if err != nil {
			return err
		}
		return nil
	})
	os.Exit(code)
}

// TestTest_Sample is your integration test
func TestTest_Sample(t *testing.T) {
	t.Parallel()

	// get DynamoDB client and do something
	svc, err := test.GetClient()
	require.Nil(t, err)
	request := svc.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"PK": {S: aws.String("abc123")},
		},
		TableName: aws.String(table),
	})
	response, err := request.Send(context.Background())
	require.Nil(t, err)
	assert.Equal(t, "abc123", *(response.Item["PK"].S))
	assert.Equal(t, "John", *(response.Item["name"].S))
}
