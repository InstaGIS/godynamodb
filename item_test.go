package godynamodb

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItem_SAsString(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"name": {S: aws.String("John")},
		})
		name, err := item.SAsString("name")
		require.Nil(t, err)
		assert.Equal(t, "John", name)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		name, err := item.SAsString("name")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Empty(t, name)
	})
	t.Run("ErrSNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"fullName": {SS: []string{"John", "Thornton"}},
		})
		name, err := item.SAsString("fullName")
		assert.Equal(t, ErrSNotFound, err)
		assert.Empty(t, name)
	})
}

func TestItem_SAsUUID(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"id": {S: aws.String("e3350140-3f7a-484d-b155-ad1f2776062d")},
		})
		id, err := item.SAsUUID("id")
		require.Nil(t, err)
		assert.Equal(t, uuid.MustParse("e3350140-3f7a-484d-b155-ad1f2776062d"), id)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		id, err := item.SAsUUID("id")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Equal(t, uuid.Nil, id)
	})
	t.Run("ErrSNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{"id": {N: aws.String("1")}})
		id, err := item.SAsUUID("id")
		assert.Equal(t, ErrSNotFound, err)
		assert.Equal(t, uuid.Nil, id)
	})
}

func TestItem_SAsURL(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"endpoint": {S: aws.String("http://localhost")},
		})
		endpoint, err := item.SAsURL("endpoint")
		require.Nil(t, err)
		assert.Equal(t, "http://localhost", endpoint.String())
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		endpoint, err := item.SAsURL("endpoint")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Nil(t, endpoint)
	})
	t.Run("ErrSNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"endpoint": {SS: []string{"http://localhost"}},
		})
		endpoint, err := item.SAsURL("endpoint")
		assert.Equal(t, ErrSNotFound, err)
		assert.Nil(t, endpoint)
	})
}

func TestItem_SAsTime(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"createdAt": {S: aws.String("2019-10-27T19:36:33Z")},
		})
		createdAt, err := item.SAsTime("createdAt", time.RFC3339)
		require.Nil(t, err)
		assert.Equal(t, time.Date(2019, 10, 27, 19, 36, 33, 0, time.UTC), createdAt)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		createdAt, err := item.SAsTime("createdAt", time.RFC3339)
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Zero(t, createdAt)
	})
	t.Run("ErrSNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"createdAt": {SS: []string{"http://localhost"}},
		})
		createdAt, err := item.SAsTime("createdAt", time.RFC3339)
		assert.Equal(t, ErrSNotFound, err)
		assert.Zero(t, createdAt)
	})
}

func TestItem_NAsString(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {N: aws.String("37")},
		})
		age, err := item.NAsString("age")
		require.Nil(t, err)
		assert.Equal(t, "37", age)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		age, err := item.NAsString("age")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Empty(t, age)
	})
	t.Run("ErrNNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {SS: []string{"37"}},
		})
		age, err := item.NAsString("age")
		assert.Equal(t, ErrNNotFound, err)
		assert.Empty(t, age)
	})
}

func TestItem_NAsInt(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {N: aws.String("37")},
		})
		age, err := item.NAsInt("age")
		require.Nil(t, err)
		assert.Equal(t, 37, age)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		age, err := item.NAsInt("age")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Zero(t, age)
	})
	t.Run("ErrNNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {SS: []string{"37"}},
		})
		age, err := item.NAsInt("age")
		assert.Equal(t, ErrNNotFound, err)
		assert.Zero(t, age)
	})
}

func TestItem_NAsInt64(t *testing.T) {
	t.Parallel()
	t.Run("OK", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {N: aws.String("37")},
		})
		age, err := item.NAsInt64("age")
		require.Nil(t, err)
		assert.Equal(t, int64(37), age)
	})
	t.Run("ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{})
		age, err := item.NAsInt64("age")
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Zero(t, age)
	})
	t.Run("ErrNNotFound", func(t *testing.T) {
		t.Parallel()
		item := Item(map[string]dynamodb.AttributeValue{
			"age": {SS: []string{"37"}},
		})
		age, err := item.NAsInt64("age")
		assert.Equal(t, ErrNNotFound, err)
		assert.Zero(t, age)
	})
}
