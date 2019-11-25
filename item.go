package godynamodb

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrSNotFound   = errors.New("S not found")
	ErrNNotFound   = errors.New("N not found")
)

// TODO: add documentation
type Item map[string]dynamodb.AttributeValue

func (item Item) SAsString(key string) (string, error) {
	value, ok := item[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	if value.S == nil {
		return "", ErrSNotFound
	}
	return *value.S, nil
}

func (item Item) SAsUUID(key string) (uuid.UUID, error) {
	value, err := item.SAsString(key)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("value %s is not a valid UUID: %w", value, err)
	}
	return id, nil
}

func (item Item) SAsURL(key string) (*url.URL, error) {
	value, err := item.SAsString(key)
	if err != nil {
		return nil, err
	}
	parsedURL, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("value %s is not a valid URL: %w", value, err)
	}
	return parsedURL, nil
}

func (item Item) SAsTime(key, layout string) (time.Time, error) {
	value, err := item.SAsString(key)
	if err != nil {
		return time.Time{}, err
	}
	n, err := time.Parse(layout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("value %s is not a valid time.Time: %w", value, err)
	}
	return n, nil
}

func (item Item) NAsString(key string) (string, error) {
	value, ok := item[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	if value.N == nil {
		return "", ErrNNotFound
	}
	return *value.N, nil
}

func (item Item) NAsInt(key string) (int, error) {
	value, err := item.NAsString(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("value %s is not a valid int: %w", value, err)
	}
	return n, nil
}

func (item Item) NAsInt64(key string) (int64, error) {
	value, err := item.NAsString(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value %s is not a valid int64: %w", value, err)
	}
	return n, nil
}
