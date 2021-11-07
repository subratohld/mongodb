package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client interface {
	Disconnect(ctx context.Context) error
	Database(name string, opts ...*options.DatabaseOptions) Database
	ListDatabases(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error)
	ListDatabaseNames(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) ([]string, error)
	StartSession(opts ...*options.SessionOptions) (mongo.Session, error)
	UseSession(ctx context.Context, fn func(mongo.SessionContext) error) error
	UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(mongo.SessionContext) error) error
	NumberSessionsInProgress() int
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

func NewClient(ctx context.Context, host, port string) (Client, error) {
	url := fmt.Sprintf("mongodb://%s:%s", host, port)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	mClient := &MongoClient{}
	mClient.Client = client
	return mClient, nil
}

type MongoClient struct {
	*mongo.Client
}

func (m *MongoClient) Database(name string, opts ...*options.DatabaseOptions) Database {
	return newDB(m, name, opts...)
}

func (m *MongoClient) Disconnect(ctx context.Context) error {
	return m.Client.Disconnect(ctx)
}
