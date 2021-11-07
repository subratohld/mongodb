package mongodb

import (
	"context"

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

// Direct=> mongodb://localhost:27017/?connect=direct
// Replicaset=> mongodb://localhost:27017,localhost:27018/?replicaSet=replset
// Sharded=> mongodb://localhost:27017,localhost:27018
// mongodb://ldap-user:ldap-pwd@localhost:27017/?authMechanism=PLAIN
func NewClient(ctx context.Context, uri string) (Client, error) {
	clientOpts := options.Client().ApplyURI(uri)
	return connect(ctx, clientOpts)
}

// AWS Credential=> options.Credential{AuthMechanism: "MONGODB-AWS", Username: "accessKeyId", Password: "secretAccessKey"}
// AWS Assumerole=> options.Credential{AuthMechanism: "MONGODB-AWS", Username: "accessKeyId", Password: "secretAccessKey", AuthMechanismProperties: map[string]string{"AWS_SESSION_TOKEN": "sessionToken"}}
func NewClientWithCredential(ctx context.Context, cred options.Credential) (Client, error) {
	clientOpts := options.Client().SetAuth(cred)
	return connect(ctx, clientOpts)
}

func NewClientWithUriCredential(ctx context.Context, uri string, cred options.Credential) (Client, error) {
	clientOpts := options.Client().ApplyURI(uri).SetAuth(cred)
	return connect(ctx, clientOpts)
}

func connect(ctx context.Context, opts ...*options.ClientOptions) (Client, error) {
	client, err := mongo.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	mClient := &MongoClient{
		Client: client,
	}
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
