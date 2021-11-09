package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Database interface {
	Name() string
	Client() Client
	Collection(name string, opts ...*options.CollectionOptions) Collection
	CreateCollection(context.Context, string, ...*options.CreateCollectionOptions) error
	ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (*mongo.Cursor, error)
	ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error)
	ListCollectionSpecifications(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]*mongo.CollectionSpecification, error)
	CreateView(ctx context.Context, name, viewOn string, pipeline interface{}, opts ...*options.CreateViewOptions) error
	Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error)
	Drop(context.Context) error
	RunCommand(ctx context.Context, runCmd interface{}, opts ...*options.RunCmdOptions) *mongo.SingleResult
	RunCommandCursor(ctx context.Context, runCmd interface{}, opts ...*options.RunCmdOptions) (*mongo.Cursor, error)
	ReadConcern() *readconcern.ReadConcern
	WriteConcern() *writeconcern.WriteConcern
	ReadPreference() *readpref.ReadPref
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

func newDB(c *client, name string, opts ...*options.DatabaseOptions) Database {
	db := c.Client.Database(name, opts...)
	return &database{
		Database: db,
		client:   c,
	}
}

type database struct {
	*mongo.Database
	client *client
}

func (db *database) Client() Client {
	return db.client
}

func (db *database) Collection(name string, opts ...*options.CollectionOptions) Collection {
	return newCollection(db, name, opts...)
}
