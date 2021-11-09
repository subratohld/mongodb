package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection interface {
	Database() Database
	Name() string
	Drop(ctx context.Context) error
	Indexes() mongo.IndexView
	InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (string, error)
	InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) ([]string, error)
	UpdateByID(ctx context.Context, id interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	FindOne(ctx context.Context, filter interface{}, result interface{}, opts ...*options.FindOneOptions) error
	Find(ctx context.Context, filter interface{}, results interface{}, opts ...*options.FindOptions) error
	FindOneAndDelete(ctx context.Context, filter map[string]interface{}, target interface{}, opts ...*options.FindOneAndDeleteOptions) error
	FindOneAndUpdate(ctx context.Context, filter map[string]interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) error
	FindOneAndReplace(ctx context.Context, filter map[string]interface{}, replace interface{}, opts ...*options.FindOneAndReplaceOptions) error
	ReplaceOne(ctx context.Context, filter map[string]interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	Aggregate(ctx context.Context, pipeline interface{}, target interface{}, opts ...*options.AggregateOptions) error
	BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	Clone(opts ...*options.CollectionOptions) (*mongo.Collection, error)
	CountDocuments(ctx context.Context, filter map[string]interface{}, opts ...*options.CountOptions) (int64, error)
	Distinct(ctx context.Context, fieldName string, filter map[string]interface{}, opts ...*options.DistinctOptions) ([]interface{}, error)
	EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error)
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

func newCollection(db *MongoDatabase, name string, opts ...*options.CollectionOptions) Collection {
	coll := db.Database.Collection(name, opts...)
	return &MongoCollection{
		db:         db,
		Collection: coll,
	}
}

type MongoCollection struct {
	db *MongoDatabase
	*mongo.Collection
}

func (coll *MongoCollection) Database() Database {
	return coll.db
}

func (coll *MongoCollection) Name() string {
	return coll.Collection.Name()
}

func (coll *MongoCollection) Drop(ctx context.Context) error {
	return coll.Collection.Drop(ctx)
}

func (coll *MongoCollection) Indexes() mongo.IndexView {
	return coll.Collection.Indexes()
}

func (coll *MongoCollection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (string, error) {
	res, err := coll.Collection.InsertOne(ctx, document, opts...)
	if err != nil {
		return "", err
	}

	obj, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", errors.New("mongodb: not a valid 'primitive.ObjectID'")
	}

	return obj.Hex(), nil
}

func (coll *MongoCollection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) ([]string, error) {
	res, err := coll.Collection.InsertMany(ctx, documents, opts...)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(res.InsertedIDs))
	for i, objId := range res.InsertedIDs {
		id, ok := objId.(primitive.ObjectID)
		if !ok {
			return nil, errors.New("mongodb: not a valid 'primitive.ObjectID'")
		}
		ids[i] = id.Hex()
	}
	return ids, nil
}

func (coll *MongoCollection) UpdateByID(ctx context.Context, id interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return coll.Collection.UpdateByID(ctx, id, update, opts...)
}

func (coll *MongoCollection) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return coll.Collection.UpdateOne(ctx, filter, update, opts...)
}

func (coll *MongoCollection) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return coll.Collection.UpdateMany(ctx, filter, update, opts...)
}

func (coll *MongoCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return coll.Collection.DeleteOne(ctx, filter, opts...)
}

func (coll *MongoCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return coll.Collection.DeleteMany(ctx, filter, opts...)
}

func (coll *MongoCollection) FindOne(ctx context.Context, filter interface{}, result interface{}, opts ...*options.FindOneOptions) error {
	res := coll.Collection.FindOne(ctx, filter, opts...)
	if err := res.Err(); err != nil {
		return err
	}

	return res.Decode(result)
}

func (coll *MongoCollection) Find(ctx context.Context, filter interface{}, results interface{}, opts ...*options.FindOptions) (err error) {
	cursor, err := coll.Collection.Find(ctx, filter, opts...)
	if err != nil {
		return err
	}

	defer func() {
		err = cursor.Close(ctx)
	}()

	err = cursor.All(ctx, results)
	return
}

func (coll *MongoCollection) FindOneAndDelete(ctx context.Context, filter map[string]interface{}, target interface{}, opts ...*options.FindOneAndDeleteOptions) error {
	res := coll.Collection.FindOneAndDelete(ctx, filter, opts...)
	if res.Err() != nil {
		return res.Err()
	}

	return res.Decode(target)
}

func (coll *MongoCollection) FindOneAndUpdate(ctx context.Context, filter map[string]interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) error {
	res := coll.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
	return res.Err()
}

func (coll *MongoCollection) FindOneAndReplace(ctx context.Context, filter map[string]interface{}, replace interface{}, opts ...*options.FindOneAndReplaceOptions) error {
	res := coll.Collection.FindOneAndReplace(ctx, filter, replace, opts...)
	return res.Err()
}

func (coll *MongoCollection) ReplaceOne(ctx context.Context, filter map[string]interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	return coll.Collection.ReplaceOne(ctx, filter, replacement, opts...)
}

func (coll *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, target interface{}, opts ...*options.AggregateOptions) (err error) {
	cursor, err := coll.Collection.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return
	}

	defer func() {
		err = cursor.Close(ctx)
	}()

	return cursor.All(ctx, target)
}

func (coll *MongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	return coll.Collection.BulkWrite(ctx, models, opts...)
}

func (coll *MongoCollection) Clone(opts ...*options.CollectionOptions) (*mongo.Collection, error) {
	return coll.Collection.Clone(opts...)
}

func (coll *MongoCollection) CountDocuments(ctx context.Context, filter map[string]interface{}, opts ...*options.CountOptions) (int64, error) {
	return coll.Collection.CountDocuments(ctx, filter, opts...)
}

func (coll *MongoCollection) Distinct(ctx context.Context, fieldName string, filter map[string]interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	return coll.Collection.Distinct(ctx, fieldName, filter, opts...)
}

func (coll *MongoCollection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	return coll.Collection.EstimatedDocumentCount(ctx, opts...)
}

func (coll *MongoCollection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	return coll.Collection.Watch(ctx, pipeline, opts...)
}
