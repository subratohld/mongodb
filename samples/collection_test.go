package testss

import (
	"context"
	"fmt"
	"testing"

	"github.com/subratohld/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	URI        = "mongodb://localhost:27017/?connect=direct"
	DB         = "testdb"
	COLLECTION = "testcoll"
)

type User struct {
	Id   string `bson:"_id,omitempty"`
	Name string `bson:"name,omitempty"`
	Age  int    `bson:"age,omitempty"`
}

func TestInsertOne(t *testing.T) {
	client, err := mongodb.NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}

	coll := client.Database(DB).Collection(COLLECTION)
	fmt.Println(coll.Database().Name())

	user := User{Name: "Priya", Age: 25}
	id, err := coll.InsertOne(context.TODO(), user)
	fmt.Println(err)
	fmt.Println(id)
}

func TestInsertMany(t *testing.T) {
	client, err := mongodb.NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	coll := client.Database(DB).Collection(COLLECTION)

	users := []interface{}{
		User{Name: "Shekhar", Age: 25},
		User{Name: "Priya", Age: 22},
	}
	id, err := coll.InsertMany(context.TODO(), users)
	fmt.Println(err)
	fmt.Println(id)
}

func TestFindOne(t *testing.T) {
	client, err := mongodb.NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	var result User

	coll := client.Database(DB).Collection(COLLECTION)

	err = coll.FindOne(context.TODO(), bson.M{"name": "Subrato"}, &result)
	printRes(err, result)

	err = coll.FindOne(context.TODO(), bson.M{}, &result)
	printRes(err, result)
}

func TestFindSorting(t *testing.T) {
	client, err := mongodb.NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	var results []User

	coll := client.Database(DB).Collection(COLLECTION)

	opts := options.Find()

	opts.SetSort(bson.D{{"name", 1}}) // Ascending order
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)

	opts.SetSort(bson.D{{"age", -1}}) // Descending order
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)

	opts.SetSort(bson.D{{"age", -1}, {"name", 1}})
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)
}

func TestFindProjection(t *testing.T) {
	client, err := mongodb.NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	var results []User

	coll := client.Database(DB).Collection(COLLECTION)

	opts := options.Find()

	opts.SetProjection(bson.D{{"_id", -1}})
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)

	opts.SetProjection(bson.D{{"age", 1}})
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)

	opts.SetProjection(bson.D{{"name", 1}})
	err = coll.Find(context.TODO(), bson.M{}, &results, opts)
	printRes(err, results)
}

func printRes(err error, data interface{}) {
	fmt.Println(err)
	if err == nil {
		fmt.Println(data)
	}

}
