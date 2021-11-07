package mongodb

import (
	"context"
	"fmt"
	"testing"
)

const (
	URI        = "mongodb://localhost:27017/?connect=direct"
	DB         = "testdb"
	COLLECTION = "testcoll"
)

type User struct {
	Id   string `bson:"_id,omitempty"`
	Name string `bson:"name"`
	Age  int    `bson:"age"`
}

func TestInsertOne(t *testing.T) {
	client, err := NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}

	coll := client.Database(DB).Collection(COLLECTION)
	fmt.Println(coll.Database().Name())

	user := User{Name: "Subrato", Age: 30}
	id, err := coll.InsertOne(context.TODO(), user)
	fmt.Println(err)
	fmt.Println(id)
}

func TestInsertMany(t *testing.T) {
	client, err := NewClient(context.TODO(), URI)
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
	client, err := NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	coll := client.Database(DB).Collection(COLLECTION)

	op := &FindOneOption{
		Filter: map[string]interface{}{
			"_id": "6187b07057e2c2a17b5ce011",
		},
	}

	var result User
	err = coll.FindOne(context.TODO(), op, &result)

	fmt.Println(err)
	fmt.Println(result)
}

func TestFind(t *testing.T) {
	client, err := NewClient(context.TODO(), URI)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err = client.Disconnect(context.TODO())
		fmt.Println("Disconnect err: ", err)
	}()

	coll := client.Database(DB).Collection(COLLECTION)

	findOpt := &FindOption{}
	findOpt.Filter = nil
	findOpt.SortBy = []Ordering{DescOrder{"_id"}}

	var results []User
	err = coll.Find(context.TODO(), findOpt, &results)

	fmt.Println(err)
	fmt.Println(results)
}
