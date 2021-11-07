package mongodb

import "go.mongodb.org/mongo-driver/mongo/options"

type Ordering interface {
	GetKey() string
	GetOrder() int
}

type AscOrder struct {
	Key string
}

func (asc AscOrder) GetKey() string {
	return asc.Key
}

func (asc AscOrder) GetOrder() int {
	return 1
}

type DescOrder struct {
	Key string
}

func (asc DescOrder) GetKey() string {
	return asc.Key
}

func (asc DescOrder) GetOrder() int {
	return -1
}

type FindOneOption struct {
	Filter map[string]interface{}
	SortBy []Ordering
	Opts   []*options.FindOneOptions
}

type FindOption struct {
	Filter map[string]interface{}
	SortBy []Ordering
	Limit  int64
	Opts   []*options.FindOptions
}
