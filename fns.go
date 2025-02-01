package ddbfns

import (
	"fmt"
	"github.com/nguyengg/go-ddb-fns/internal"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

// Fns contains the attributes parsed from DynamoDB struct tags `dynamodbav`.
//
// Specifically, Fns parses and understands these custom tag values:
//
//	// Hash key is required, sort key is optional. If present, their types must marshal to a valid key type
//	// (S, N, or B). The keys are required to make `attribute_not_exists` work on creating the condition expression
//	// for the PutItem request of an item that shouldn't exist in database.
//	Field string `dynamodbav:"-,hashkey" tableName:"my-table"`
//	Field string `dynamodbav:"-,sortkey"`
//
//	// Versioned attribute must have `version` show up in its `dynamodbav` tag. It must be a numeric type that
//	// marshals to type N in DynamoDB.
//	Field int64 `dynamodbav:"-,version"`
//
//	// Timestamp attributes must have `createdTime` and/or `modifiedTime` in its `dynamodbav` tag. It must be a
//	// [time.Time] value. In this example, both attributes marshal to type N in DynamoDB as epoch millisecond.
//	Field time.Time `dynamodbav:"-,createdTime,unixtime"`
//	Field time.Time `dynamodbav:"-,modifiedTime,unixtime"`
//
// The zero-value Fns instance is ready for use. Prefer NewFns which can perform validation on the struct type.
type Fns struct {
	// Encoder is the attributevalue.Encoder to marshal structs into DynamoDB items.
	//
	// If nil, a default one will be created.
	Encoder *attributevalue.Encoder
	// Decoder is the attributevalue.Decoder to unmarshal results from DynamoDB.
	//
	// If nil, a default one will be created.
	Decoder *attributevalue.Decoder

	init  sync.Once
	cache sync.Map
}

// ParseOpts customises the parsing and validation of NewFns, [Fns.ParseFromStruct], and [Fns.ParseFromType].
type ParseOpts struct {
	// MustHaveVersion, if true, will fail parsing if the struct does not have any field tagged as
	// `dynamodbav:",version"`.
	MustHaveVersion bool
	// MustHaveTimestamps, if true, will fail parsing if the struct does not have any field tagged as
	// `dynamodbav:",createdTime" or `dynamodbav:",modifiedTime".
	MustHaveTimestamps bool
}

// NewFns can be used to parse and validate the struct tags.
//
// This method should be called at least once (can be in the unit test) for every struct that will be used with Fns.
func NewFns[T any](optFns ...func(*ParseOpts)) (*Fns, error) {
	opts := &ParseOpts{}
	for _, fn := range optFns {
		fn(opts)
	}

	f := &Fns{}
	f.init.Do(f.initFn)

	if err := f.ParseFromType(reflect.TypeFor[T]()); err != nil {
		return nil, err
	}

	return f, nil
}

// ParseFromStruct parses and caches the struct tags given by an instance of the struct.
//
// Returns an error if there are validation issues.
func (f *Fns) ParseFromStruct(v interface{}, optFns ...func(*ParseOpts)) error {
	return f.ParseFromType(reflect.TypeOf(v), optFns...)
}

// ParseFromType parses and caches the struct tags given by its type.
//
// Returns an error if there are validation issues.
func (f *Fns) ParseFromType(t reflect.Type, optFns ...func(*ParseOpts)) error {
	opts := ParseOpts{}
	for _, fn := range optFns {
		fn(&opts)
	}

	m, err := internal.ParseFromType(t)
	if err != nil {
		return err
	}

	if m.HashKey == nil {
		return fmt.Errorf(`no hashKey field in type "%s"`, t.Name())
	}
	if opts.MustHaveVersion && m.Version == nil {
		return fmt.Errorf(`no version field in type "%s"`, t.Name())
	}
	if opts.MustHaveTimestamps && m.CreatedTime == nil && m.ModifiedTime == nil {
		return fmt.Errorf(`no timestamp fields in type "%s"`, t.Name())
	}

	f.cache.Store(t, m)
	return nil
}

func (f *Fns) loadOrParse(t reflect.Type) (*internal.Model, error) {
	t = internal.DereferencedType(t)
	v, ok := f.cache.Load(t)
	if ok {
		return v.(*internal.Model), nil
	}

	m, err := internal.ParseFromType(t)
	if err != nil {
		return nil, err
	}

	f.cache.Store(t, m)
	return m, nil
}

func (f *Fns) initFn() {
	if f.Encoder == nil {
		f.Encoder = attributevalue.NewEncoder()
	}
	if f.Decoder == nil {
		f.Decoder = attributevalue.NewDecoder()
	}
}

// DefaultFns is the zero-value Fns instance used by Put, Update, and Delete.
var DefaultFns = &Fns{}
