package ddbfns

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

// GetOpts customises [Fns.Get] operations per each invocation.
type GetOpts struct {
	// TableName modifies the [dynamodb.GetItemInput.TableName]
	TableName string
	// ConsistentRead modifies the [dynamodb.GetItemInput.ConsistentRead]
	ConsistentRead *bool
	// ExpressionAttributeNames modifies the [dynamodb.GetItemInput.ExpressionAttributeNames]
	ExpressionAttributeNames map[string]string
	// ProjectionExpression modifies the [dynamodb.GetItemInput.ProjectionExpression]
	ProjectionExpression *string
	// ReturnConsumedCapacity modifies the [dynamodb.GetItemInput.ReturnConsumedCapacity]
	ReturnConsumedCapacity types.ReturnConsumedCapacity

	projectionExpressionNames []string
}

// Get creates the GetItem request for the given item.
//
// This is mostly a convenient method to create the GetItemInput without having to manually pull the key attributes out
// of the struct. Additionally, GetOpts provides convenient methods to customise the projection expression as well
// (see WithProjectionExpression).
func (f *Fns) Get(v interface{}, optFns ...func(*GetOpts)) (*dynamodb.GetItemInput, error) {
	f.init.Do(f.initFn)

	opts := &GetOpts{}
	for _, fn := range optFns {
		fn(opts)
	}

	attrs, err := f.loadOrParse(reflect.TypeOf(v))
	if err != nil {
		return nil, err
	}

	// GetItem only needs the key.
	var key map[string]types.AttributeValue
	if av, err := f.Encoder.Encode(v); err != nil {
		return nil, err
	} else if asMap, ok := av.(*types.AttributeValueMemberM); !ok {
		return nil, fmt.Errorf("item did not encode to M type")
	} else {
		item := asMap.Value
		key = map[string]types.AttributeValue{attrs.HashKey.Name: item[attrs.HashKey.Name]}
		if attrs.SortKey != nil {
			key[attrs.SortKey.Name] = item[attrs.SortKey.Name]
		}
	}

	if names := opts.projectionExpressionNames; len(names) != 0 {
		projection := expression.NamesList(expression.Name(names[0]))
		for _, name := range names[1:] {
			projection = projection.AddNames(expression.Name(name))
		}

		expr, err := expression.NewBuilder().WithProjection(projection).Build()
		if err != nil {
			return nil, fmt.Errorf("build expressions error: %w", err)
		}

		opts.ExpressionAttributeNames = expr.Names()
		opts.ProjectionExpression = expr.Projection()
	}

	return &dynamodb.GetItemInput{
		Key:                      key,
		TableName:                &opts.TableName,
		ConsistentRead:           opts.ConsistentRead,
		ExpressionAttributeNames: opts.ExpressionAttributeNames,
		ProjectionExpression:     opts.ProjectionExpression,
		ReturnConsumedCapacity:   opts.ReturnConsumedCapacity,
	}, nil
}

// DoGet performs a [Fns.Get] and then executes the request with the specified DynamoDB client.
func (f *Fns) DoGet(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(*GetOpts)) (*dynamodb.GetItemOutput, error) {
	input, err := f.Get(v, optFns...)
	if err != nil {
		return nil, err
	}

	return client.GetItem(ctx, input)
}

// Get creates the GetItem request for the given item.
//
// Get is a wrapper around [DefaultFns.Get]; see [Fns.Get] for more information.
func Get(v interface{}, optFns ...func(*GetOpts)) (*dynamodb.GetItemInput, error) {
	return DefaultFns.Get(v, optFns...)
}

// DoGet is a wrapper around [DefaultFns.DoGet]; see [Fns.DoGet] for more information.
func DoGet(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(*GetOpts)) (*dynamodb.GetItemOutput, error) {
	return DefaultFns.DoGet(ctx, client, v, optFns...)
}

// WithProjectionExpression can be used to set GetOpts.ProjectionExpression and GetOpts.ExpressionAttributeNames.
//
// This will override existing values of both GetOpts.ProjectionExpression and GetOpts.ExpressionAttributeNames.
func WithProjectionExpression(name string, names ...string) func(*GetOpts) {
	return func(opts *GetOpts) {
		opts.projectionExpressionNames = append([]string{name}, names...)
	}
}
