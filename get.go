package ddbfns

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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

	if opts.TableName == nil {
		opts.TableName = attrs.TableName
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
		TableName:                opts.TableName,
		ConsistentRead:           opts.ConsistentRead,
		ExpressionAttributeNames: opts.ExpressionAttributeNames,
		ProjectionExpression:     opts.ProjectionExpression,
		ReturnConsumedCapacity:   opts.ReturnConsumedCapacity,
	}, nil
}

// DoGet performs a [Fns.Get] and then executes the request with the specified DynamoDB client.
//
// The hash key attribute should have a `tableName` tag such as:
//
//	Field string `dynamodbav:"-,hashkey" tableName:"my-table"`
//
// If the field doesn't have `tableName` tag, you must override the [GetOpts.TableName] for the request to succeed.
func (f *Fns) DoGet(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(*GetOpts)) (*dynamodb.GetItemOutput, error) {
	var opts *GetOpts
	optFns = append(optFns, func(o *GetOpts) {
		opts = o
	})

	input, err := f.Get(v, optFns...)
	if err != nil {
		return nil, err
	}

	getItemOutput, err := client.GetItem(ctx, input)
	if err != nil || opts.out == nil {
		return getItemOutput, err
	}

	if item := getItemOutput.Item; len(item) != 0 {
		err = f.Decoder.Decode(&types.AttributeValueMemberM{Value: item}, opts.out)
	}

	return getItemOutput, err
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
