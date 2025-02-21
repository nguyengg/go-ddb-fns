package ddbfns

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Delete creates the DeleteItem request for the given item.
//
// The current item's version is used in the `#version = :version` condition expression to perform optimistic locking.
func (f *Fns) Delete(v interface{}, optFns ...func(*DeleteOpts)) (*dynamodb.DeleteItemInput, error) {
	f.init.Do(f.initFn)

	opts := &DeleteOpts{}
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

	// DeleteItem only needs the key.
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

	iv := reflect.Indirect(reflect.ValueOf(v))

	if versionAttr := attrs.Version; !opts.DisableOptimisticLocking && versionAttr != nil {
		version, err := versionAttr.Get(iv)
		if err != nil {
			return nil, fmt.Errorf("get version value error: %w", err)
		}

		switch {
		case version.IsZero():
			opts.And(expression.Name(attrs.HashKey.Name).AttributeNotExists())
		case version.CanInt():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatInt(version.Int(), 10)})))
		case version.CanUint():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatUint(version.Uint(), 10)})))
		case version.CanFloat():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatFloat(version.Float(), 'f', -1, 64)})))
		default:
			panic(fmt.Errorf("version attribute's type (%s) is unknown numeric type", version.Type()))
		}
	}

	if opts.condition.IsSet() {
		expr, err := expression.NewBuilder().WithCondition(opts.condition).Build()
		if err != nil {
			return nil, fmt.Errorf("build expressions error: %w", err)
		}

		return &dynamodb.DeleteItemInput{
			Key:                                 key,
			TableName:                           opts.TableName,
			ConditionExpression:                 expr.Condition(),
			ExpressionAttributeNames:            expr.Names(),
			ExpressionAttributeValues:           expr.Values(),
			ReturnConsumedCapacity:              opts.ReturnConsumedCapacity,
			ReturnItemCollectionMetrics:         opts.ReturnItemCollectionMetrics,
			ReturnValues:                        opts.ReturnValues,
			ReturnValuesOnConditionCheckFailure: opts.ReturnValuesOnConditionCheckFailure,
		}, nil
	}

	return &dynamodb.DeleteItemInput{
		Key:                                 key,
		TableName:                           opts.TableName,
		ReturnConsumedCapacity:              opts.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics:         opts.ReturnItemCollectionMetrics,
		ReturnValues:                        opts.ReturnValues,
		ReturnValuesOnConditionCheckFailure: opts.ReturnValuesOnConditionCheckFailure,
	}, nil
}

// DoDelete performs a [Fns.DoDelete] and then executes the request with the specified DynamoDB client.
func (f *Fns) DoDelete(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(ops *DeleteOpts)) (*dynamodb.DeleteItemOutput, error) {
	var opts *DeleteOpts
	optFns = append(optFns, func(o *DeleteOpts) {
		opts = o
	})

	input, err := f.Delete(v, optFns...)
	if err != nil {
		return nil, err
	}

	deleteItemOutput, err := client.DeleteItem(ctx, input)
	if err != nil || opts.out == nil {
		return deleteItemOutput, err
	}

	if item := deleteItemOutput.Attributes; len(item) != 0 {
		err = f.Decoder.Decode(&types.AttributeValueMemberM{Value: item}, opts.out)
	}

	return deleteItemOutput, err
}

// Delete creates the DeleteItem request for the given item.
//
// Delete is a wrapper around [DefaultFns.Delete]; see [Fns.Delete] for more information.
func Delete(v interface{}) (*dynamodb.DeleteItemInput, error) {
	return DefaultFns.Delete(v)
}

// DoDelete is a wrapper around [DefaultFns.DoDelete]; see [Fns.DoDelete] for more information.
func DoDelete(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(ops *DeleteOpts)) (*dynamodb.DeleteItemOutput, error) {
	return DefaultFns.DoDelete(ctx, client, v, optFns...)
}
