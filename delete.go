package ddbfns

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"strconv"
)

// DeleteOps customises [Fns.Delete] operations per each invocation.
type DeleteOps struct {
	// DisableOptimisticLocking, if true, will skip all logic concerning version attribute.
	DisableOptimisticLocking bool
	// DisableAutoGeneratedTimestamps, if true, will skip all logic concerning timestamp attributes.
	DisableAutoGeneratedTimestamps bool

	// TableName modifies the [dynamodb.DeleteItemInput.TableName]
	TableName *string
	// ReturnConsumedCapacity modifies the [dynamodb.DeleteItemInput.ReturnConsumedCapacity]
	ReturnConsumedCapacity types.ReturnConsumedCapacity
	// ReturnItemCollectionMetrics modifies the [dynamodb.DeleteItemInput.ReturnItemCollectionMetrics]
	ReturnItemCollectionMetrics types.ReturnItemCollectionMetrics
	// ReturnValues modifies the [dynamodb.DeleteItemInput.ReturnValues]
	ReturnValues types.ReturnValue
	// ReturnValuesOnConditionCheckFailure modifies the [dynamodb.DeleteItemInput.ReturnValuesOnConditionCheckFailure].
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

// Delete creates the DeleteItem request for the given item.
//
// The current item's version is used in the `#version = :version` condition expression to perform optimistic locking.
func (f *Fns) Delete(v interface{}, optFns ...func(ops *DeleteOps)) (*dynamodb.DeleteItemInput, error) {
	f.init.Do(f.initFn)

	opts := &DeleteOps{}
	for _, fn := range optFns {
		fn(opts)
	}

	iv := reflect.ValueOf(v)
	attrs, err := f.loadOrParse(reflect.TypeOf(v))
	if err != nil {
		return nil, err
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

	condition := expression.ConditionBuilder{}

	if versionAttr := attrs.Version; !opts.DisableOptimisticLocking && versionAttr != nil {
		version, err := versionAttr.Get(iv)
		if err != nil {
			return nil, fmt.Errorf("get version value error: %w", err)
		}

		switch {
		case version.IsZero():
			condition = expression.Name(attrs.HashKey.Name).AttributeNotExists()
		case version.CanInt():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatInt(version.Int(), 10)}))
		case version.CanUint():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatUint(version.Uint(), 10)}))
		case version.CanFloat():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatFloat(version.Float(), 'f', -1, 64)}))
		default:
			panic(fmt.Errorf("version attribute's type (%s) is unknown numeric type", version.Type()))
		}
	}

	if condition.IsSet() {
		expr, err := expression.NewBuilder().WithCondition(condition).Build()
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
func (f *Fns) DoDelete(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(ops *DeleteOps)) (*dynamodb.DeleteItemOutput, error) {
	input, err := f.Delete(v, optFns...)
	if err != nil {
		return nil, err
	}

	return client.DeleteItem(ctx, input)
}

// Delete creates the DeleteItem request for the given item.
//
// Delete is a wrapper around [DefaultFns.Delete]; see [Fns.Delete] for more information.
func Delete(v interface{}) (*dynamodb.DeleteItemInput, error) {
	return DefaultFns.Delete(v)
}

// DoDelete is a wrapper around [DefaultFns.DoDelete]; see [Fns.DoDelete] for more information.
func DoDelete(ctx context.Context, client *dynamodb.Client, v interface{}, optFns ...func(ops *DeleteOps)) (*dynamodb.DeleteItemOutput, error) {
	return DefaultFns.DoDelete(ctx, client, v, optFns...)
}
