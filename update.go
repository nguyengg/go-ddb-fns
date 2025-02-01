package ddbfns

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateOpts customises [Fns.Update] operations per each invocation.
type UpdateOpts struct {
	// DisableOptimisticLocking, if true, will skip all logic concerning version attribute.
	DisableOptimisticLocking bool
	// DisableAutoGeneratedTimestamps, if true, will skip all logic concerning timestamp attributes.
	DisableAutoGeneratedTimestamps bool

	// TableName modifies the [dynamodb.UpdateItemInput.TableName]
	TableName string
	// ReturnConsumedCapacity modifies the [dynamodb.UpdateItemInput.ReturnConsumedCapacity]
	ReturnConsumedCapacity types.ReturnConsumedCapacity
	// ReturnItemCollectionMetrics modifies the [dynamodb.UpdateItemInput.ReturnItemCollectionMetrics]
	ReturnItemCollectionMetrics types.ReturnItemCollectionMetrics
	// ReturnValues modifies the [dynamodb.UpdateItemInput.ReturnValues]
	ReturnValues types.ReturnValue
	// ReturnValuesOnConditionCheckFailure modifies the [dynamodb.UpdateItemInput.ReturnValuesOnConditionCheckFailure].
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

// Update creates the UpdateItem request for the given item and at least one update expression.
//
// If the item's version is at its zero value, `attribute_not_exists(#hash_key)` is used as the condition expression
// to prevent overriding an existing item with the same key in database. An `ADD #version 1` update expression will be
// used to update the version.
//
// If the item's version is not at its zero value, `#version = :version` is used as the condition expression to perform
// optimistic locking. An `ADD #version 1` update expression will be used to update the version.
//
// Modified time will always be set to [time.Now] unless disabled by UpdateOpts.
func (f *Fns) Update(v interface{}, update expression.UpdateBuilder, optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemInput, error) {
	f.init.Do(f.initFn)

	opts := &UpdateOpts{}
	for _, fn := range optFns {
		fn(opts)
	}

	iv := reflect.ValueOf(v)
	attrs, err := f.loadOrParse(reflect.TypeOf(v))
	if err != nil {
		return nil, err
	}

	// UpdateItem only needs the key.
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
			update = update.Set(expression.Name(versionAttr.Name), expression.Value(&types.AttributeValueMemberN{Value: "1"}))
		case version.CanInt():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatInt(version.Int(), 10)}))
			update = update.Add(expression.Name(versionAttr.Name), expression.Value(1))
		case version.CanUint():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatUint(version.Uint(), 10)}))
			update = update.Add(expression.Name(versionAttr.Name), expression.Value(1))
		case version.CanFloat():
			condition = expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatFloat(version.Float(), 'f', -1, 64)}))
			update = update.Add(expression.Name(versionAttr.Name), expression.Value(1))
		default:
			panic(fmt.Errorf("version attribute's type (%s) is unknown numeric type", version.Type()))
		}
	}

	now := time.Now()

	if modifiedTimeAttr := attrs.ModifiedTime; !opts.DisableAutoGeneratedTimestamps && modifiedTimeAttr != nil {
		modifiedTime, err := modifiedTimeAttr.Get(iv)
		if err != nil {
			return nil, fmt.Errorf("get modifiedTime value error: %w", err)
		}

		var av types.AttributeValue
		if modifiedTimeAttr.UnixTime {
			av, err = attributevalue.UnixTime(now).MarshalDynamoDBAttributeValue()
			if err != nil {
				return nil, fmt.Errorf("encode modifiedTime as UnixTime error: %w", err)
			}
		} else {
			updateValue := reflect.ValueOf(now).Convert(modifiedTime.Type())
			if av, err = f.Encoder.Encode(updateValue.Interface()); err != nil {
				return nil, fmt.Errorf("encode modifiedTime error: %w", err)
			}
		}

		update = update.Set(expression.Name(modifiedTimeAttr.Name), expression.Value(av))
	}

	var expr expression.Expression
	if condition.IsSet() {
		expr, err = expression.NewBuilder().WithUpdate(update).WithCondition(condition).Build()
	} else {
		expr, err = expression.NewBuilder().WithUpdate(update).Build()
	}
	if err != nil {
		return nil, fmt.Errorf("build expressions error: %w", err)
	}

	return &dynamodb.UpdateItemInput{
		Key:                                 key,
		TableName:                           &opts.TableName,
		ConditionExpression:                 expr.Condition(),
		ExpressionAttributeNames:            expr.Names(),
		ExpressionAttributeValues:           expr.Values(),
		ReturnConsumedCapacity:              opts.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics:         opts.ReturnItemCollectionMetrics,
		ReturnValues:                        opts.ReturnValues,
		ReturnValuesOnConditionCheckFailure: opts.ReturnValuesOnConditionCheckFailure,
		UpdateExpression:                    expr.Update(),
	}, nil
}

// DoUpdate performs a [Fns.Update] and then executes the request with the specified DynamoDB client.
func (f *Fns) DoUpdate(ctx context.Context, client *dynamodb.Client, v interface{}, update expression.UpdateBuilder, optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemOutput, error) {
	input, err := f.Update(v, update, optFns...)
	if err != nil {
		return nil, err
	}

	return client.UpdateItem(ctx, input)
}

// Update creates the UpdateItem request for the given item and at least one update expression.
//
// Update is a wrapper around [DefaultFns.Update]; see [Fns.Update] for more information.
func Update(v interface{}, update expression.UpdateBuilder, optFns ...func(opts *UpdateOpts)) (*dynamodb.UpdateItemInput, error) {
	return DefaultFns.Update(v, update, optFns...)
}

// DoUpdate is a wrapper around [DefaultFns.DoUpdate]; see [Fns.DoUpdate] for more information.
func DoUpdate(ctx context.Context, client *dynamodb.Client, v interface{}, update expression.UpdateBuilder, optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemOutput, error) {
	return DefaultFns.DoUpdate(ctx, client, v, update, optFns...)
}
