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
func (f *Fns) Update(v interface{}, requiredUpdateFn func(*UpdateOpts), optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemInput, error) {
	f.init.Do(f.initFn)

	opts := &UpdateOpts{}
	requiredUpdateFn(opts)
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

	iv := reflect.Indirect(reflect.ValueOf(v))

	if versionAttr := attrs.Version; !opts.DisableOptimisticLocking && versionAttr != nil {
		version, err := versionAttr.Get(iv)
		if err != nil {
			return nil, fmt.Errorf("get version value error: %w", err)
		}

		switch {
		case version.IsZero():
			opts.And(expression.Name(attrs.HashKey.Name).AttributeNotExists())
			opts.Set(versionAttr.Name, &types.AttributeValueMemberN{Value: "1"})
		case version.CanInt():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatInt(version.Int(), 10)})))
			opts.Add(versionAttr.Name, 1)
		case version.CanUint():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatUint(version.Uint(), 10)})))
			opts.Add(versionAttr.Name, 1)
		case version.CanFloat():
			opts.And(expression.Name(versionAttr.Name).Equal(expression.Value(&types.AttributeValueMemberN{Value: strconv.FormatFloat(version.Float(), 'f', -1, 64)})))
			opts.Add(versionAttr.Name, 1)
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

		opts.Set(modifiedTimeAttr.Name, av)
	}

	var expr expression.Expression
	if opts.condition.IsSet() {
		expr, err = expression.NewBuilder().WithUpdate(opts.update).WithCondition(opts.condition).Build()
	} else {
		expr, err = expression.NewBuilder().WithUpdate(opts.update).Build()
	}
	if err != nil {
		return nil, fmt.Errorf("build expressions error: %w", err)
	}

	return &dynamodb.UpdateItemInput{
		Key:                                 key,
		TableName:                           opts.TableName,
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
func (f *Fns) DoUpdate(ctx context.Context, client *dynamodb.Client, v interface{}, requiredUpdateFn func(*UpdateOpts), optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemOutput, error) {
	var opts *UpdateOpts
	optFns = append(optFns, func(o *UpdateOpts) {
		opts = o
	})

	input, err := f.Update(v, requiredUpdateFn, optFns...)
	if err != nil {
		return nil, err
	}

	updateItemOutput, err := client.UpdateItem(ctx, input)
	if err != nil || opts.out == nil {
		return updateItemOutput, err
	}

	if item := updateItemOutput.Attributes; len(item) != 0 {
		err = f.Decoder.Decode(&types.AttributeValueMemberM{Value: item}, opts.out)
	}

	return updateItemOutput, err
}

// Update creates the UpdateItem request for the given item and at least one update expression.
//
// Update is a wrapper around [DefaultFns.Update]; see [Fns.Update] for more information.
func Update(v interface{}, requiredUpdateFn func(*UpdateOpts), optFns ...func(opts *UpdateOpts)) (*dynamodb.UpdateItemInput, error) {
	return DefaultFns.Update(v, requiredUpdateFn, optFns...)
}

// DoUpdate is a wrapper around [DefaultFns.DoUpdate]; see [Fns.DoUpdate] for more information.
func DoUpdate(ctx context.Context, client *dynamodb.Client, v interface{}, requiredUpdateFn func(*UpdateOpts), optFns ...func(*UpdateOpts)) (*dynamodb.UpdateItemOutput, error) {
	return DefaultFns.DoUpdate(ctx, client, v, requiredUpdateFn, optFns...)
}
