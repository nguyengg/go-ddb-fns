package ddbfns

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// GetOpts customises [Fns.Get] operations per each invocation.
type GetOpts struct {
	// TableName modifies the [dynamodb.GetItemInput.TableName]
	TableName *string
	// ConsistentRead modifies the [dynamodb.GetItemInput.ConsistentRead]
	ConsistentRead *bool
	// ExpressionAttributeNames modifies the [dynamodb.GetItemInput.ExpressionAttributeNames]
	ExpressionAttributeNames map[string]string
	// ProjectionExpression modifies the [dynamodb.GetItemInput.ProjectionExpression]
	ProjectionExpression *string
	// ReturnConsumedCapacity modifies the [dynamodb.GetItemInput.ReturnConsumedCapacity]
	ReturnConsumedCapacity types.ReturnConsumedCapacity

	projectionExpressionNames []string
	out                       interface{}
}

// Decode will decode the [dynamodb.GetItemOutput.Item] into the given struct pointer.
//
// This opt is only used by DoGet to avoid having to manually unmarshal the returned item from DynamoDB. Unmarshalling
// error will be returned to caller. If the returned item is empty, unmarshalling will not happen.
func (o *GetOpts) Decode(out interface{}) *GetOpts {
	o.out = out
	return o
}

// WithTableName overrides [GetOpts.TableName].
func (o *GetOpts) WithTableName(tableName string) *GetOpts {
	o.TableName = &tableName
	return o
}
