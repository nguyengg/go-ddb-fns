package ddbfns

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// WithTableName overrides [GetOpts.TableName].
func (o *GetOpts) WithTableName(tableName string) *GetOpts {
	o.TableName = &tableName
	return o
}

// WithTableName overrides [PutOpts.TableName].
func (o *PutOpts) WithTableName(tableName string) *PutOpts {
	o.TableName = &tableName
	return o
}

// WithTableName overrides [UpdateOpts.TableName].
func (o *UpdateOpts) WithTableName(tableName string) *UpdateOpts {
	o.TableName = &tableName
	return o
}

// WithTableName overrides [DeleteOps.TableName].
func (o *DeleteOps) WithTableName(tableName string) *DeleteOps {
	o.TableName = &tableName
	return o
}

// Decode will decode the [dynamodb.GetItemOutput.Item] into the given struct pointer.
//
// This opt is only used by DoGet to avoid having to manually unmarshal the returned item from DynamoDB. Unmarshalling
// error will be returned to caller. If the returned item is empty, unmarshalling will not happen.
func (o *GetOpts) Decode(out interface{}) *GetOpts {
	o.out = out
	return o
}

// WithReturnValues overrides [PutOpts.ReturnValues].
func (o *PutOpts) WithReturnValues(returnValues types.ReturnValue) *PutOpts {
	o.ReturnValues = returnValues
	return o
}

// WithReturnValues overrides [UpdateOpts.ReturnValues].
func (o *UpdateOpts) WithReturnValues(returnValues types.ReturnValue) *UpdateOpts {
	o.ReturnValues = returnValues
	return o
}

// WithReturnValues overrides [DeleteOps.ReturnValues].
func (o *DeleteOps) WithReturnValues(returnValues types.ReturnValue) *DeleteOps {
	o.ReturnValues = returnValues
	return o
}

// WithReturnValuesOnConditionCheckFailure overrides [PutOpts.ReturnValuesOnConditionCheckFailure].
func (o *PutOpts) WithReturnValuesOnConditionCheckFailure(returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure) *PutOpts {
	o.ReturnValuesOnConditionCheckFailure = returnValuesOnConditionCheckFailure
	return o
}

// WithReturnValuesOnConditionCheckFailure overrides [UpdateOpts.ReturnValuesOnConditionCheckFailure].
func (o *UpdateOpts) WithReturnValuesOnConditionCheckFailure(returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure) *UpdateOpts {
	o.ReturnValuesOnConditionCheckFailure = returnValuesOnConditionCheckFailure
	return o
}

// WithReturnValuesOnConditionCheckFailure overrides [DeleteOps.ReturnValuesOnConditionCheckFailure].
func (o *DeleteOps) WithReturnValuesOnConditionCheckFailure(returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure) *DeleteOps {
	o.ReturnValuesOnConditionCheckFailure = returnValuesOnConditionCheckFailure
	return o
}

// Decode will decode the [dynamodb.PutItemOutput.Attributes] into the given struct pointer.
//
// This opt is only used by DoPut to avoid having to manually unmarshal the returned item from DynamoDB.
// Unmarshalling error will be returned to caller. If the returned item is empty, unmarshalling will not happen.
//
// Should be used with WithReturnValues or WithReturnValuesOnConditionCheckFailure.
func (o *PutOpts) Decode(out interface{}) *PutOpts {
	o.out = out
	return o
}

// Decode will decode the [dynamodb.UpdateItemOutput.Attributes] into the given struct pointer.
//
// This opt is only used by DoUpdate to avoid having to manually unmarshal the returned item from DynamoDB.
// Unmarshalling error will be returned to caller. If the returned item is empty, unmarshalling will not happen.
//
// Should be used with WithReturnValues or WithReturnValuesOnConditionCheckFailure.
func (o *UpdateOpts) Decode(out interface{}) *UpdateOpts {
	o.out = out
	return o
}

// Decode will decode the [dynamodb.DeleteItemOutput.Attributes] into the given struct pointer.
//
// This opt is only used by DoDelete to avoid having to manually unmarshal the returned item from DynamoDB.
// Unmarshalling error will be returned to caller. If the returned item is empty, unmarshalling will not happen.
//
// Should be used with WithReturnValues or WithReturnValuesOnConditionCheckFailure.
func (o *DeleteOps) Decode(out interface{}) *DeleteOps {
	o.out = out
	return o
}
