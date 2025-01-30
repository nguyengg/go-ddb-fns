package ddbfns

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFns_DeleteWithVersion(t *testing.T) {
	type Test struct {
		Id      string `dynamodbav:"id,hashkey"`
		Sort    string `dynamodbav:"sort,sortkey"`
		Version int64  `dynamodbav:"version,version"`
	}
	input := Test{
		Id:   "hello",
		Sort: "world",
		// Doesn't matter the value here, it will be used for the condition expression.
		Version: 3,
	}

	// this is to make sure the input item is not mutated.
	before := MustToJSON(input)

	got, err := Delete(input)
	if err != nil {
		t.Errorf("Delete() error = %v", err)
		return
	}

	assert.JSONEq(t, before, MustToJSON(input))
	assert.Equal(t, "#0 = :0", *got.ConditionExpression)
	assert.Equal(t, map[string]string{"#0": "version"}, got.ExpressionAttributeNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": &types.AttributeValueMemberN{Value: "3"}}, got.ExpressionAttributeValues)
	assert.Equal(t, map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "hello"}, "sort": &types.AttributeValueMemberS{Value: "world"}}, got.Key)
}

func TestFns_DeleteNoVersion(t *testing.T) {
	type Test struct {
		Id string `dynamodbav:"id,hashkey"`
	}
	input := Test{
		Id: "hello",
	}

	// this is to make sure the input item is not mutated.
	before := MustToJSON(input)

	got, err := Delete(input)
	if err != nil {
		t.Errorf("Delete() error = %v", err)
		return
	}

	assert.JSONEq(t, before, MustToJSON(input))
	assert.Nil(t, got.ConditionExpression)
	assert.Empty(t, got.ExpressionAttributeNames)
	assert.Empty(t, got.ExpressionAttributeValues)
	assert.Equal(t, map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "hello"}}, got.Key)
}
