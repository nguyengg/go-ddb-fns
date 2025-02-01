package ddbfns

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestSetOrRemove(t *testing.T) {
	expr, err := expression.NewBuilder().
		WithUpdate(NewUpdateBuilder().
			SetOrRemove(true, true, "notes", "hello, world!").
			Build()).
		Build()
	if err != nil {
		t.Errorf("SetOrRemove() error: %v", err)
	}

	assert.Equal(t, "SET #0 = :0\n", *expr.Update())
	assert.Equal(t, map[string]string{"#0": "notes"}, expr.Names())
	assert.Equal(t, map[string]types.AttributeValue{":0": &types.AttributeValueMemberS{Value: "hello, world!"}}, expr.Values())
}

func TestSetOrRemoveWithExistingUpdateExpression(t *testing.T) {
	expr, err := expression.NewBuilder().
		WithUpdate(NewUpdateBuilder().
			Set("version", 3).
			SetOrRemove(false, true, "notes", "hello, world!").
			Build()).
		Build()
	if err != nil {
		t.Errorf("SetOrRemove() error: %v", err)
	}

	assert.Equal(t, "REMOVE #0\nSET #1 = :0\n", *expr.Update())
	assert.Equal(t, map[string]string{"#0": "notes", "#1": "version"}, expr.Names())
	assert.Equal(t, map[string]types.AttributeValue{":0": &types.AttributeValueMemberN{Value: "3"}}, expr.Values())
}
