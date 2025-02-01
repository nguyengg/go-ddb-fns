package ddbfns

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

// And is a zero-value-aware variant of expression.And where the left argument can be the zero-value.
func And(left expression.ConditionBuilder, right expression.ConditionBuilder, other ...expression.ConditionBuilder) expression.ConditionBuilder {
	if left.IsSet() {
		return left.And(right, other...)
	}
	switch len(other) {
	case 0:
		return right
	case 1:
		return right.And(other[0])
	default:
		return right.And(other[0], other[1:]...)
	}
}

// Or is a zero-value-aware variant of expression.Or where the left argument can be the zero-value.
func Or(left expression.ConditionBuilder, right expression.ConditionBuilder, other ...expression.ConditionBuilder) expression.ConditionBuilder {
	if left.IsSet() {
		return left.Or(right, other...)
	}
	switch len(other) {
	case 0:
		return right
	case 1:
		return right.Or(other[0])
	default:
		return right.Or(other[0], other[1:]...)
	}
}

// UpdateBuilder helper that offers SetOrRemove in addition to simplifying existing methods from expression.UpdateBuilder.
//
// The zero-value is ready for use. At least one update expression must be created or the eventual expression building
// will fail.
type UpdateBuilder struct {
	update expression.UpdateBuilder
}

// NewUpdateBuilder creates a new zero-value UpdateBuilder.
func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{}
}

// Delete adds an expression.UpdateBuilder.Delete expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Delete(name string, value interface{}) *UpdateBuilder {
	b.update = b.update.Delete(expression.Name(name), expression.Value(value))

	return b
}

// Set adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Set(name string, value interface{}) *UpdateBuilder {
	b.update = b.update.Set(expression.Name(name), expression.Value(value))

	return b
}

// SetOrRemove adds either Set or Remove action to the update expression.
//
// If set is true, a SET action will be added.
// If set is false, only when remove is true then a REMOVE action will be added.
//
// | set   | remove | action
// | true  | *      | SET
// | false | true   | REMOVE
// | false | false  | no-op
//
// This is useful for distinguishing between PUT/POST (remove=true) that replaces attributes with clobbering behaviour
// vs. PATCH (remove=false) that will only update attributes that are non-nil. An example is given:
//
//	func PUT(body Request) {
//		// because it's a PUT request, if notes is empty, instead of writing empty string to database, we'll remove it.
//		update = SetOrRemove(expression.UpdateBuilder{}, true, true, "notes", body.Notes)
//	}
//
//	func PATCH(body Request) {
//		// only when notes is non-empty that we'll update it. an empty notes just means caller didn't try to update it.
//		update = SetOrRemove(expression.UpdateBuilder{}, body.Notes != "", false, "notes", body.Notes)
//	}
//
//	func Update(method string, body Request) {
//		// an attempt to unify the methods may look like this.
//		update = SetOrRemove(expression.UpdateBuilder{}, body.Notes != "", method != "PATCH", "notes", body.Notes)
//	}
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) SetOrRemove(set, remove bool, name string, value interface{}) *UpdateBuilder {
	if set {
		b.update = b.update.Set(expression.Name(name), expression.Value(value))
	}

	if remove {
		b.update = b.update.Remove(expression.Name(name))
	}

	return b
}

// Remove adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Remove(name string) *UpdateBuilder {
	b.update = b.update.Remove(expression.Name(name))

	return b
}
