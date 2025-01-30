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
// The name and value will be wrapped with an `expression.Name` and `expression.Value` so don't bother wrapping them
// ahead of time.
func SetOrRemove(update expression.UpdateBuilder, set, remove bool, name string, value interface{}) expression.UpdateBuilder {
	if set {
		return update.Set(expression.Name(name), expression.Value(value))
	}

	if remove {
		return update.Remove(expression.Name(name))
	}

	return update
}
