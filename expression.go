package ddbfns

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

// And adds an expression.And to the condition expression.
func (o *DeleteOpts) And(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *DeleteOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.And(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.And(other[0])
	default:
		o.condition = right.And(other[0], other[1:]...)
	}
	return o
}

// Or adds an expression.And to the condition expression.
func (o *DeleteOpts) Or(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *DeleteOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.Or(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.Or(other[0])
	default:
		o.condition = right.Or(other[0], other[1:]...)
	}
	return o
}

// And adds an expression.And to the condition expression.
func (o *PutOpts) And(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *PutOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.And(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.And(other[0])
	default:
		o.condition = right.And(other[0], other[1:]...)
	}
	return o
}

// Or adds an expression.And to the condition expression.
func (o *PutOpts) Or(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *PutOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.Or(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.Or(other[0])
	default:
		o.condition = right.Or(other[0], other[1:]...)
	}
	return o
}

// And adds an expression.And to the condition expression.
func (o *UpdateOpts) And(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *UpdateOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.And(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.And(other[0])
	default:
		o.condition = right.And(other[0], other[1:]...)
	}
	return o
}

// Or adds an expression.And to the condition expression.
func (o *UpdateOpts) Or(right expression.ConditionBuilder, other ...expression.ConditionBuilder) *UpdateOpts {
	if o.condition.IsSet() {
		o.condition = o.condition.Or(right, other...)
		return o
	}

	switch len(other) {
	case 0:
		o.condition = right
	case 1:
		o.condition = right.Or(other[0])
	default:
		o.condition = right.Or(other[0], other[1:]...)
	}
	return o
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

// Build returns the underlying expression.UpdateBuilder.
func (b *UpdateBuilder) Build() expression.UpdateBuilder {
	return b.update
}

// Add adds an expression.UpdateBuilder.Add expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Add(name string, value interface{}) *UpdateBuilder {
	b.update = b.update.Add(expression.Name(name), expression.Value(value))

	return b
}

// Add adds an expression.UpdateBuilder.Add expression.
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) Add(name string, value interface{}) *UpdateOpts {
	o.update = o.update.Add(expression.Name(name), expression.Value(value))

	return o
}

// Delete adds an expression.UpdateBuilder.Delete expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Delete(name string, value interface{}) *UpdateBuilder {
	b.update = b.update.Delete(expression.Name(name), expression.Value(value))

	return b
}

// Delete adds an expression.UpdateBuilder.Delete expression.
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) Delete(name string, value interface{}) *UpdateOpts {
	o.update = o.update.Delete(expression.Name(name), expression.Value(value))

	return o
}

// Set adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Set(name string, value interface{}) *UpdateBuilder {
	b.update = b.update.Set(expression.Name(name), expression.Value(value))

	return b
}

// Set adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) Set(name string, value interface{}) *UpdateOpts {
	o.update = o.update.Set(expression.Name(name), expression.Value(value))

	return o
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
		return b
	}

	if remove {
		b.update = b.update.Remove(expression.Name(name))
	}

	return b
}

// SetOrRemove is the UpdateOpts equivalent of [UpdateBuilder.SetOrRemove].
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) SetOrRemove(set, remove bool, name string, value interface{}) *UpdateOpts {
	if set {
		o.update = o.update.Set(expression.Name(name), expression.Value(value))
		return o
	}

	if remove {
		o.update = o.update.Remove(expression.Name(name))
	}

	return o
}

// SetOrRemoveStringPointer is a variant of SetOrRemove that creates a REMOVE if the pointer is nil, a SET otherwise.
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) SetOrRemoveStringPointer(name string, value *string) *UpdateOpts {
	if value != nil {
		o.update = o.update.Set(expression.Name(name), expression.Value(*value))
		return o
	}

	o.update = o.update.Remove(expression.Name(name))
	return o
}

// Remove adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateBuilder methods, the name and value will be wrapped with an `expression.Name` and
// `expression.Value`.
func (b *UpdateBuilder) Remove(name string) *UpdateBuilder {
	b.update = b.update.Remove(expression.Name(name))

	return b
}

// Remove adds an expression.UpdateBuilder.Set expression.
//
// Like all other UpdateOpts methods to modify the update expression, the name and value will be wrapped with an
// `expression.Name` and `expression.Value`.
func (o *UpdateOpts) Remove(name string) *UpdateOpts {
	o.update = o.update.Remove(expression.Name(name))

	return o
}
