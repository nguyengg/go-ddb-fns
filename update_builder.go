package ddbfns

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

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
		return b
	}

	if remove {
		b.update = b.update.Remove(expression.Name(name))
	}

	return b
}

// SetOrRemoveStringPointer is a specialization of SetOrRemove for string pointer value.
//
// If ptr is a nil pointer, no action is taken. If ptr dereferences to an empty string, a REMOVE action is used.
// A non-empty string otherwise will result in a SET action.
//
// Like all other UpdateBuilder methods, the name will be wrapped with an `expression.Name` and dereferenced value
// `expression.Value`.
func (b *UpdateBuilder) SetOrRemoveStringPointer(name string, ptr *string) *UpdateBuilder {
	if ptr == nil {
		return b
	}

	if v := *ptr; v != "" {
		b.update = b.update.Set(expression.Name(name), expression.Value(v))
		return b
	}

	b.update = b.update.Remove(expression.Name(name))
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
