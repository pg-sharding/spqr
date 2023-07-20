package matchers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegexpMatcher(t *testing.T) {
	assert.NoError(t, RegexpMatcher("qwe asd er", "a.d"), "regexp matcher should find match at any position")
	assert.NoError(t, RegexpMatcher("qwe", "qwe"), "regexp matcher should also match full string")
	assert.NoError(t, RegexpMatcher("qwe asd er", "^.*qwe.*$"), "regexp matcher should also match full string with patterns")
	assert.Error(t, RegexpMatcher("qwe asd er", "boo"), "regexp matcher should match not match anything")
	assert.NoError(t, RegexpMatcher("1       1", "^1[[:space:]]+1$"), "regexp matcher should match [[:space:]]")
}

func TestJsonExactlyMatcher(t *testing.T) {
	var a, e string
	a = `{"a":1, "b":0.2, "c": [1,2], "z": null, "e": {"x":"y"}}`
	e = `{"a":1, "c": [1,2], "b":0.2,    "z": null, "e": {"x":   "y"}}`
	assert.NoError(t, JSONExactlyMatcher(a, e), "exact json matcher should match jsons despite key orders and spaces")
	a = `{"a":2, "b":0.2, "c": [1,2], "d": null, "e": {"x":"y"}}`
	assert.Error(t, JSONExactlyMatcher(a, e), "exact json matcher should not match if value changes")
}

func TestJSONMatcher(t *testing.T) {
	var a, e string
	a = `{"a":1, "b":0.2, "c": [1,2], "d": null, "e": {"x":"y"}}`
	e = `{"a":1, "c": [1,2], "b":0.2,    "d": null, "e": {"x":   "y"}}`
	assert.NoError(t, JSONMatcher(a, e), "json matcher should match jsons despite key orders and spaces")
	e = `{"a":1, "d": null}`
	assert.NoError(t, JSONMatcher(a, e), "json matcher should ignore extra keys")
	a = `
	{
		"a":1,
		"e": {
			"a": 1,
			"e": {
				"a": 1,
				"ok": true
			}
		}
	}
	`
	e = `{"e":{"e":{"ok":true}}}`
	assert.NoError(t, JSONMatcher(a, e), "json matcher should match deep nested jsons")
	e = `{"e":{"e":{"res":"ok"}}}`
	assert.Error(t, JSONMatcher(a, e), "json matcher should not match if field is missing")
	e = `{"e":{"e":{"ok":1}}}`
	assert.Error(t, JSONMatcher(a, e), "json matcher should not match if field type is different")
	e = `{"e":{"e":{"ok":false}}}`
	assert.Error(t, JSONMatcher(a, e), "json matcher should not match if field value is different")
	a = `
	{
		"e": [
			{"a": 1},
			{"b": 2},
			{"c": 3},
			{"d": 4},
			{"e": 5}
		]
	}
	`
	e = `
	{
		"e": [
			{"b": 2},
			{"d": 4}
		]
	}
	`
	assert.NoError(t, JSONMatcher(a, e), "json matcher should match parts of arrays, preserving order")
	e = `
	{
		"e": [
			{"d": 4},
			{"b": 2}
		]
	}
	`
	assert.Error(t, JSONMatcher(a, e), "json matcher should not match parts of arrays, if order differs")
}
