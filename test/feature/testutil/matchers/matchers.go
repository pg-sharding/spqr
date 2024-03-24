package matchers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var registry map[string]Matcher

// Matcher is function that matches two string values in some way
// It returns nil if arguments match or error with mismatch description otherwise
type Matcher func(actual string, expected string) error

// MatcherError is an Error instance describing mismatch
type MatcherError struct {
	actual   string
	expected string
}

func (me MatcherError) Error() string {
	if strings.Contains(me.actual, "\n") || strings.Contains(me.expected, "\n") {
		return fmt.Sprintf("actual value doesn't match expected\nExpected:\n%s\nBut was:\n%s\n", me.expected, me.actual)
	}
	return fmt.Sprintf("actual value '%s' doesn't match expected '%s'", me.actual, me.expected)
}

// JSONMatcherError is MatchError with mismatch path details
type JSONMatcherError struct {
	MatcherError
	path []string
}

func (jme JSONMatcherError) Error() string {
	return fmt.Sprintf("actual value doesn't match expected at path '%s'\nExpected:\n%s\nBut was:\n%s\n", strings.Join(jme.path, "."), jme.expected, jme.actual)
}

// RegexpMatcher matches actual value against regular expression
func RegexpMatcher(actual string, expected string) error {
	if regexp.MustCompile(expected).Find([]byte(actual)) == nil {
		return &MatcherError{actual, expected}
	}
	return nil
}

// nolint: gocyclo
func jsonContains(a, e interface{}, path []string) []string {
	av := reflect.ValueOf(a)
	ev := reflect.ValueOf(e)
	if (a == nil && e != nil) || (a != nil && e == nil) {
		return path
	}
	if a == nil && e == nil {
		return nil
	}
	if av.Kind() != ev.Kind() {
		return path
	}
	switch ev.Kind() {
	case reflect.Map:
		em, ok := e.(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("unexpected expected JSON datatype %s at path %s", reflect.TypeOf(e), path))
		}
		am, ok := a.(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("unexpected actual JSON datatype %s at path %s", reflect.TypeOf(a), path))
		}
		for k, v1 := range em {
			v2, ok := am[k]
			if !ok {
				return append(path, k)
			}
			res := jsonContains(v2, v1, append(path, k))
			if len(res) != 0 {
				return res
			}
		}
	case reflect.Slice:
		es, ok := e.([]interface{})
		if !ok {
			panic(fmt.Sprintf("unexpected expected JSON datatype %s at path %s", reflect.TypeOf(e), path))
		}
		as, ok := a.([]interface{})
		if !ok {
			panic(fmt.Sprintf("unexpected actual JSON datatype %s at path %s", reflect.TypeOf(a), path))
		}
		j := 0
		for i, v1 := range es {
			for ; j < len(as); j++ {
				v2 := as[j]
				res := jsonContains(v2, v1, append(path, strconv.Itoa(j)))
				if len(res) == 0 {
					break
				}
			}
			if j == len(as) {
				return append(path, strconv.Itoa(i))
			}
		}
	case reflect.Bool:
		if a.(bool) != e.(bool) {
			return path
		}
	case reflect.Float64:
		// assume int math
		if a.(float64) != e.(float64) {
			return path
		}
	case reflect.String:
		if a.(string) != e.(string) {
			return path
		}
	default:
		panic(fmt.Sprintf("unexpected JSON datatype %s at path %s", reflect.TypeOf(e), path))
	}
	return nil
}

// JSONMatcher checks that actual JSON value deeply contains expected value
// In other words actual value should be superset of expected
// See tests for more examples
func JSONMatcher(actual string, expected string) error {
	var a, e interface{}
	if err := json.Unmarshal([]byte(actual), &a); err != nil {
		return fmt.Errorf("actual value is not valid json: %s", err)
	}
	if err := json.Unmarshal([]byte(expected), &e); err != nil {
		panic(fmt.Errorf("expected value is not valid json: %s", err))
	}
	res := jsonContains(a, e, []string{""})
	if len(res) > 0 {
		return &JSONMatcherError{MatcherError{actual, expected}, res}
	}
	return nil
}

// JSONExactlyMatcher checks that actual and expected JSON represents
// exactly the same data structure
func JSONExactlyMatcher(actual string, expected string) error {
	var a, e interface{}
	if err := json.Unmarshal([]byte(actual), &a); err != nil {
		return fmt.Errorf("actual value is not valid json: %s", err)
	}
	if err := json.Unmarshal([]byte(expected), &e); err != nil {
		panic(fmt.Errorf("expected value is not valid json: %s", err))
	}
	if !reflect.DeepEqual(a, e) {
		return &MatcherError{actual, expected}
	}
	return nil
}

// GetMatcher returns registered matcher by name
func GetMatcher(name string) (Matcher, error) {
	if matcher, ok := registry[name]; ok {
		return matcher, nil
	}
	return nil, fmt.Errorf("no such matcher: %s", name)
}

// RegisterMatcher registers new matcher with given name.
// Should be typically called in init() function.
func RegisterMatcher(name string, matcher Matcher) {
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("matcher %s already exists", name))
	}
	registry[name] = matcher
}

func init() {
	registry = make(map[string]Matcher)
	RegisterMatcher("regexp", RegexpMatcher)
	RegisterMatcher("json", JSONMatcher)
	RegisterMatcher("json_exactly", JSONExactlyMatcher)
}
