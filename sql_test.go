package querysql

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var aAndB = `{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1}}, { "field": "b", "condition":{ "type":"greater", "filter":"abc" }}]}`
var aOrB = `{ "glue":"or", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1}}, { "field": "b", "condition":{ "type":"greater", "filter":"abc" }}]}`
var cOrC = `{ "glue":"or", "rules":[{ "field": "a", "condition":{ "type":"is null" }}, { "field": "b", "condition":{ "type":"range100", "filter":500 }}]}`

var cases = [][]string{
	[]string{`{}`, "", ""},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"equal", "filter":1 }}]}`,
		"a = ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notEqual", "filter":1 }}]}`,
		"a <> ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1 }}]}`,
		"a < ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"lessOrEqual", "filter":1 }}]}`,
		"a <= ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"greater", "filter":1 }}]}`,
		"a > ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"greaterOrEqual", "filter":1 }}]}`,
		"a >= ?",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"contains", "filter":1 }}]}`,
		"INSTR(a, ?) > 0",
		"1",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notContains", "filter":1 }}]}`,
		"INSTR(a, ?) < 0",
		"1",
	},
	[]string{
		aAndB,
		"( a < ? AND b > ? )",
		"1,abc",
	},
	[]string{
		aOrB,
		"( a < ? OR b > ? )",
		"1,abc",
	},
	[]string{
		`{ "glue":"AND", "rules":[` + aAndB + `,` + aOrB + `,{ "field":"c", "condition": { "type":"equal", "filter":3 } }]}`,
		"( ( a < ? AND b > ? ) AND ( a < ? OR b > ? ) AND c = ? )",
		"1,abc,1,abc,3",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "includes":[1,2,3]}]}`,
		"a IN(?,?,?)",
		"1,2,3",
	},
	[]string{
		`{ "glue":"and", "rules":[{ "field": "a", "includes":["a","b","c"]}]}`,
		"a IN(?,?,?)",
		"a,b,c",
	},
}


func anyToStringArray(some []interface{}) (string, error) {
	out := make([]string, 0, len(some))
	for _, x := range some {
		str, strOk := x.(string)
		if strOk {
			out = append(out, str)
			continue
		}

		num, numOk := x.(float64)
		if numOk {
			out = append(out, strconv.Itoa(int(num)))
			continue
		}

		return "", fmt.Errorf("can't convert %+v to a string", x)
	}

	return strings.Join(out, ","), nil
}

func TestSQL(t *testing.T) {
	for _, line := range cases {
		format, err := FromJSON([]byte(line[0]))
		if err != nil {
			t.Errorf("can't parse json\nj: %s\n%f", line[0], err)
			continue
		}

		sql, vals, err := GetSQL(format, nil)
		if err != nil {
			t.Errorf("can't generate sql\nj: %s\n%f", line[0], err)
			continue
		}
		if sql != line[1] {
			t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", line[0], line[1], sql)
			continue
		}

		valsStr, err := anyToStringArray(vals)
		if err != nil {
			t.Errorf("can't convert parameters\nj: %s\n%f", line[0], err)
			continue
		}

		if valsStr != line[2] {
			t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", line[0], line[2], valsStr)
			continue
		}
	}
}

func TestWhitelist(t *testing.T) {
	format, err := FromJSON([]byte(aAndB))
	if err != nil {
		t.Errorf("can't parse json\nj: %s\n%f", aAndB, err)
		return
	}

	_, _, err = GetSQL(format, nil)
	if err != nil {
		t.Errorf("doesn't work without config")
		return
	}
	_, _, err = GetSQL(format, &SQLConfig{})
	if err != nil {
		t.Errorf("doesn't work without whitelist")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{Whitelist: map[string]bool{"a": true, "b": true}})
	if err != nil {
		t.Errorf("doesn't work with fields allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{Whitelist: map[string]bool{"a": true}})
	if err == nil {
		t.Errorf("doesn't return error when field is not allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{Whitelist: map[string]bool{"b": true}})
	if err == nil {
		t.Errorf("doesn't return error when field is not allowed")
		return
	}
}

func TestCustomOperation(t *testing.T) {
	format, err := FromJSON([]byte(cOrC))
	if err != nil {
		t.Errorf("can't parse json\nj: %s\n%f", aAndB, err)
		return
	}

	sql, vals, err := GetSQL(format, &SQLConfig{
		Operations: map[string]CustomOperation{
			"is null" : func(n string, c Condition) (string, []interface{}, error) {
				return fmt.Sprintf("%s IS NULL", n), NoValues, nil
			},
			"range100" : func(n string, c Condition) (string, []interface{}, error) {
				return fmt.Sprintf("( %s > ? AND %s < ? + 100 )", n, n), []interface{}{c.Value, c.Value}, nil
			},
		},
	})

	if err != nil {
		t.Errorf("can't generate sql: %s\n%f", cOrC, err)
		return
	}

	check := "( a IS NULL OR ( b > ? AND b < ? + 100 ) )"
	if sql != check {
		t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", cOrC, check, sql)
		return
	}

		valsStr, err := anyToStringArray(vals)
		if err != nil {
			t.Errorf("can't convert parameters\nj: %s\n%f", cOrC, err)
			return
		}

		check = "500,500"
		if valsStr != check {
			t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", cOrC, check, valsStr)
			return
		}
}