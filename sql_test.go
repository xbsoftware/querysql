package querysql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var aAndB = `{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1}}, { "field": "b", "condition":{ "type":"greater", "filter":"abc" }}]}`
var aOrB = `{ "glue":"or", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1}}, { "field": "b", "condition":{ "type":"greater", "filter":"abc" }}]}`
var cOrC = `{ "glue":"or", "rules":[{ "field": "a", "condition":{ "type":"is null" }}, { "field": "b", "condition":{ "type":"range100", "filter":500 }}]}`
var JSONaAndB = `{ "glue":"and", "rules":[{ "field": "json:cfg.a", "condition":{ "type":"less", "filter":1}}, { "field": "json:cfg.b", "condition":{ "type":"greater", "filter":"abc" }}]}`

var cases = [][]string{
	{`{}`, "", "", ""},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"equal", "filter":1 }}]}`,
		"a = ?",
		"a = $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notEqual", "filter":1 }}]}`,
		"a <> ?",
		"a <> $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1 }}]}`,
		"a < ?",
		"a < $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"lessOrEqual", "filter":1 }}]}`,
		"a <= ?",
		"a <= $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"greater", "filter":1 }}]}`,
		"a > ?",
		"a > $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"greaterOrEqual", "filter":1 }}]}`,
		"a >= ?",
		"a >= $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"contains", "filter":1 }}]}`,
		"INSTR(a, ?) > 0",
		"a LIKE '%' || $1 || '%'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notContains", "filter":1 }}]}`,
		"INSTR(a, ?) = 0",
		"a NOT LIKE '%' || $1 || '%'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"beginsWith", "filter":"1" }}]}`,
		"a LIKE CONCAT(?, '%')",
		"a LIKE $1 || '%'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notBeginsWith", "filter":"1" }}]}`,
		"a NOT LIKE CONCAT(?, '%')",
		"a NOT LIKE $1 || '%'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"endsWith", "filter":"1" }}]}`,
		"a LIKE CONCAT('%', ?)",
		"a LIKE '%' || $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notEndsWith", "filter":"1" }}]}`,
		"a NOT LIKE CONCAT('%', ?)",
		"a NOT LIKE '%' || $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"between", "filter":{ "start":1, "end":2 } }}]}`,
		"( a > ? AND a < ? )",
		"( a > $1 AND a < $2 )",
		"1,2",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"between", "filter":{ "start":1 } }}]}`,
		"a > ?",
		"a > $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"between", "filter":{ "end":2 } }}]}`,
		"a < ?",
		"a < $1",
		"2",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notBetween", "filter":{ "start":1, "end":2 } }}]}`,
		"( a < ? OR a > ? )",
		"( a < $1 OR a > $2 )",
		"1,2",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notBetween", "filter":{ "start":1 } }}]}`,
		"a < ?",
		"a < $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"notBetween", "filter":{ "end":2 } }}]}`,
		"a > ?",
		"a > $1",
		"2",
	},
	{
		aAndB,
		"( a < ? AND b > ? )",
		"( a < $1 AND b > $2 )",
		"1,abc",
	},
	{
		aOrB,
		"( a < ? OR b > ? )",
		"( a < $1 OR b > $2 )",
		"1,abc",
	},
	{
		`{ "glue":"AND", "rules":[` + aAndB + `,` + aOrB + `,{ "field":"c", "condition": { "type":"equal", "filter":3 } }]}`,
		"( ( a < ? AND b > ? ) AND ( a < ? OR b > ? ) AND c = ? )",
		"( ( a < $1 AND b > $2 ) AND ( a < $3 OR b > $4 ) AND c = $5 )",
		"1,abc,1,abc,3",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "includes":[1,2,3]}]}`,
		"a IN(?,?,?)",
		"a IN($1,$2,$3)",
		"1,2,3",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "includes":["a","b","c"]}]}`,
		"a IN(?,?,?)",
		"a IN($1,$2,$3)",
		"a,b,c",
	},
}

var psqlCases = [][]string{
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.a", "condition":{ "type":"equal", "filter":1 }}]}`,
		"(\"cfg\"->'a')::text = $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "mytable.json:cfg.a", "condition":{ "type":"equal", "filter":1 }}]}`,
		"(\"mytable\".\"cfg\"->'a')::text = $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.b:numeric", "condition":{ "type":"notEqual", "filter":1 }}]}`,
		"(\"cfg\"->'b')::numeric <> $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.b:numeric", "condition":{ "type":"less", "filter":1 }}]}`,
		"(\"cfg\"->'b')::numeric < $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.b:numeric", "condition":{ "type":"lessOrEqual", "filter":1 }}]}`,
		"(\"cfg\"->'b')::numeric <= $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.b:numeric", "condition":{ "type":"greater", "filter":1 }}]}`,
		"(\"cfg\"->'b')::numeric > $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.b:numeric", "condition":{ "type":"greaterOrEqual", "filter":1 }}]}`,
		"(\"cfg\"->'b')::numeric >= $1",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.a", "condition":{ "type":"contains", "filter":1 }}]}`,
		"(\"cfg\"->'a')::text LIKE '\"%' || $1 || '%\"'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.a", "condition":{ "type":"notContains", "filter":1 }}]}`,
		"(\"cfg\"->'a')::text NOT LIKE '\"%' || $1 || '%\"'",
		"1",
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.c:date", "condition":{ "type":"equal", "filter":"2006/01/02" }}]}`,
		"CAST((\"cfg\"->'c')::text AS DATE) = $1",
		`2006/01/02`,
	},
	{
		`{ "glue":"and", "rules":[{ "field": "json:cfg.c:date", "condition":{ "type":"notBetween", "filter":{ "start":"2006/01/02", "end":"2006/01/9" } }}]}`,
		"( CAST((\"cfg\"->'c')::text AS DATE) < $1 OR CAST((\"cfg\"->'c')::text AS DATE) > $2 )",
		`2006/01/02,2006/01/9`,
	},
}

var aliases = [][]string{
	{
		`{}`,
		`{}`,
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "condition":{ "type":"less", "filter":1 }}]}`,
		`{}`,
	},
	{
		`{ "glue":"or", "rules":[{ "field": "a", "alias": "name", "condition":{ "type":"equal", "filter":1 }}]}`,
		`{"a":"name"}`,
	},
	{
		`{ "glue":"and", "rules":[{ "field": "a", "alias": "b", "condition":{ "type":"notEqual", "filter":1 }},{ "field": "b", "alias": "a", "condition":{ "type":"equal", "filter":2 }}]}`,
		`{"a":"b","b":"a"}`,
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

		if valsStr != line[3] {
			t.Errorf("wrong sql generated (values)\nj: %s\ns: %s\nr: %s", line[0], line[3], valsStr)
			continue
		}
	}
}

func TestPostgre(t *testing.T) {
	queryConfig := SQLConfig{
		// Whitelist: map[string]bool{
		// 	"a": true,
		// 	"b": true,
		// 	"c": true,
		// }
	}
	for _, line := range cases {
		format, err := FromJSON([]byte(line[0]))
		if err != nil {
			t.Errorf("can't parse json\nj: %s\n%f", line[0], err)
			continue
		}

		sql, vals, err := GetSQL(format, &queryConfig, &PostgreSQL{})
		if err != nil {
			t.Errorf("can't generate sql\nj: %s\n%f", line[0], err)
			continue
		}
		if sql != line[2] {
			t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", line[0], line[2], sql)
			continue
		}

		valsStr, err := anyToStringArray(vals)
		if err != nil {
			t.Errorf("can't convert parameters\nj: %s\n%f", line[0], err)
			continue
		}

		if valsStr != line[3] {
			t.Errorf("wrong sql generated\nj: %s\ns: %s\nr: %s", line[0], line[3], valsStr)
			continue
		}
	}
}

func TestPostgreJSON(t *testing.T) {
	queryConfig := SQLConfig{
		// Whitelist: map[string]bool{
		// 	"a": true,
		// 	"b": true,
		// 	"c": true,
		// }
	}
	for _, line := range psqlCases {
		format, err := FromJSON([]byte(line[0]))
		if err != nil {
			t.Errorf("can't parse json\nj: %s\n%f", line[0], err)
			continue
		}

		sql, vals, err := GetSQL(format, &queryConfig, &PostgreSQL{})
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

func TestWhitelistPG(t *testing.T) {
	format, err := FromJSON([]byte(JSONaAndB))
	if err != nil {
		t.Errorf("can't parse json\nj: %s\n%f", aAndB, err)
		return
	}

	_, _, err = GetSQL(format, nil, &PostgreSQL{})
	if err != nil {
		t.Errorf("doesn't work without config")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{}, &PostgreSQL{})
	if err != nil {
		t.Errorf("doesn't work without whitelist")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{
		WhitelistFunc: func(name string) bool {
			return strings.HasPrefix(name, "json:cfg.")
		}})
	if err != nil {
		t.Errorf("doesn't work with fields allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{
		WhitelistFunc: func(name string) bool {
			return strings.HasPrefix(name, "json:cfg.a")
		},
		Whitelist: map[string]bool{"json:cfg.b": true},
	})
	if err != nil {
		t.Errorf("doesn't work with fields allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{
		Whitelist: map[string]bool{"json:cfg.a": true, "json:cfg.b": true},
	})
	if err != nil {
		t.Errorf("doesn't work with fields allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{
		Whitelist: map[string]bool{"json:cfg.b": true},
	})
	if err == nil {
		t.Errorf("doesn't return error when field is not allowed")
		return
	}

	_, _, err = GetSQL(format, &SQLConfig{
		WhitelistFunc: func(name string) bool {
			return strings.HasPrefix(name, "json:cfgx")
		},
	})
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
			"is null": func(n string, r string, values []interface{}) (string, []interface{}, error) {
				return fmt.Sprintf("%s IS NULL", n), NoValues, nil
			},
			"range100": func(n string, r string, values []interface{}) (string, []interface{}, error) {
				out := []interface{}{values[0], values[0]}
				return fmt.Sprintf("( %s > ? AND %s < ? + 100 )", n, n), out, nil
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

func TestAlias(t *testing.T) {
	queryConfig := SQLConfig{
		Aliases: make(map[string]string),
	}

	for _, line := range aliases {
		format, err := FromJSON([]byte(line[0]))
		if err != nil {
			t.Errorf("can't parse json\nj: %s\n%f", line[0], err)
			continue
		}

		_, _, err = GetSQL(format, &queryConfig)
		if err != nil {
			t.Errorf("can't generate sql\nj: %s\n%f", line[0], err)
			continue
		}

		alias, err := json.Marshal(queryConfig.Aliases)
		if err != nil {
			t.Errorf("can't parse json\nj: %s\n%f", queryConfig.Aliases, err)
			continue
		}

		if string(alias) != line[1] {
			t.Errorf("different aliases\nj: %s\ns: %s\n", alias, line[1])
			continue
		}
	}
}
