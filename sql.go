package querysql

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Filter struct {
	Glue      string        `json:"glue"`
	Field     string        `json:"field"`
	Condition Condition     `json:"condition"`
	Includes  []interface{} `json:"includes"`
	Kids      []Filter      `json:"data"`
}

type Condition struct {
	Rule  string      `json:"rule"`
	Value interface{} `json:"value"`
}

type CustomOperation func(string, Condition) (string, []interface{}, error)

type SQLConfig struct {
	Whitelist  map[string]bool
	Operations map[string]CustomOperation
}

func FromJSON(text []byte) (Filter, error) {
	f := Filter{}
	err := json.Unmarshal(text, &f)
	return f, err
}

var NoValues = make([]interface{}, 0)

func inSQL(field string, data []interface{}) (string, []interface{}, error) {
	marks := make([]string, len(data))
	for i := range marks {
		marks[i] = "?"
	}

	sql := fmt.Sprintf("%s IN(%s)", field, strings.Join(marks, ","))
	return sql, data, nil
}

func GetSQL(data Filter, config *SQLConfig) (string, []interface{}, error) {
	if data.Kids == nil {
		if config != nil && config.Whitelist != nil && !config.Whitelist[data.Field] {
			return "", nil, fmt.Errorf("field name is not in whitelist: %s", data.Field)
		}

		if len(data.Includes) > 0 {
			return inSQL(data.Field, data.Includes)
		}

		switch data.Condition.Rule {
		case "":
			return "", NoValues, nil
		case "equal":
			return fmt.Sprintf("%s = ?", data.Field), []interface{}{data.Condition.Value}, nil
		case "less":
			return fmt.Sprintf("%s < ?", data.Field), []interface{}{data.Condition.Value}, nil
		case "greater":
			return fmt.Sprintf("%s > ?", data.Field), []interface{}{data.Condition.Value}, nil
		}

		if config.Operations != nil {
			op, opOk := config.Operations[data.Condition.Rule]
			if opOk {
				return op(data.Field, data.Condition)
			}
		}

		return "", NoValues, fmt.Errorf("unknown operation: %s", data.Condition.Rule)
	}

	out := make([]string, 0, len(data.Kids))
	values := make([]interface{}, 0)

	for _, r := range data.Kids {
		subSql, subValues, err := GetSQL(r, config)
		if err != nil {
			return "", nil, err
		}
		out = append(out, subSql)
		values = append(values, subValues...)
	}

	var glue string
	if data.Glue == "or" {
		glue = " OR "
	} else {
		glue = " AND "
	}

	outStr := strings.Join(out, glue)
	if len(data.Kids) > 1 {
		outStr = "( " + outStr + " )"
	}

	return outStr, values, nil
}
