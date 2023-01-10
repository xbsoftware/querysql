package querysql

import (
	"encoding/json"
	"fmt"
	"strings"
)

type DBDriver interface {
	Mark() string
	IsJSON(name string) (string, bool)

	Contains(v string, isJSON bool) string
	NotContains(v string, isJSON bool) string
	BeginsWith(v string, isJSON bool) string
	NotBeginsWith(v string, isJSON bool) string
	EndsWith(v string, isJSON bool) string
	NotEndsWith(v string, isJSON bool) string
}

type Filter struct {
	Glue      string        `json:"glue"`
	Field     string        `json:"field"`
	Condition Condition     `json:"condition"`
	Includes  []interface{} `json:"includes"`
	Alias     string        `json:"alias"`
	Kids      []Filter      `json:"rules"`
}

type CustomOperation func(string, string, []interface{}) (string, []interface{}, error)

type CheckFunction = func(string) bool
type SQLConfig struct {
	WhitelistFunc CheckFunction
	Whitelist     map[string]bool
	Operations    map[string]CustomOperation
}

func FromJSON(text []byte) (Filter, error) {
	f := Filter{}
	err := json.Unmarshal(text, &f)

	return f, err
}

var NoValues = make([]interface{}, 0)

func inSQL(field string, data []interface{}, db DBDriver) (string, []interface{}, error) {
	marks := make([]string, len(data))
	for i := range marks {
		marks[i] = db.Mark()
	}

	sql := fmt.Sprintf("%s IN(%s)", field, strings.Join(marks, ","))
	return sql, data, nil
}

func GetSQL(data Filter, aliases map[string]interface{}, config *SQLConfig, dbArr ...DBDriver) (string, []interface{}, error) {
	var db DBDriver
	if len(dbArr) > 0 {
		db = dbArr[0]
	} else {
		db = MySQL{}
	}

	if data.Kids == nil {
		if data.Field == "" {
			return "", make([]interface{}, 0), nil
		}

		if !checkWhitelist(data.Field, config) {
			return "", nil, fmt.Errorf("field name is not in whitelist: %s", data.Field)
		}

		name, isDynamicField := db.IsJSON(data.Field)

		var values []interface{}
		var includes bool
		if data.Alias != "" && len(aliases) > 0 {
			aliasValue, ok := aliases[data.Alias]
			if ok {
				// get alias values
				values, includes = aliasValue.([]interface{})
				if !includes {
					values = getValues(aliasValue)
				}
			}
		}

		if values == nil {
			// get filter values
			if len(data.Includes) > 0 {
				includes = true
				values = data.Includes
			} else {
				values = data.Condition.getValues()
			}
		}

		if includes {
			return inSQL(name, values, db)
		}

		switch data.Condition.Rule {
		case "":
			return "", NoValues, nil
		case "equal":
			return fmt.Sprintf("%s = %s", name, db.Mark()), values, nil
		case "notEqual":
			return fmt.Sprintf("%s <> %s", name, db.Mark()), values, nil
		case "contains":
			return db.Contains(name, isDynamicField), values, nil
		case "notContains":
			return db.NotContains(name, isDynamicField), values, nil
		case "lessOrEqual":
			return fmt.Sprintf("%s <= %s", name, db.Mark()), values, nil
		case "greaterOrEqual":
			return fmt.Sprintf("%s >= %s", name, db.Mark()), values, nil
		case "less":
			return fmt.Sprintf("%s < %s", name, db.Mark()), values, nil
		case "notBetween":
			if len(values) != 2 {
				return "", nil, fmt.Errorf("wrong number of parameters for notBetween operation: %d", len(values))
			}

			if values[0] == nil {
				return fmt.Sprintf("%s > %s", name, db.Mark()), values[1:], nil
			} else if values[1] == nil {
				return fmt.Sprintf("%s < %s", name, db.Mark()), values[:1], nil
			} else {
				return fmt.Sprintf("( %s < %s OR %s > %s )", name, db.Mark(), name, db.Mark()), values, nil
			}
		case "between":
			if len(values) != 2 {
				return "", nil, fmt.Errorf("wrong number of parameters for notBetween operation: %d", len(values))
			}

			if values[0] == nil {
				return fmt.Sprintf("%s < %s", name, db.Mark()), values[1:], nil
			} else if values[1] == nil {
				return fmt.Sprintf("%s > %s", name, db.Mark()), values[:1], nil
			} else {
				return fmt.Sprintf("( %s > %s AND %s < %s )", name, db.Mark(), name, db.Mark()), values, nil
			}
		case "greater":
			return fmt.Sprintf("%s > %s", name, db.Mark()), values, nil
		case "beginsWith":
			return db.BeginsWith(name, isDynamicField), values, nil
		case "notBeginsWith":
			return db.NotBeginsWith(name, isDynamicField), values, nil
		case "endsWith":
			return db.EndsWith(name, isDynamicField), values, nil
		case "notEndsWith":
			return db.NotEndsWith(name, isDynamicField), values, nil
		}

		if config != nil && config.Operations != nil {
			op, opOk := config.Operations[data.Condition.Rule]
			if opOk {
				return op(name, data.Condition.Rule, data.Condition.getValues())
			}
		}

		return "", NoValues, fmt.Errorf("unknown operation: %s", data.Condition.Rule)
	}

	out := make([]string, 0, len(data.Kids))
	values := make([]interface{}, 0)

	for _, r := range data.Kids {
		subSql, subValues, err := GetSQL(r, aliases, config, db)
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

func checkWhitelist(name string, config *SQLConfig) bool {
	if config == nil {
		return true
	}
	if config.Whitelist == nil && config.WhitelistFunc == nil {
		return true
	}

	if config.Whitelist != nil {
		if config.Whitelist[name] {
			return true
		}
	}

	if config.WhitelistFunc != nil {
		return config.WhitelistFunc(name)
	}

	return false

}
