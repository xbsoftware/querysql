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
	Type      string        `json:"type"`
	Predicate string        `json:"predicate"`
	Filter    string        `json:"filter"`
	Value     interface{}   `json:"value"`
	Includes  []interface{} `json:"includes"`
	Alias     string        `json:"alias"`
	Rules     []Filter      `json:"rules"`
}

func getValues(i interface{}) []interface{} {
	switch values := i.(type) {
	case map[string]interface{}:
		return []interface{}{values["start"], values["end"]}
	case []interface{}:
		return values
	default:
		return []interface{}{i}
	}
}

type CustomOperation func(string, string, []interface{}) (string, []interface{}, error)
type CustomPredicate func(string, string) (string, error)

type CheckFunction = func(string) bool
type SQLConfig struct {
	WhitelistFunc CheckFunction
	Whitelist     map[string]bool
	Operations    map[string]CustomOperation
	Predicates    map[string]CustomPredicate
	Aliases       map[string]interface{}
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

func GetSQL(data Filter, config *SQLConfig, dbArr ...DBDriver) (string, []interface{}, error) {
	var db DBDriver
	if len(dbArr) > 0 {
		db = dbArr[0]
	} else {
		db = MySQL{}
	}

	if data.Rules == nil {
		if data.Field == "" {
			return "", make([]interface{}, 0), nil
		}

		if !checkWhitelist(data.Field, config) {
			return "", nil, fmt.Errorf("field name is not in whitelist: %s", data.Field)
		}

		name, isDynamicField := db.IsJSON(data.Field)

		var err error
		values := getValues(data.Value)

		if config != nil && config.Aliases != nil && data.Alias != "" {
			if alias, ok := config.Aliases[data.Alias]; ok {
				aliases := getValues(alias)
				if len(data.Includes) > 0 {
					return inSQL(name, aliases, db)
				} else {
					values = aliases
				}
			}
		}

		if len(data.Includes) > 0 {
			return inSQL(name, data.Includes, db)
		}

		if config != nil && config.Predicates != nil {
			if pr, prOk := config.Predicates[data.Predicate]; prOk {
				name, err = pr(name, data.Predicate)
				if err != nil {
					return "", NoValues, err
				}
			} else {
				return "", NoValues, fmt.Errorf("unknown predicate: %s", data.Predicate)
			}
		}

		switch data.Filter {
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
			if op, opOk := config.Operations[data.Filter]; opOk {
				return op(name, data.Filter, values)
			}
		}

		return "", NoValues, fmt.Errorf("unknown operation: %s", data.Filter)
	}

	out := make([]string, 0, len(data.Rules))
	values := make([]interface{}, 0)

	for _, r := range data.Rules {
		subSql, subValues, err := GetSQL(r, config, db)
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
	if len(data.Rules) > 1 {
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

	if config.Whitelist != nil && name != "" {
		if config.Whitelist[name] {
			return true
		}
	}

	if config.WhitelistFunc != nil {
		return config.WhitelistFunc(name)
	}

	return false

}
