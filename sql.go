package querysql

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DB_MYSQL DatabaseType = iota
	DB_POSTGRESQL
)

type DatabaseType int

var DB DatabaseType = DB_MYSQL

type Filter struct {
	Glue      string        `json:"glue"`
	Field     string        `json:"field"`
	Condition Condition     `json:"condition"`
	Includes  []interface{} `json:"includes"`
	Kids      []Filter      `json:"rules"`
}

type CustomOperation func(string, string, []interface{}) (string, []interface{}, error)

type SQLConfig struct {
	Whitelist         map[string]bool
	Operations        map[string]CustomOperation
	DynamicFields     []DynamicField
	DynamicConfigName string
}

type DynamicField struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

func FromJSON(text []byte) (Filter, error) {
	f := Filter{}
	err := json.Unmarshal(text, &f)

	return f, err
}

var NoValues = make([]interface{}, 0)

func inSQL(field string, data []interface{}, placeholder string) (string, []interface{}, error) {
	marks := make([]string, len(data))
	for i := range marks {
		marks[i] = placeholder
	}

	sql := fmt.Sprintf("%s IN(%s)", field, strings.Join(marks, ","))
	return sql, data, nil
}

func GetSQL(data Filter, config *SQLConfig) (string, []interface{}, error) {
	if data.Kids == nil {
		if config != nil && config.Whitelist != nil && !config.Whitelist[data.Field] {
			return "", nil, fmt.Errorf("field name is not in whitelist: %s", data.Field)
		}

		ph, err := getPlaceholder()
		if err != nil {
			return "", nil, err
		}

		var isDynamicField bool
		if DB == DB_POSTGRESQL {
			f := getDynamicField(config.DynamicFields, data.Field)
			if f != nil {
				if config.DynamicConfigName == "" {
					return "", nil, fmt.Errorf("dynamic config name is empty")
				}
				parts := strings.Split(data.Field, ".")
				tp := GetJSONBType(f.Type)
				if tp == "date" {
					if len(parts) == 1 {
						data.Field = fmt.Sprintf("CAST((%s->'%s')::text AS DATE)", config.DynamicConfigName, parts[0])
					} else if len(parts) == 2 {
						data.Field = fmt.Sprintf("CAST((\"%s\".%s->'%s')::text AS DATE)", parts[0], config.DynamicConfigName, parts[1])
					}
				} else {
					if len(parts) == 1 {
						data.Field = fmt.Sprintf("(%s->'%s')::%s", config.DynamicConfigName, parts[0], tp)
					} else if len(parts) == 2 {
						data.Field = fmt.Sprintf("(\"%s\".%s->'%s')::%s", parts[0], config.DynamicConfigName, parts[1], tp)
					}
				}
				isDynamicField = true
			}
		}

		if len(data.Includes) > 0 {
			return inSQL(data.Field, data.Includes, ph)
		}

		values := data.Condition.getValues()
		switch data.Condition.Rule {
		case "":
			return "", NoValues, nil
		case "equal":
			return fmt.Sprintf("%s = %s", data.Field, ph), values, nil
		case "notEqual":
			return fmt.Sprintf("%s <> %s", data.Field, ph), values, nil
		case "contains":
			switch DB {
			case DB_MYSQL:
				return fmt.Sprintf("INSTR(%s, ?) > 0", data.Field), values, nil
			case DB_POSTGRESQL:
				if isDynamicField {
					return fmt.Sprintf("%s LIKE '\"%%' ||  $  || '%%\"'", data.Field), values, nil
				}
				return fmt.Sprintf("%s LIKE '%%' ||  $  || '%%'", data.Field), values, nil
			}
		case "notContains":
			switch DB {
			case DB_MYSQL:
				return fmt.Sprintf("INSTR(%s, ?) = 0", data.Field), values, nil
			case DB_POSTGRESQL:
				if isDynamicField {
					return fmt.Sprintf("%s NOT LIKE '\"%%' ||  $  || '%%\"'", data.Field), values, nil
				}
				return fmt.Sprintf("%s NOT LIKE '%%' ||  $  || '%%'", data.Field), values, nil
			}
		case "lessOrEqual":
			return fmt.Sprintf("%s <= %s", data.Field, ph), values, nil
		case "greaterOrEqual":
			return fmt.Sprintf("%s >= %s", data.Field, ph), values, nil
		case "less":
			return fmt.Sprintf("%s < %s", data.Field, ph), values, nil
		case "notBetween":
			if len(values) != 2 {
				return "", nil, fmt.Errorf("wrong number of parameters for notBetween operation: %d", len(values))
			}

			if values[0] == nil {
				return fmt.Sprintf("%s > %s", data.Field, ph), values[1:], nil
			} else if values[1] == nil {
				return fmt.Sprintf("%s < %s", data.Field, ph), values[:1], nil
			} else {
				return fmt.Sprintf("( %s < %s OR %s > %s )", data.Field, ph, data.Field, ph), values, nil
			}
		case "between":
			if len(values) != 2 {
				return "", nil, fmt.Errorf("wrong number of parameters for notBetween operation: %d", len(values))
			}

			if values[0] == nil {
				return fmt.Sprintf("%s < %s", data.Field, ph), values[1:], nil
			} else if values[1] == nil {
				return fmt.Sprintf("%s > %s", data.Field, ph), values[:1], nil
			} else {
				return fmt.Sprintf("( %s > %s AND %s < %s )", data.Field, ph, data.Field, ph), values, nil
			}
		case "greater":
			return fmt.Sprintf("%s > %s", data.Field, ph), values, nil
		case "beginsWith":
			var search string
			switch DB {
			case DB_MYSQL:
				search = "CONCAT(?, '%')"
			case DB_POSTGRESQL:
				if isDynamicField {
					search = "'\"' ||  $  || '%'"
				} else {
					search = " $  || '%'"
				}
			}
			return fmt.Sprintf("%s LIKE %s", data.Field, search), values, nil
		case "notBeginsWith":
			var search string
			switch DB {
			case DB_MYSQL:
				search = "CONCAT(?, '%')"
			case DB_POSTGRESQL:
				if isDynamicField {
					search = "'\"' ||  $  || '%'"
				} else {
					search = " $  || '%'"
				}
			}
			return fmt.Sprintf("%s NOT LIKE %s", data.Field, search), values, nil
		case "endsWith":
			var search string
			switch DB {
			case DB_MYSQL:
				search = "CONCAT('%', ?)"
			case DB_POSTGRESQL:
				if isDynamicField {
					search = "'%' ||  $  || '\"'"
				} else {
					search = "'%' ||  $ "
				}
			}
			return fmt.Sprintf("%s LIKE %s", data.Field, search), values, nil
		case "notEndsWith":
			var search string
			switch DB {
			case DB_MYSQL:
				search = "CONCAT('%', ?)"
			case DB_POSTGRESQL:
				if isDynamicField {
					search = "'%' ||  $ || '\"'"
				} else {
					search = "'%' ||  $ "
				}
			}
			return fmt.Sprintf("%s NOT LIKE %s", data.Field, search), values, nil
		}

		if config != nil && config.Operations != nil {
			op, opOk := config.Operations[data.Condition.Rule]
			if opOk {
				return op(data.Field, data.Condition.Rule, data.Condition.getValues())
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

	if DB == DB_POSTGRESQL {
		n := 1
		for strings.Contains(outStr, " $ ") {
			outStr = strings.Replace(outStr, " $ ", fmt.Sprintf("$%d", n), 1)
			n = n + 1
		}
	}

	return outStr, values, nil
}

func getPlaceholder() (string, error) {
	switch DB {
	case DB_MYSQL:
		return "?", nil
	case DB_POSTGRESQL:
		return " $ ", nil
	default:
		return "", fmt.Errorf("unknown database")
	}
}

func getDynamicField(array []DynamicField, value string) *DynamicField {
	for _, v := range array {
		if v.Key == value {
			return &v
		}
	}
	return nil
}

func GetJSONBType(t string) string {
	switch t {
	case "number":
		return "numeric"
	default:
		return t
	}
}
