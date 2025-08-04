# JSON to SQL Query

This library converts a JSON object into a SQL WHERE clause.

## Features

*   Converts JSON to SQL.
*   Supports nested conditions.
*   Supports multiple database dialects (MySQL and PostgreSQL).

## Installation

```sh
go get github.com/xbsoftware/querysql@v2.0.0
```

## V2 API

### `GetSQL`

```go
func GetSQL(data Filter, config *SQLConfig, dbArr ...DBDriver) (string, []interface{}, error)
```

-   `data`: The `Filter` object, which can be created from JSON using `FromJSON`.
-   `config`: An optional `SQLConfig` for advanced configuration.
-   `dbArr`: An optional `DBDriver` for database-specific SQL generation. Defaults to `MySQL{}`.

### `Filter` Struct

The `Filter` struct is the main data structure for building queries.

```go
type Filter struct {
    Glue      string        `json:"glue"`
    Field     string        `json:"field"`
    Type      string        `json:"type"`
    Predicate string        `json:"predicate"`
    Filter    string        `json:"filter"`
    Value     interface{}   `json:"value"`
    Includes  []interface{} `json:"includes"`
    Rules     []Filter      `json:"rules"`
}
```

### `SQLConfig`

The `SQLConfig` struct allows you to customize the behavior of `GetSQL`.

```go
type SQLConfig struct {
    WhitelistFunc CheckFunction
    Whitelist     map[string]bool
    Operations    map[string]CustomOperation
    Predicates    map[string]CustomPredicate
}
```

-   `Whitelist` and `WhitelistFunc`: Restrict which fields can be used in the query.
-   `Operations`: Define custom operations.
-   `Predicates`: Define custom predicates.

### `CustomPredicate`

A `CustomPredicate` allows you to modify the field name in the SQL query, for example, by wrapping it in a function call.

**Example:**

You can define a custom predicate to extract the year from a date field.

```go
	jsonString := `{
		"field": "created_at",
		"predicate": "year",
		"filter": "equal",
		"value": 2024
	}`

	filter, _ := querysql.FromJSON([]byte(jsonString))

	config := &querysql.SQLConfig{
		Predicates: map[string]querysql.CustomPredicate{
			"year": func(n string, p string) (string, error) {
				return fmt.Sprintf("YEAR(%s)", n), nil
			},
		},
	}

	sql, values, _ := querysql.GetSQL(filter, config)
```

### `CustomOperation`

A `CustomOperation` allows you to define a new, custom filter operation.

**Example:**

You can define a custom `is_empty` operation to check for empty strings.

```go
	jsonString := `{
		"field": "name",
		"filter": "is_empty"
	}`

	filter, _ := querysql.FromJSON([]byte(jsonString))

	config := &querysql.SQLConfig{
		Operations: map[string]querysql.CustomOperation{
			"is_empty": func(field string, filter string, values []interface{}) (string, []interface{}, error) {
				return fmt.Sprintf("%s = ''", field), []interface{}{}, nil
			},
		},
	}

	sql, values, _ := querysql.GetSQL(filter, config)
```

## Usage

Here is a basic example of how to use the library:

```go
package main

import (
    "fmt"
    "github.com/xbsoftware/querysql"
)

func main() {
    jsonString := `{
        "glue": "and",
        "rules": [{
            "field": "age",
            "filter": "less",
            "value": 42
        }, {
            "field": "region",
            "includes": [1, 2, 6]
        }]
    }`

    filter, err := querysql.FromJSON([]byte(jsonString))
    if err != nil {
        panic(err)
    }

    // Using the default MySQL driver
    sql, values, err := querysql.GetSQL(filter, nil)
    if err != nil {
        panic(err)
    }
    fmt.Println(sql)    // ( age < ? AND region IN(?,?,?) )
    fmt.Println(values) // [42 1 2 6]

    // Using the PostgreSQL driver
    sql, values, err = querysql.GetSQL(filter, nil, &querysql.PostgreSQL{})
    if err != nil {
        panic(err)
    }
    fmt.Println(sql)    // ( age < $1 AND region IN($2,$3,$4) )
    fmt.Println(values) // [42 1 2 6]
}
```

## Migrating from V1 to V2

The V2 release introduces several breaking changes. Here's how to migrate your code.

### `Filter` Struct Changes

The `Filter` struct has been redesigned.

**V1:**

```go
type Filter struct {
    Glue      string        `json:"glue"`
    Field     string        `json:"field"`
    Condition Condition     `json:"condition"`
    Includes  []interface{} `json:"includes"`
    Kids      []Filter      `json:"rules"`
}
```

**V2:**

```go
type Filter struct {
    Glue      string        `json:"glue"`
    Field     string        `json:"field"`
    Type      string        `json:"type"`
    Predicate string        `json:"predicate"`
    Filter    string        `json:"filter"`
    Value     interface{}   `json:"value"`
    Includes  []interface{} `json:"includes"`
    Rules     []Filter      `json:"rules"`
}
```

-   The `Condition` field has been removed. Its functionality is now handled by the `Type`, `Predicate`, `Filter`, and `Value` fields.
-   The `Kids` field has been renamed to `Rules`.

### `CustomPredicate` Signature Change

The `CustomPredicate` function signature has been simplified.

**V1:**

```go
type CustomPredicate func(fieldName string, predicateName string, values []interface{}) (string, []interface{}, error)
```

**V2:**

```go
type CustomPredicate func(fieldName string, predicateName string) (string, error)
```

The `values` parameter and return value have been removed.

### `SQLConfig` Changes

The `SQLConfig` struct now includes a `Predicates` map to support custom predicates.

**V1:**

```go
type SQLConfig struct {
    WhitelistFunc CheckFunction
    Whitelist     map[string]bool
    Operations    map[string]CustomOperation
}
```

**V2:**

```go
type SQLConfig struct {
    WhitelistFunc CheckFunction
    Whitelist     map[string]bool
    Operations    map[string]CustomOperation
    Predicates    map[string]CustomPredicate
}
```

### Supported operations

- equal
- notEqual
- contains
- notContains
- lessOrEqual
- greaterOrEqual
- less
- notBetween
- between
- greater
- beginsWith
- notBeginsWith
- endsWith
- notEndsWith

### Nesting

Blocks can be nested as follows:

```json
{
  "glue": "and",
  "rules": [
    ruleA,
    {
      "glue": "or",
      "rules": [
        ruleC,
        ruleD
      ]
    }
  ]
}
```

### `between` / `notBetween`

For these operations, both `start` and `end` values can be provided.

```json
{
    "field": "age",
    "filter": "between",
    "value": { "start": 10, "end": 99 }
}
```

If only `start` or `end` is provided, the operation will change to `less` or `greater` automatically.
