JSON to SQL Query
==================

Converts JSON config to SQL Query

```js
{
  "glue": "and",
  "rules": [
    {
      "field": "age",
      "filter": "less",
      "value": 42
    },
    {
      "field": "region",
      "includes": [1,2,6]
    }
  ]
}
```

### Supported operations ( type )

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

### nesting

Blocks can be nested like next

```js
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

### between / notBetween

For those operations, both start and end values can be provided

```js
{
  "field": "age",
  "filter": "between",
  "value": {
    "start": 10,
    "end": 99
  }
}
```

if only *start* or *end* provided, the operation will change to *less* or *greater* automatically