JSON to SQL Query
==================

Converts JSON config to SQL Query

```json
{
  "glue": "and",
  "rules": [{
    "field":"age",
    "condition":{
      "type": "less",
      "filter": 42
    } 
  },{
    "field":"region",
    "includes": [1,2,6]
  }] 
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

### between / notBetween

For those operations, both start and end values can be provided

```json
{
    "field":"age",
    "condition":{
      "type": "between",
      "filter": { "start": 10, "end": 99 }
    } 
  }
```

if only *start* or *end* provided, the operation will change to *less* or *greater* automatically


### aliases

The rule can has alias value. This means that the value of the rule will be replaced by the alias value

```json
{
    "field":"age",
    "alias": "userId",
    "condition":{
      "type": "between",
      "filter": { "start": 10, "end": 99 }
    } 
  }
```

The **alias:value** hash is passed with the filter config

```json
{
  "userId": 3,
  "ids": [1, 2, 3]
}
```

This feature allows to reuse a filter config as templates and replace the values of aliases with any others