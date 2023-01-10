package querysql

type Condition struct {
	Rule  string      `json:"type"`
	Value interface{} `json:"filter"`
}

func (c *Condition) getValues() []interface{} {
	return getValues(c.Value)
}

func getValues(v interface{}) []interface{} {
	valueMap, ok := v.(map[string]interface{})
	if !ok {
		return []interface{}{v}
	}

	return []interface{}{valueMap["start"], valueMap["end"]}
}
