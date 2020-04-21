package querysql

type Condition struct {
	Rule  string `json:"type"`
	Value interface{} `json:"filter"`
}

func (c *Condition) getValues() []interface{} {
	valueMap, ok := c.Value.(map[string]interface{})
	if !ok {
		return []interface{}{c.Value}
	}

	return []interface{}{valueMap["start"], valueMap["end"]}
}