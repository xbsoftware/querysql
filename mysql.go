package querysql

import (
	"fmt"
)

type MySQL struct{}

func (m MySQL) Mark() string {
	return "?"
}

func (m MySQL) IsJSON(name string) (string, bool) {
	return name, false
}

func (m MySQL) Contains(v string, isJSON bool) string {
	return fmt.Sprintf("INSTR(%s, ?) > 0", v)
}

func (m MySQL) NotContains(v string, isJSON bool) string {
	return fmt.Sprintf("INSTR(%s, ?) = 0", v)
}

func (m MySQL) BeginsWith(v string, isJSON bool) string {
	search := "CONCAT(?, '%')"
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m MySQL) NotBeginsWith(v string, isJSON bool) string {
	search := "CONCAT(?, '%')"
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}

func (m MySQL) EndsWith(v string, isJSON bool) string {
	search := "CONCAT('%', ?)"
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m MySQL) NotEndsWith(v string, isJSON bool) string {
	search := "CONCAT('%', ?)"
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}
