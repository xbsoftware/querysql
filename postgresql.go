package querysql

import (
	"fmt"
	"strings"
)

type PostgreSQL struct {
	counter int
}

func (m *PostgreSQL) Reset() {
	m.counter = 1
}

func (m *PostgreSQL) Mark() string {
	m.counter += 1
	t := fmt.Sprintf("$%d", m.counter)
	return t
}

func (m *PostgreSQL) IsJSON(v string) (string, bool) {
	//table.json:field.name:type
	fieldOnly := strings.HasPrefix(v, "json:")
	var dot int
	if !fieldOnly {
		dot = strings.Index(v, ".")
		if dot == -1 || !strings.HasPrefix(v[dot+1:], "json:") {
			return v, false
		}
	}

	// separate table and field
	table := ""
	field := v
	if !fieldOnly {
		table = v[:dot]
		field = v[dot+1:]
	}

	// separate field name and meta info
	meta := strings.Split(field, ":")
	name := strings.Split(meta[1], ".")

	var tp string
	if len(meta) == 3 {
		tp = meta[2]
	}

	var s, e string
	if tp == "date" {
		s = "CAST("
		e = " AS DATE)"
		tp = "text"
	} else if tp == "" {
		tp = "text"
	}

	if table != "" {
		return fmt.Sprintf("%s(\"%s\".\"%s\"->'%s')::%s%s", s, table, name[0], name[1], tp, e), true
	}
	return fmt.Sprintf("%s(\"%s\"->'%s')::%s%s", s, name[0], name[1], tp, e), true
}

func (m *PostgreSQL) Contains(v string, isJSON bool) string {
	if isJSON {
		// Quotes (" ... ") are needed for correct work. Fields of type text in JSONB are wrapped by default
		return fmt.Sprintf("%s LIKE '\"%%' || %s || '%%\"'", v, m.Mark())
	}
	return fmt.Sprintf("%s LIKE '%%' || %s || '%%'", v, m.Mark())
}

func (m *PostgreSQL) NotContains(v string, isJSON bool) string {
	if isJSON {
		return fmt.Sprintf("%s NOT LIKE '\"%%' || %s || '%%\"'", v, m.Mark())
	}
	return fmt.Sprintf("%s NOT LIKE '%%' || %s || '%%'", v, m.Mark())
}

func (m *PostgreSQL) BeginsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'\"' || " + m.Mark() + " || '%'"
	} else {
		search = m.Mark() + " || '%'"
	}
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m *PostgreSQL) NotBeginsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'\"' || " + m.Mark() + " || '%'"
	} else {
		search = m.Mark() + " || '%'"
	}
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}

func (m *PostgreSQL) EndsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'%' || " + m.Mark() + " || '\"'"
	} else {
		search = "'%' || " + m.Mark()
	}
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m *PostgreSQL) NotEndsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'%' || " + m.Mark() + " || '\"'"
	} else {
		search = "'%' || " + m.Mark()
	}
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}
