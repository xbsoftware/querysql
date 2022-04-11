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
	var table, field, name string
	var meta []string
	tp := "text"

	parts := strings.SplitN(v, ".", 3)
	if len(parts) == 2 && strings.HasPrefix(parts[0], "json:") {
		// [0]:json:field, [1]:name:type
		field = parts[0][5:]
		meta = strings.SplitN(parts[1], ":", 2)
	} else if len(parts) == 3 && strings.HasPrefix(parts[1], "json:") {
		// [0]:table, [1]:json:field, [2]:name:type
		table = parts[0]
		field = parts[1][5:]
		meta = strings.SplitN(parts[2], ":", 2)
	} else {
		return v, false
	}
	name = meta[0]
	if len(meta) == 2 {
		tp = meta[1]
	}

	var s, e string
	if tp == "date" {
		s = "CAST("
		e = " AS DATE)"
		tp = "text"
	} else if tp == "" {
		tp = "text"
	}

	if len(parts) == 3 {
		name = fmt.Sprintf("%s(\"%s\".\"%s\"->'%s')::%s%s", s, table, field, name, tp, e)
	} else {
		name = fmt.Sprintf("%s(\"%s\"->'%s')::%s%s", s, field, name, tp, e)
	}
	return name, true
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
		search = "'\"' ||  " + m.Mark() + "  || '%'"
	} else {
		search = m.Mark() + "  || '%'"
	}
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m *PostgreSQL) NotBeginsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'\"' ||  " + m.Mark() + "  || '%'"
	} else {
		search = m.Mark() + "  || '%'"
	}
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}

func (m *PostgreSQL) EndsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'%' ||  " + m.Mark() + "  || '\"'"
	} else {
		search = "'%' ||  " + m.Mark() + " "
	}
	return fmt.Sprintf("%s LIKE %s", v, search)
}

func (m *PostgreSQL) NotEndsWith(v string, isJSON bool) string {
	var search string
	if isJSON {
		search = "'%' ||  " + m.Mark() + " || '\"'"
	} else {
		search = "'%' ||  " + m.Mark() + " "
	}
	return fmt.Sprintf("%s NOT LIKE %s", v, search)
}
