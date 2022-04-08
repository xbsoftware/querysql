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

func (m *PostgreSQL) IsJSON(name string) (string, bool) {
	if !strings.HasPrefix(name, "json:") {
		return name, false
	}

	//json:table.field.name:type
	meta := strings.Split(name, ":")
	parts := strings.SplitN(meta[1], ".", 3)

	//apply type
	tp := "text"
	if len(meta) > 2 {
		tp = meta[2]
	}
	var s, e string
	if tp == "date" {
		s = "CAST("
		e = " AS DATE)"
		tp = "text"
	}

	if len(parts) == 3 {
		name = fmt.Sprintf("%s(\"%s\".%s->'%s')::%s%s", s, parts[0], parts[1], parts[2], tp, e)
	} else if len(parts) == 2 {
		name = fmt.Sprintf("%s(%s->'%s')::%s%s", s, parts[0], parts[1], tp, e)
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
