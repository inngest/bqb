package bqb

import (
	"errors"
	"fmt"
	"strings"
)

type FromMod struct {
	table string
}

func (f FromMod) validate() error {
	if f.table == "" {
		return errors.New("table cannot be empty")
	}
	if sanitizeTableName(f.table) == "" {
		return fmt.Errorf("table is unsanitary")
	}

	return nil
}

type FromQueryMod struct {
	alias string
	query errorableStringer
}

type GroupByMod struct {
	column string
}

type LimitMod struct {
	value int
}

type OrderByMod struct {
	column string
}

type ParamMod struct {
	name  string
	value any
}

type QueryMod struct {
	fromMods      []FromMod
	fromQueryMods []FromQueryMod
	groupByMods   []GroupByMod
	limitMods     []LimitMod
	orderByMods   []OrderByMod
	selectMods    []SelectMod
	unknownMods   []any
	whereMods     []whereMod
}

func (q *QueryMod) Limit(limit int) {
	q.limitMods = append(q.limitMods, Limit(limit))
}

func (q QueryMod) String() (string, error) {
	if err := q.validate(); err != nil {
		return "", err
	}

	query := "SELECT"
	for _, mod := range q.selectMods {
		query += " " + mod.column + ","
	}

	if len(q.fromMods) == 1 {
		fromMod := q.fromMods[0]
		if err := fromMod.validate(); err != nil {
			return "", err
		}

		query += "\nFROM " + fromMod.table
	} else if len(q.fromQueryMods) == 1 {
		fromQueryMod := q.fromQueryMods[0]
		queryStr, err := fromQueryMod.query.String()
		if err != nil {
			return "", err
		}

		query += "\nFROM (\n"
		query += indent(queryStr)
		query += "\n) AS " + fromQueryMod.alias
	}

	for i, mod := range q.whereMods {
		if i == 0 {
			query += "\nWHERE"
		} else {
			query += "\n  AND"
		}

		clause, err := mod.String()
		if err != nil {
			return "", err
		}
		query += " " + clause
	}

	if len(q.groupByMods) > 0 {
		query += "\nGROUP BY"
		for i, mod := range q.groupByMods {
			if i > 0 {
				query += ","
			}
			query += " " + mod.column
		}
	}

	if len(q.orderByMods) > 0 {
		query += "\nORDER BY"
		for i, mod := range q.orderByMods {
			if i > 0 {
				query += ","
			}
			query += " " + mod.column
		}
	}

	if len(q.limitMods) == 1 {
		query += fmt.Sprintf("\nLIMIT %d", +q.limitMods[0].value)
	}

	return query, nil
}

func (q QueryMod) validate() error {
	if len(q.fromMods)+len(q.fromQueryMods) != 1 {
		return errors.New("must have 1 from clause")
	}
	if len(q.limitMods) > 1 {
		return errors.New("cannot have more than 1 limit clause")
	}
	if len(q.selectMods) == 0 {
		return errors.New("must have at least 1 select clause")
	}
	if len(q.unknownMods) > 0 {
		if _, ok := q.unknownMods[0].(ParamMod); ok {
			return errors.New("param mods are only allowed in Build()")
		}

		return errors.New("unknown mod")
	}
	return nil
}

func (q *QueryMod) Where(clause string) {
	q.whereMods = append(q.whereMods, Where(clause))
}

type SelectMod struct {
	column string
}

type UnionAllMod struct {
	queryMods []QueryMod
}

func (u UnionAllMod) String() (string, error) {
	queries := make([]string, len(u.queryMods))
	for i, q := range u.queryMods {
		queryStr, err := q.String()
		if err != nil {
			return "", err
		}

		queries[i] = "(" + queryStr + ")"
	}

	return strings.Join(queries, "\n\nUNION ALL\n\n"), nil
}

type whereMod interface {
	isWhereMod()
	String() (string, error)
}

type WhereMod struct {
	clause string
}

func (w WhereMod) isWhereMod() {}

func (w WhereMod) String() (string, error) {
	return w.clause, nil
}

type WhereQueryMod struct {
	clause string
	query  errorableStringer
}

func (w WhereQueryMod) isWhereMod() {}

func (w WhereQueryMod) String() (string, error) {
	if err := w.validate(); err != nil {
		return "", err
	}

	queryStr, err := w.query.String()
	if err != nil {
		return "", err
	}

	// Wrap with parens to avoid precedence issues. For example,
	// "UNNEST(<query>)" will cause an error but "UNNEST((<query>))" won't.
	queryStr = fmt.Sprintf("(\n%s\n)", indent(queryStr))

	return fmt.Sprintf(w.clause, queryStr), nil
}

func (w WhereQueryMod) validate() error {
	if strings.Count(w.clause, "%s") != 1 {
		return errors.New("WhereQuery clause must have exactly 1 %s directive")
	}

	return nil
}
