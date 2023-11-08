package bqb

import (
	"fmt"
)

type errorableStringer interface {
	String() (string, error)
}

func From(table string) FromMod {
	return FromMod{table: table}
}

func FromQuery(alias string, query errorableStringer) FromQueryMod {
	return FromQueryMod{
		alias: alias,
		query: query,
	}
}

func GroupBy(columns ...string) []GroupByMod {
	out := make([]GroupByMod, len(columns))
	for i, c := range columns {
		out[i] = GroupByMod{column: c}
	}
	return out
}

func Limit(limit int) LimitMod {
	return LimitMod{value: limit}
}

func OrderBy(columns ...string) []OrderByMod {
	out := make([]OrderByMod, len(columns))
	for i, c := range columns {
		out[i] = OrderByMod{column: c}
	}
	return out
}

func ParamBool(name string, value bool) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamFloat64(name string, value float64) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamInt(name string, value int) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamInt64(name string, value int64) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamStr(name string, value string) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamStrSlice(name string, value []string) ParamMod {
	return ParamMod{
		name:  name,
		value: value,
	}
}

func ParamStringer(name string, value fmt.Stringer) ParamMod {
	return ParamMod{
		name:  name,
		value: value.String(),
	}
}

func Query(mods ...any) QueryMod {
	q := QueryMod{}

	for _, mod := range mods {
		if v, ok := mod.(FromMod); ok {
			q.fromMods = append(q.fromMods, v)
			continue
		}
		if v, ok := mod.(FromQueryMod); ok {
			q.fromQueryMods = append(q.fromQueryMods, v)
			continue
		}
		if v, ok := mod.([]GroupByMod); ok {
			q.groupByMods = append(q.groupByMods, v...)
			continue
		}
		if v, ok := mod.(LimitMod); ok {
			q.limitMods = append(q.limitMods, v)
			continue
		}
		if v, ok := mod.([]OrderByMod); ok {
			q.orderByMods = append(q.orderByMods, v...)
			continue
		}
		if v, ok := mod.([]SelectMod); ok {
			q.selectMods = append(q.selectMods, v...)
			continue
		}
		if v, ok := mod.(whereMod); ok {
			q.whereMods = append(q.whereMods, v)
			continue
		}
		q.unknownMods = append(q.unknownMods, mod)
	}

	return q
}

func Select(columns ...string) []SelectMod {
	out := make([]SelectMod, len(columns))
	for i, c := range columns {
		out[i] = SelectMod{column: c}
	}
	return out
}

func UnionAll(queries ...QueryMod) UnionAllMod {
	return UnionAllMod{queryMods: queries}
}

func Where(clause string) WhereMod {
	return WhereMod{
		clause: clause,
	}
}

func WhereQuery(clause string, query QueryMod) WhereQueryMod {
	return WhereQueryMod{
		clause: clause,
		query:  query,
	}
}
