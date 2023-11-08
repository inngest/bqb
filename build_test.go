package bqb

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

var (
	client = bigquery.Client{}
)

func TestBasic(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			Where("age > 50"),
			Where("name = 'bob'"),
		),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id, name, age,
FROM my_table
WHERE age > 50
  AND name = 'bob'`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestComplex(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select(
				"run_id",
				"ANY_VALUE(run_started_at) AS run_started_at",
				"ANY_VALUE(run_ended_at) AS run_ended_at",
				"COALESCE(MAX(status), 'Running') AS status",
			),
			FromQuery(
				"sub",
				UnionAll(
					Query(
						Select(
							"run_id",
							"run_started_at",
							"null AS run_ended_at",
							"null AS status",
						),
						From("workflow_run_starts"),
						Where("run_started_at >= @lower_time"),
						Where("run_started_at < @upper_time"),
						Where("account_id = @account_id"),
						Where("workspace_id = @workspace_id"),
						Where("workflow_id = @workflow_id"),
					),
					Query(
						Select(
							"run_id",
							"run_started_at",
							"run_ended_at",
							"status",
						),
						From("workflow_run_ends_by_end_time"),
						Where("run_ended_at >= @lower_time"),
						Where("run_ended_at < @upper_time"),
						Where("account_id = @account_id"),
						Where("workspace_id = @workspace_id"),
						Where("workflow_id = @workflow_id"),
					),
				),
			),
			GroupBy("run_id"),
		),
		ParamInt("account_id", 0),
		ParamInt("lower_time", 0),
		ParamInt("upper_time", 0),
		ParamInt("workflow_id", 0),
		ParamInt("workspace_id", 0),
	)
	r.NoError(err)

	expectedQuery := `
SELECT run_id, ANY_VALUE(run_started_at) AS run_started_at, ANY_VALUE(run_ended_at) AS run_ended_at, COALESCE(MAX(status), 'Running') AS status,
FROM (
  (SELECT run_id, run_started_at, null AS run_ended_at, null AS status,
  FROM workflow_run_starts
  WHERE run_started_at >= @lower_time
    AND run_started_at < @upper_time
    AND account_id = @account_id
    AND workspace_id = @workspace_id
    AND workflow_id = @workflow_id)
  
  UNION ALL
  
  (SELECT run_id, run_started_at, run_ended_at, status,
  FROM workflow_run_ends_by_end_time
  WHERE run_ended_at >= @lower_time
    AND run_ended_at < @upper_time
    AND account_id = @account_id
    AND workspace_id = @workspace_id
    AND workflow_id = @workflow_id)
) AS sub
GROUP BY run_id`

	r.Equal(expectedQuery, query.Q)

	expectedParams := []bigquery.QueryParameter{
		{Name: "account_id", Value: 0},
		{Name: "lower_time", Value: 0},
		{Name: "upper_time", Value: 0},
		{Name: "workflow_id", Value: 0},
		{Name: "workspace_id", Value: 0},
	}
	r.ElementsMatch(expectedParams, query.Parameters)
}

func TestParams(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			Where("age > @age"),
			Where("name = @name"),
		),
		ParamInt("age", 50),
		ParamStr("name", "bob"),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id, name, age,
FROM my_table
WHERE age > @age
  AND name = @name`

	r.Equal(expectedQuery, query.Q)

	expectedParams := []bigquery.QueryParameter{
		{Name: "age", Value: 50},
		{Name: "name", Value: "bob"},
	}
	r.ElementsMatch(expectedParams, query.Parameters)
}

func TestTooFewParams(t *testing.T) {
	r := require.New(t)

	_, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			Where("name = @name"),
			Where("age > @age"),
		),
		ParamStr("name", "bob"),
	)
	r.Error(err)
}

func TestWrongParams(t *testing.T) {
	r := require.New(t)

	_, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			Where("age > @age"),
			Where("name = @name"),
		),
		ParamInt("foo", 0),
		ParamStr("name", "bob"),
	)
	r.Error(err)
}

func TestExtraParam(t *testing.T) {
	r := require.New(t)

	_, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			Where("age > @age"),
			Where("name = @name"),
		),
		ParamInt("age", 50),
		ParamInt("foo", 0),
		ParamStr("name", "bob"),
	)
	r.Error(err)
}

func TestGroupBy(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			GroupBy("id", "name"),
		),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id, name, age,
FROM my_table
GROUP BY id, name`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestUnionAll(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		UnionAll(
			Query(
				Select("foo"),
				From("my_table_1"),
			),
			Query(
				Select("bar"),
				From("my_table_2"),
			),
		),
	)
	r.NoError(err)

	expectedQuery := `
(SELECT foo,
FROM my_table_1)

UNION ALL

(SELECT bar,
FROM my_table_2)`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestSubQuery(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id"),
			FromQuery(
				"my_sub_query",
				Query(
					Select("id"),
					From("my_table"),
				),
			),
		),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id,
FROM (
  SELECT id,
  FROM my_table
) AS my_sub_query`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestLimit(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id"),
			From("my_table"),
			Limit(1),
		),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id,
FROM my_table
LIMIT 1`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestOrderBy(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			OrderBy("id", "name ASC"),
		),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id, name, age,
FROM my_table
ORDER BY id, name ASC`

	r.Equal(expectedQuery, query.Q)
	r.Len(query.Parameters, 0)
}

func TestWhereQuery(t *testing.T) {
	r := require.New(t)

	query, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			WhereQuery(
				"id IN UNNEST(%s)",
				Query(
					Select("id"),
					From("my_other_table"),
					Where("age > @age"),
				),
			),
		),
		ParamInt("age", 50),
	)
	r.NoError(err)

	expectedQuery := `
SELECT id, name, age,
FROM my_table
WHERE id IN UNNEST((
  SELECT id,
  FROM my_other_table
  WHERE age > @age
))`

	r.Equal(expectedQuery, query.Q)
	expectedParams := []bigquery.QueryParameter{
		{Name: "age", Value: 50},
	}
	r.ElementsMatch(expectedParams, query.Parameters)
}

func TestInvalidWhereQuery(t *testing.T) {
	r := require.New(t)

	// No %s directive.
	_, err := Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			WhereQuery(
				"id IN UNNEST(oops)",
				Query(
					Select("id"),
					From("my_other_table"),
				),
			),
		),
	)
	r.Error(err)

	// Too many %s directives.
	_, err = Build(
		&client,
		Query(
			Select("id", "name", "age"),
			From("my_table"),
			WhereQuery(
				"id IN UNNEST(%s, %s)",
				Query(
					Select("id"),
					From("my_other_table"),
				),
			),
		),
	)
	r.Error(err)
}
