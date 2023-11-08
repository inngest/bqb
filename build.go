package bqb

import (
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
)

func Build(
	client *bigquery.Client,
	query errorableStringer,
	paramMods ...ParamMod,
) (*bigquery.Query, error) {
	queryStr, err := query.String()
	if err != nil {
		return nil, err
	}
	queryStr = "\n" + queryStr

	paramNames := map[string]any{}
	for _, p := range extractParamNames(queryStr) {
		paramNames[p] = nil
	}

	if len(paramNames) != len(paramMods) {
		return nil, fmt.Errorf("%d params found in query but %d params provided", len(paramNames), len(paramMods))
	}

	bqQuery := client.Query(queryStr)

	bqQuery.Parameters = make([]bigquery.QueryParameter, len(paramMods))
	for i, mod := range paramMods {
		if _, ok := paramNames[mod.name]; !ok {
			return nil, errors.New("param not found in query: " + mod.name)
		}

		bqQuery.Parameters[i] = bigquery.QueryParameter{
			Name:  mod.name,
			Value: mod.value,
		}
	}

	return bqQuery, nil
}
