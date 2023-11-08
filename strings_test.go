package bqb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_sanitizeColumnName(t *testing.T) {
	r := require.New(t)

	r.Equal(sanitizeColumnName("my_column"), "my_column")
	r.Equal(sanitizeColumnName("; DROP TABLE my_table"), "")
}

func Test_sanitizeTableName(t *testing.T) {
	r := require.New(t)

	r.Equal(sanitizeTableName("my_table"), "my_table")
	r.Equal(sanitizeTableName("; DROP TABLE my_table"), "")
}
