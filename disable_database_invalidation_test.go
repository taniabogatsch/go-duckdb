//go:build linux && amd64

package duckdb

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDisableDatabaseInvalidation(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:?disable_database_invalidation=true")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
