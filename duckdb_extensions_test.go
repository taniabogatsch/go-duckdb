//go:build !duckdb_use_lib && !windows

package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenSQLite(t *testing.T) {
	db := openDbWrapper(t, `sqlite:testdata/pets.sqlite`)
	defer closeDbWrapper(t, db)

	var species string
	res := db.QueryRow(`SELECT species FROM pets WHERE id = 1`)
	require.NoError(t, res.Scan(&species))
	require.Equal(t, "Gopher", species)
}

func TestLoadHTTPFS(t *testing.T) {
	defer VerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`INSTALL httpfs`)
	require.NoError(t, err)
	_, err = db.Exec(`LOAD httpfs`)
	require.NoError(t, err)
}
