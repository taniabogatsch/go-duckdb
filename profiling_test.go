package duckdb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := conn.ExecContext(context.Background(), `PRAGMA enable_profiling = 'no_output'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)
	require.NoError(t, err)

	res, err := conn.QueryContext(context.Background(), `SELECT range AS i FROM range(100) ORDER BY i`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	info, err := GetProfilingInfo(conn)
	require.NoError(t, err)

	_, err = conn.ExecContext(context.Background(), `PRAGMA disable_profiling`)
	require.NoError(t, err)

	// Verify the metrics.
	require.NotEmpty(t, info.Metrics, "metrics must not be empty")
	require.NotEmpty(t, info.Children, "children must not be empty")
	require.NotEmpty(t, info.Children[0].Metrics, "child metrics must not be empty")
}

func TestErrProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := GetProfilingInfo(conn)
	testError(t, err, errProfilingInfoEmpty.Error())
}

func TestLogging(t *testing.T) {
	defer func() {
		require.NoError(t, os.Remove("file_read.db"))
	}()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	_, err := conn.ExecContext(ctx, `SET enable_logging=true`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET logging_level='trace'`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `ATTACH 'file_read.db' (BLOCK_SIZE 16384)`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `CREATE TABLE file_read.tbl AS SELECT range::BIGINT AS id FROM range(10000)`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `DETACH file_read`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `ATTACH 'file_read.db'`)
	require.NoError(t, err)

	var id int64
	r := conn.QueryRowContext(ctx, `SELECT id FROM file_read.tbl WHERE id = 42`)
	require.NoError(t, err)
	require.NoError(t, r.Scan(&id))
	require.Equal(t, int64(42), id)

	// Print logs
	res, err := conn.QueryContext(ctx, `SELECT message FROM duckdb_logs`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	for res.Next() {
		var msg string
		require.NoError(t, res.Scan(&msg))
		fmt.Println(msg)
	}

	var empty bool
	r = conn.QueryRowContext(ctx, `SELECT COUNT(*) = 0 FROM duckdb_logs`)
	require.NoError(t, err)
	require.NoError(t, r.Scan(&empty))
	require.Equal(t, false, empty)

	// Clear logs.
	_, err = conn.ExecContext(ctx, `PRAGMA truncate_duckdb_logs`)
	require.NoError(t, err)
}
