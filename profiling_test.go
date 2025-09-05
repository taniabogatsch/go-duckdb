package duckdb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	var empty bool
	r = conn.QueryRowContext(ctx, `SELECT COUNT(*) = 0 FROM duckdb_logs`)
	require.NoError(t, err)
	require.NoError(t, r.Scan(&empty))
	require.Equal(t, false, empty)

	// Clear logs.
	_, err = conn.ExecContext(ctx, `PRAGMA truncate_duckdb_logs`)
	require.NoError(t, err)
}

func TestMetrics(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	var v int64
	r := conn.QueryRowContext(ctx, `SELECT MAX(range + ?)::BIGINT FROM range(100)`, 43)
	require.NoError(t, r.Scan(&v))
	require.Equal(t, int64(142), v)

	m := GetMetrics(conn)

	_, ok := m["goBindTime"]
	require.True(t, ok)
	_, ok = m["goPrepareTime"]
	require.True(t, ok)
	_, ok = m["goExecTime"]
	require.True(t, ok)
	_, ok = m["goExecPending"]
	require.True(t, ok)
}

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
