//go:build duckdb_arrow

package duckdb

/*
#include <stdlib.h>
#include <stdint.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArrayStream {
	void (*get_schema)(struct ArrowArrayStream*);
	void (*get_next)(struct ArrowArrayStream*);
	void (*get_last_error)(struct ArrowArrayStream*);
	void (*release)(struct ArrowArrayStream*);
	void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// Arrow exposes DuckDB Apache Arrow interface.
// https://duckdb.org/docs/api/c/api#arrow-interface
type Arrow struct {
	conn *Conn
}

// NewArrowFromConn returns a new Arrow from a DuckDB driver connection.
func NewArrowFromConn(driverConn driver.Conn) (*Arrow, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}

	if conn.closed {
		return nil, errClosedCon
	}
	return &Arrow{conn: conn}, nil
}

// QueryContext prepares statements, executes them, returns Apache Arrow array.RecordReader as a result of the last
// executed statement. Arguments are bound to the last statement.
func (a *Arrow) QueryContext(ctx context.Context, query string, args ...any) (array.RecordReader, error) {
	if a.conn.closed {
		return nil, errClosedCon
	}

	extractedStmts, size, errExtract := a.conn.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer apiDestroyExtracted(&extractedStmts)

	// Execute all statements without args, except the last one.
	for i := uint64(0); i < size-1; i++ {
		extractedStmt, err := a.conn.prepareExtractedStmt(extractedStmts, i)
		if err != nil {
			return nil, err
		}

		// Send nil args to execute the statement and ignore the result.
		_, err = extractedStmt.ExecContext(ctx, nil)
		errClose := extractedStmt.Close()
		if err != nil || errClose != nil {
			return nil, errors.Join(err, errClose)
		}
	}

	// Prepare and execute the last statement with args.
	stmt, err := a.conn.prepareExtractedStmt(extractedStmts, size-1)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	res, err := a.execute(stmt, a.anyArgsToNamedArgs(args))
	if err != nil {
		return nil, err
	}
	defer apiDestroyArrow(res)

	sc, err := a.queryArrowSchema(res)
	if err != nil {
		return nil, err
	}

	var recs []arrow.Record
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	rowCount := uint64(apiArrowRowCount(*res))
	var retrievedRows uint64
	for retrievedRows < rowCount {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rec, err := a.queryArrowArray(res, sc)
		if err != nil {
			return nil, err
		}

		recs = append(recs, rec)
		retrievedRows += uint64(rec.NumRows())
	}
	return array.NewRecordReader(sc, recs)
}

// queryArrowSchema fetches the internal arrow schema from the arrow result.
func (a *Arrow) queryArrowSchema(res *apiArrow) (*arrow.Schema, error) {
	schema := C.calloc(1, C.sizeof_struct_ArrowSchema)
	defer func() {
		cdata.ReleaseCArrowSchema((*cdata.CArrowSchema)(schema))
		C.free(schema)
	}()

	arrowSchema := apiArrowSchema{
		Ptr: unsafe.Pointer(&schema),
	}

	state := apiQueryArrowSchema(*res, &arrowSchema)
	if apiState(state) == apiStateError {
		return nil, errors.New("duckdb_query_arrow_schema")
	}

	sc, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(schema))
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCArrowSchema", err)
	}
	return sc, nil
}

// queryArrowArray fetches an internal arrow array from the arrow result.
//
// This function can be called multiple time to get next chunks,
// which will free the previous out_array.
func (a *Arrow) queryArrowArray(res *apiArrow, sc *arrow.Schema) (arrow.Record, error) {
	arr := C.calloc(1, C.sizeof_struct_ArrowArray)
	defer func() {
		cdata.ReleaseCArrowArray((*cdata.CArrowArray)(arr))
		C.free(arr)
	}()

	arrowArray := apiArrowArray{
		Ptr: unsafe.Pointer(&arr),
	}

	state := apiQueryArrowArray(*res, &arrowArray)
	if apiState(state) == apiStateError {
		return nil, errors.New("duckdb_query_arrow_array")
	}

	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(arr), sc)
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCRecordBatchWithSchema", err)
	}

	return rec, nil
}

func (a *Arrow) execute(s *Stmt, args []driver.NamedValue) (*apiArrow, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}

	var res apiArrow
	state := apiExecutePreparedArrow(*s.preparedStmt, &res)
	if apiState(state) == apiStateError {
		errMsg := apiQueryArrowError(res)
		apiDestroyArrow(&res)
		return nil, fmt.Errorf("failed to execute the prepared arrow: %v", errMsg)
	}
	return &res, nil
}

func (a *Arrow) anyArgsToNamedArgs(args []any) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}

	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	return argsToNamedArgs(values)
}

// RegisterView registers an Arrow record reader as a view with the given name in DuckDB.
// The returned release function must be called to release the memory once the view is no longer needed.
func (a *Arrow) RegisterView(reader array.RecordReader, name string) (release func(), err error) {
	if a.conn.closed {
		return nil, errClosedCon
	}

	// duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_stream arrow);

	stream := C.calloc(1, C.sizeof_struct_ArrowArrayStream)
	release = func() {
		C.free(stream)
	}
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(stream))

	arrowStream := apiArrowStream{
		Ptr: unsafe.Pointer(stream),
	}

	state := apiArrowScan(a.conn.conn, name, arrowStream)
	if apiState(state) == apiStateError {
		release()
		return nil, errors.New("duckdb_arrow_scan")
	}
	return release, nil
}
