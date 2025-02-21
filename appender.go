package duckdb

import (
	"database/sql/driver"
	"errors"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	conn     *Conn
	schema   string
	table    string
	appender apiAppender
	closed   bool

	// The appender storage before flushing any data.
	chunks []DataChunk
	// The column types of the table to append to.
	types []apiLogicalType
	// The column names of the table to append to.
	names []string
	// The number of appended rows.
	rowCount int
}

// NewAppenderFromConn returns a new Appender for the default catalog from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema string, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender from a DuckDB driver connection.
func NewAppender(driverConn driver.Conn, catalog string, schema string, table string) (*Appender, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, getError(errInvalidCon, nil)
	}
	if conn.closed {
		return nil, getError(errClosedCon, nil)
	}

	var appender apiAppender
	state := apiAppenderCreateExt(conn.conn, catalog, schema, table, &appender)
	if apiState(state) == apiStateError {
		err := getDuckDBError(apiAppenderError(appender))
		apiAppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		conn:     conn,
		schema:   schema,
		table:    table,
		appender: appender,
		rowCount: 0,
	}

	var tableDesc apiTableDescription
	state = apiTableDescriptionCreateExt(conn.conn, catalog, schema, table, &tableDesc)
	defer apiTableDescriptionDestroy(&tableDesc)
	if apiState(state) == apiStateError {
		apiAppenderDestroy(&appender)
		err := getDuckDBError(apiTableDescriptionError(tableDesc))
		return nil, getError(errTableDescCreation, err)
	}

	// Get the column names and types.
	columnCount := apiAppenderColumnCount(appender)
	for i := uint64(0); i < columnCount; i++ {
		colType := apiAppenderColumnType(appender, i)
		a.types = append(a.types, colType)

		colName := apiTableDescriptionGetColumnName(tableDesc, i)
		a.names = append(a.names, colName)

		// Ensure that we only create an appender for supported column types.
		t := Type(apiGetTypeId(colType))
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err := addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyTypeSlice(a.types)
			apiAppenderDestroy(&appender)
			return nil, getError(errAppenderCreation, err)
		}
	}
	return a, nil
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if err := a.appendDataChunks(); err != nil {
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	state := apiAppenderFlush(a.appender)
	if apiState(state) == apiStateError {
		err := getDuckDBError(apiAppenderError(a.appender))
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}
	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunks()

	// We flush before closing to get a meaningful error message.
	var errFlush error
	state := apiAppenderFlush(a.appender)
	if apiState(state) == apiStateError {
		errFlush = getDuckDBError(apiAppenderError(a.appender))
	}

	// Destroy all appender data and the appender.
	destroyTypeSlice(a.types)
	var errClose error
	state = apiAppenderDestroy(&a.appender)
	if apiState(state) == apiStateError {
		errClose = errAppenderClose
	}

	err := errors.Join(errAppend, errFlush, errClose)
	if err != nil {
		return getError(invalidatedAppenderError(err), nil)
	}
	return nil
}

// AppendRow appends a row of values to the appender.
// The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}
	return nil
}

// AppendRowMap appends a row of values to the appender.
// The values are provided as a column name to argument mapping.
func (a *Appender) AppendRowMap(m map[string]driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	if len(m) != len(a.names) {
		// TODO: ensure that each name maps to a column, remember default columns
		panic("TODO: implement default values")
	}

	if err := a.newDataChunk(); err != nil {
		return err
	}

	// Set all values.
	for i, name := range a.names {
		chunk := &a.chunks[len(a.chunks)-1]
		err := chunk.SetValue(i, a.rowCount, m[name])
		if err != nil {
			return err
		}
	}
	a.rowCount++
	return nil
}

func (a *Appender) newDataChunk() error {
	if a.rowCount != GetDataChunkCapacity() && len(a.chunks) != 0 {
		return nil
	}
	// Create a new data chunk if
	// - the current chunk is full, or
	// - this chunk is the initial chunk.
	var chunk DataChunk
	if err := chunk.initFromTypes(a.types, true); err != nil {
		return err
	}
	a.chunks = append(a.chunks, chunk)
	a.rowCount = 0
	return nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return columnCountError(len(args), len(a.types))
	}

	if err := a.newDataChunk(); err != nil {
		return err
	}

	// Set all values.
	for i, val := range args {
		chunk := &a.chunks[len(a.chunks)-1]
		err := chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}
	a.rowCount++
	return nil
}

func (a *Appender) appendDataChunks() error {
	var err error

	for i, chunk := range a.chunks {
		// All data chunks except the last are at maximum capacity.
		size := GetDataChunkCapacity()
		if i == len(a.chunks)-1 {
			size = a.rowCount
		}
		if err = chunk.SetSize(size); err != nil {
			break
		}

		state := apiAppendDataChunk(a.appender, chunk.chunk)
		if apiState(state) == apiStateError {
			err = getDuckDBError(apiAppenderError(a.appender))
			break
		}
	}

	for _, chunk := range a.chunks {
		chunk.close()
	}

	a.chunks = a.chunks[:0]
	a.rowCount = 0
	return err
}

func destroyTypeSlice(slice []apiLogicalType) {
	for _, t := range slice {
		apiDestroyLogicalType(&t)
	}
}
