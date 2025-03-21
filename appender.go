package duckdb

import (
	"database/sql/driver"
	"errors"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	conn     *Conn
	schema   string
	table    string
	appender mapping.Appender
	closed   bool

	// The appender storage before flushing any data.
	chunks []DataChunk
	// The column types of the table to append to.
	types []mapping.LogicalType
	// The column names of the table to append to.
	names []string
	// The number of appended rows.
	rowCount int
	// The active columns of the appender.
	activeColumns []bool
}

// NewAppenderFromConn returns a new Appender for the default catalog from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender from a DuckDB driver connection.
func NewAppender(driverConn driver.Conn, catalog, schema, table string) (*Appender, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, getError(errInvalidCon, nil)
	}
	if conn.closed {
		return nil, getError(errClosedCon, nil)
	}

	var appender mapping.Appender
	state := mapping.AppenderCreateExt(conn.conn, catalog, schema, table, &appender)
	if state == mapping.StateError {
		err := getDuckDBError(mapping.AppenderError(appender))
		mapping.AppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		conn:     conn,
		schema:   schema,
		table:    table,
		appender: appender,
		rowCount: 0,
	}

	var tableDesc mapping.TableDescription
	state = mapping.TableDescriptionCreateExt(conn.conn, catalog, schema, table, &tableDesc)
	defer mapping.TableDescriptionDestroy(&tableDesc)
	if state == mapping.StateError {
		mapping.AppenderDestroy(&appender)
		err := getDuckDBError(mapping.TableDescriptionError(tableDesc))
		return nil, getError(errTableDescCreation, err)
	}

	// Get the column types.
	columnCount := mapping.AppenderColumnCount(appender)
	for i := mapping.IdxT(0); i < columnCount; i++ {
		colType := mapping.AppenderColumnType(appender, i)
		a.types = append(a.types, colType)

		colName := mapping.TableDescriptionGetColumnName(tableDesc, i)
		a.names = append(a.names, colName)

		a.activeColumns = append(a.activeColumns, true)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err := addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyTypeSlice(a.types)
			mapping.AppenderDestroy(&appender)
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
	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		err := getDuckDBError(mapping.AppenderError(a.appender))
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
	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		errFlush = getDuckDBError(mapping.AppenderError(a.appender))
	}

	// Destroy all appender data and the appender.
	destroyTypeSlice(a.types)
	var errClose error
	if mapping.AppenderDestroy(&a.appender) == mapping.StateError {
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
// If there are fewer values than columns, then the trailing columns default to their default values or NULL.
// Note that this can trigger a Flush, if the number of values changes between calls to AppendRow[...].
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	// TODO: Make opt-in with boolean or so. or better, move to safe version or so
	if a.mustChangeActiveColumnsSlice(args) {
		if err := a.changeActiveColumns(); err != nil {
			return getError(errAppenderAppendRow, err)
		}
	}

	if err := a.newDataChunk(); err != nil {
		return getError(errAppenderAppendRow, err)
	}

	// Set all values.
	for i, val := range args {
		chunk := &a.chunks[len(a.chunks)-1]
		err := chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return getError(errAppenderAppendRow, err)
		}
	}

	a.rowCount++

	return nil
}

func (a *Appender) mustChangeActiveColumnsMap(m map[string]driver.Value) bool {
	// Change the active columns.
	activeColumnsChange := false
	for i, name := range a.names {
		_, ok := m[name]
		if ok && !a.activeColumns[i] {
			activeColumnsChange = true
			a.activeColumns[i] = true
		}
		if !ok && a.activeColumns[i] {
			activeColumnsChange = true
			a.activeColumns[i] = false
		}
	}
	return activeColumnsChange
}

func (a *Appender) changeActiveColumns() error {
	if err := a.Flush(); err != nil {
		return err
	}
	a.chunks = append(a.chunks, chunk)

	state := mapping.AppenderClearColumns(a.appender)
	if state == mapping.StateError {
		return getDuckDBError(mapping.AppenderError(a.appender))
	}
	for i, active := range a.activeColumns {
		if active {
			state = mapping.AppenderAddColumn(a.appender, a.names[i])
			if state == mapping.StateError {
				return getDuckDBError(mapping.AppenderError(a.appender))
			}
		}
	}
	return nil
}

// AppendRowMap appends a row of values to the appender.
// The values are provided as a column name to argument mapping.
// If there are fewer values than columns, then the missing columns default to their default values or NULL.
// Note that this can trigger a Flush, if the number of values changes between calls to AppendRow[...].
func (a *Appender) AppendRowMap(m map[string]driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	// TODO: Make opt-in with boolean or so.
	if a.mustChangeActiveColumnsMap(m) {
		if err := a.changeActiveColumns(); err != nil {
			return getError(errAppenderAppendRow, err)
		}
	}

	if err := a.newDataChunk(); err != nil {
		return err
	}

	// Set all values.
	for i, active := range a.activeColumns {
		if active {
			chunk := &a.chunks[len(a.chunks)-1]
			err := chunk.SetValue(i, a.rowCount, m[a.names[i]])
			if err != nil {
				return err
			}
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

func (a *Appender) mustChangeActiveColumnsSlice(args []driver.Value) bool {
	activeColumnsChange := false
	for i := 0; i < len(args); i++ {
		if !a.activeColumns[i] {
			activeColumnsChange = true
			a.activeColumns[i] = true
		}
	}
	for i := len(args); i < len(a.activeColumns); i++ {
		if a.activeColumns[i] {
			activeColumnsChange = true
			a.activeColumns[i] = false
		}
	}
	return activeColumnsChange
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
		if mapping.AppendDataChunk(a.appender, chunk.chunk) == mapping.StateError {
			err = getDuckDBError(mapping.AppenderError(a.appender))
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

func destroyTypeSlice(slice []mapping.LogicalType) {
	for _, t := range slice {
		mapping.DestroyLogicalType(&t)
	}
}
