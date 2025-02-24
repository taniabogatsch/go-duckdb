package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
)

type StmtType apiStatementType

const (
	STATEMENT_TYPE_INVALID      = StmtType(apiStatementTypeInvalid)
	STATEMENT_TYPE_SELECT       = StmtType(apiStatementTypeSelect)
	STATEMENT_TYPE_INSERT       = StmtType(apiStatementTypeInsert)
	STATEMENT_TYPE_UPDATE       = StmtType(apiStatementTypeUpdate)
	STATEMENT_TYPE_EXPLAIN      = StmtType(apiStatementTypeExplain)
	STATEMENT_TYPE_DELETE       = StmtType(apiStatementTypeDelete)
	STATEMENT_TYPE_PREPARE      = StmtType(apiStatementTypePrepare)
	STATEMENT_TYPE_CREATE       = StmtType(apiStatementTypeCreate)
	STATEMENT_TYPE_EXECUTE      = StmtType(apiStatementTypeExecute)
	STATEMENT_TYPE_ALTER        = StmtType(apiStatementTypeAlter)
	STATEMENT_TYPE_TRANSACTION  = StmtType(apiStatementTypeTransaction)
	STATEMENT_TYPE_COPY         = StmtType(apiStatementTypeCopy)
	STATEMENT_TYPE_ANALYZE      = StmtType(apiStatementTypeAnalyze)
	STATEMENT_TYPE_VARIABLE_SET = StmtType(apiStatementTypeVariableSet)
	STATEMENT_TYPE_CREATE_FUNC  = StmtType(apiStatementTypeCreateFunc)
	STATEMENT_TYPE_DROP         = StmtType(apiStatementTypeDrop)
	STATEMENT_TYPE_EXPORT       = StmtType(apiStatementTypeExport)
	STATEMENT_TYPE_PRAGMA       = StmtType(apiStatementTypePragma)
	STATEMENT_TYPE_VACUUM       = StmtType(apiStatementTypeVacuum)
	STATEMENT_TYPE_CALL         = StmtType(apiStatementTypeCall)
	STATEMENT_TYPE_SET          = StmtType(apiStatementTypeSet)
	STATEMENT_TYPE_LOAD         = StmtType(apiStatementTypeLoad)
	STATEMENT_TYPE_RELATION     = StmtType(apiStatementTypeRelation)
	STATEMENT_TYPE_EXTENSION    = StmtType(apiStatementTypeExtension)
	STATEMENT_TYPE_LOGICAL_PLAN = StmtType(apiStatementTypeLogicalPlan)
	STATEMENT_TYPE_ATTACH       = StmtType(apiStatementTypeAttach)
	STATEMENT_TYPE_DETACH       = StmtType(apiStatementTypeDetach)
	STATEMENT_TYPE_MULTI        = StmtType(apiStatementTypeMulti)
)

// Stmt implements the driver.Stmt interface.
type Stmt struct {
	conn             *Conn
	preparedStmt     *apiPreparedStatement
	closeOnRowsClose bool
	bound            bool
	closed           bool
	rows             bool
}

// Close the statement.
// Implements the driver.Stmt interface.
func (s *Stmt) Close() error {
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	apiDestroyPrepare(s.preparedStmt)
	return nil
}

// NumInput returns the number of placeholder parameters.
// Implements the driver.Stmt interface.
func (s *Stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	count := apiNParams(*s.preparedStmt)
	return int(count)
}

// ParamName returns the name of the parameter at the given index (1-based).
func (s *Stmt) ParamName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	if s.preparedStmt == nil {
		return "", errUninitializedStmt
	}

	count := apiNParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return "", getError(errAPI, paramIndexError(n, uint64(count)))
	}

	name := apiParameterName(*s.preparedStmt, uint64(n))
	return name, nil
}

// ParamType returns the expected type of the parameter at the given index (1-based).
func (s *Stmt) ParamType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	if s.preparedStmt == nil {
		return TYPE_INVALID, errUninitializedStmt
	}

	count := apiNParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return TYPE_INVALID, getError(errAPI, paramIndexError(n, uint64(count)))
	}

	t := apiParamType(*s.preparedStmt, uint64(n))
	return Type(t), nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	if s.preparedStmt == nil {
		return STATEMENT_TYPE_INVALID, errUninitializedStmt
	}

	t := apiPreparedStatementType(*s.preparedStmt)
	return StmtType(t), nil
}

// Bind the parameters to the statement.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) Bind(args []driver.NamedValue) error {
	if s.closed {
		return errors.Join(errCouldNotBind, errClosedStmt)
	}
	if s.preparedStmt == nil {
		return errors.Join(errCouldNotBind, errUninitializedStmt)
	}
	return s.bind(args)
}

func (s *Stmt) bindHugeint(val *big.Int, n int) (apiState, error) {
	hugeint, err := hugeIntFromNative(val)
	if err != nil {
		return apiStateError, err
	}
	state := apiBindHugeInt(*s.preparedStmt, uint64(n+1), hugeint)
	return apiState(state), nil
}

func (s *Stmt) bindTimestamp(val driver.NamedValue, t Type, n int) (apiState, error) {
	ts, err := getAPITimestamp(t, val.Value)
	if err != nil {
		return apiStateError, err
	}
	state := apiBindTimestamp(*s.preparedStmt, uint64(n+1), ts)
	return apiState(state), nil
}

func (s *Stmt) bindDate(val driver.NamedValue, n int) (apiState, error) {
	date, err := getAPIDate(val.Value)
	if err != nil {
		return apiStateError, err
	}
	state := apiBindDate(*s.preparedStmt, uint64(n+1), date)
	return apiState(state), nil
}

func (s *Stmt) bindTime(val driver.NamedValue, t Type, n int) (apiState, error) {
	ticks, err := getTimeTicks(val.Value)
	if err != nil {
		return apiStateError, err
	}

	if t == TYPE_TIME {
		var ti apiTime
		apiTimeSetMicros(&ti, ticks)
		state := apiBindTime(*s.preparedStmt, uint64(n+1), ti)
		return apiState(state), nil
	}

	// TYPE_TIME_TZ: The UTC offset is 0.
	ti := apiCreateTimeTZ(ticks, 0)
	v := apiCreateTimeTZValue(ti)
	state := apiBindValue(*s.preparedStmt, uint64(n+1), v)
	apiDestroyValue(&v)
	return apiState(state), nil
}

func (s *Stmt) bindComplexValue(val driver.NamedValue, n int) (apiState, error) {
	t, err := s.ParamType(n + 1)
	if err != nil {
		return apiStateError, err
	}
	if name, ok := unsupportedTypeToStringMap[t]; ok {
		return apiStateError, addIndexToError(unsupportedTypeError(name), n+1)
	}

	switch t {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		return s.bindTimestamp(val, t, n)
	case TYPE_DATE:
		return s.bindDate(val, n)
	case TYPE_TIME, TYPE_TIME_TZ:
		return s.bindTime(val, t, n)
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_LIST, TYPE_STRUCT, TYPE_MAP,
		TYPE_ARRAY, TYPE_ENUM:
		// FIXME: for timestamps: distinguish between timestamp[_s|ms|ns] once available.
		// FIXME: for other types: duckdb_param_logical_type once available, then create duckdb_value + duckdb_bind_value
		// FIXME: for other types: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[t]
		return apiStateError, addIndexToError(unsupportedTypeError(name), n+1)
	}
	return apiStateError, addIndexToError(unsupportedTypeError(unknownTypeErrMsg), n+1)
}

func (s *Stmt) bindValue(val driver.NamedValue, n int) (apiState, error) {
	switch v := val.Value.(type) {
	case bool:
		return apiState(apiBindBoolean(*s.preparedStmt, uint64(n+1), v)), nil
	case int8:
		return apiState(apiBindInt8(*s.preparedStmt, uint64(n+1), v)), nil
	case int16:
		return apiState(apiBindInt16(*s.preparedStmt, uint64(n+1), v)), nil
	case int32:
		return apiState(apiBindInt32(*s.preparedStmt, uint64(n+1), v)), nil
	case int64:
		return apiState(apiBindInt64(*s.preparedStmt, uint64(n+1), v)), nil
	case int:
		// int is at least 32 bits.
		return apiState(apiBindInt64(*s.preparedStmt, uint64(n+1), int64(v))), nil
	case *big.Int:
		return s.bindHugeint(v, n)
	case Decimal:
		// FIXME: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[TYPE_DECIMAL]
		return apiStateError, addIndexToError(unsupportedTypeError(name), n+1)
	case uint8:
		return apiState(apiBindUInt8(*s.preparedStmt, uint64(n+1), v)), nil
	case uint16:
		return apiState(apiBindUInt16(*s.preparedStmt, uint64(n+1), v)), nil
	case uint32:
		return apiState(apiBindUInt32(*s.preparedStmt, uint64(n+1), v)), nil
	case uint64:
		return apiState(apiBindUInt64(*s.preparedStmt, uint64(n+1), v)), nil
	case float32:
		return apiState(apiBindFloat(*s.preparedStmt, uint64(n+1), v)), nil
	case float64:
		return apiState(apiBindDouble(*s.preparedStmt, uint64(n+1), v)), nil
	case string:
		return apiState(apiBindVarchar(*s.preparedStmt, uint64(n+1), v)), nil
	case []byte:
		return apiState(apiBindBlob(*s.preparedStmt, uint64(n+1), v)), nil
	case Interval:
		return apiState(apiBindInterval(*s.preparedStmt, uint64(n+1), v.getAPIInterval())), nil
	case nil:
		return apiState(apiBindNull(*s.preparedStmt, uint64(n+1))), nil
	}
	return s.bindComplexValue(val, n)
}

func (s *Stmt) bind(args []driver.NamedValue) error {
	if s.NumInput() > len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	// relaxed length check allow for unused parameters.
	for i := 0; i < s.NumInput(); i++ {
		name := apiParameterName(*s.preparedStmt, uint64(i+1))

		// fallback on index position
		arg := args[i]

		// override with ordinal if set
		for _, v := range args {
			if v.Ordinal == i+1 {
				arg = v
			}
		}

		// override with name if set
		for _, v := range args {
			if v.Name == name {
				arg = v
			}
		}

		state, err := s.bindValue(arg, i)
		if apiState(state) == apiStateError {
			errMsg := apiPrepareError(*s.preparedStmt)
			err = errors.Join(err, getDuckDBError(errMsg))
			return errors.Join(errCouldNotBind, err)
		}
	}

	s.bound = true
	return nil
}

// Deprecated: Use ExecContext instead.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), argsToNamedArgs(args))
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.StmtExecContext interface.
func (s *Stmt) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	defer apiDestroyResult(res)

	ra := apiValueInt64(res, 0, 0)
	return &result{ra}, nil
}

// ExecBound executes a bound query that doesn't return rows, such as an INSERT or UPDATE.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) ExecBound(ctx context.Context) (driver.Result, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	defer apiDestroyResult(res)

	ra := apiValueInt64(res, 0, 0)
	return &result{ra}, nil
}

// Deprecated: Use QueryContext instead.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), argsToNamedArgs(args))
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.StmtQueryContext interface.
func (s *Stmt) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// QueryBound executes a bound query that may return rows, such as a SELECT.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) QueryBound(ctx context.Context) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// This method executes the query in steps and checks if context is cancelled before executing each step.
// It uses Pending Result Interface C APIs to achieve this. Reference - https://duckdb.org/docs/api/c/api#pending-result-interface
func (s *Stmt) execute(ctx context.Context, args []driver.NamedValue) (*apiResult, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext with active Rows")
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}
	return s.executeBound(ctx)
}

func (s *Stmt) executeBound(ctx context.Context) (*apiResult, error) {
	var pendingRes apiPendingResult
	state := apiPendingPrepared(*s.preparedStmt, &pendingRes)
	if apiState(state) == apiStateError {
		dbErr := getDuckDBError(apiPendingError(pendingRes))
		apiDestroyPending(&pendingRes)
		return nil, dbErr
	}
	defer apiDestroyPending(&pendingRes)

	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			apiInterrupt(s.conn.conn)
			close(bgDoneCh)
			return
		case <-mainDoneCh:
			close(bgDoneCh)
			return
		}
	}()

	var res apiResult
	state = apiExecutePending(pendingRes, &res)
	close(mainDoneCh)
	// also wait for background goroutine to finish
	// sometimes the bg goroutine is not scheduled immediately and by that time if another query is running on this connection
	// it can cancel that query so need to wait for it to finish as well
	<-bgDoneCh
	if apiState(state) == apiStateError {
		if ctx.Err() != nil {
			apiDestroyResult(&res)
			return nil, ctx.Err()
		}

		err := getDuckDBError(apiResultError(&res))
		apiDestroyResult(&res)
		return nil, err
	}
	return &res, nil
}

func argsToNamedArgs(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for n, param := range values {
		args[n].Value = param
		args[n].Ordinal = n + 1
	}
	return args
}
