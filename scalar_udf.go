package duckdb

//// Related issues: https://golang.org/issue/19835, https://golang.org/issue/19837.
//
///*
//#include <stdlib.h>
//#include <duckdb.h>
//
//void scalar_udf_callback(duckdb_function_info, duckdb_data_chunk input, duckdb_vector output);
//void scalar_udf_delete_callback(void *data);
//
//typedef void (*scalar_udf_callback_t)(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
//*/
//import "C"
//import (
//	"database/sql"
//	"errors"
//	"runtime/cgo"
//	"unsafe"
//)
//
//type ScalarFunctionProvider interface {
//	//Config() TableFunctionConfig
//	//BindArguments(named map[string]any, args ...any) (TableFunction, []ColumnMetaData, error)
//}
//
//// TODO: 'Provider' sounds a bit odd
//
//func RegisterScalarUDF(c *sql.Conn, name string, function ScalarFunctionProvider) error {
//	// c.Raw exposes the underlying driver connection.
//	err := c.Raw(func(anyConn any) error {
//
//		driverConn := anyConn.(*conn)
//		functionName := C.CString(name)
//		defer C.free(unsafe.Pointer(functionName))
//
//		extraInfoHandle := cgo.NewHandle(function)
//
//		scalarFunction := C.duckdb_create_scalar_function()
//		C.duckdb_scalar_function_set_name(scalarFunction, functionName)
//		C.duckdb_scalar_function_set_function(scalarFunction, C.scalar_udf_callback_t(C.scalar_udf_callback))
//		C.duckdb_scalar_function_set_extra_info(
//			scalarFunction,
//			unsafe.Pointer(&extraInfoHandle),
//			C.duckdb_delete_callback_t(C.scalar_udf_delete_callback))
//
//		//C.duckdb_scalar_function_set_return_type(scalarFunction, TODO)
//
//		//for _, inputType := range inputTypes {
//		//	C.duckdb_scalar_function_add_parameter(scalarFunction, inputType)
//		//	C.duckdb_destroy_logical_type(&inputType)
//		//}
//
//		state := C.duckdb_register_scalar_function(driverConn.duckdbCon, scalarFunction)
//		C.duckdb_destroy_scalar_function(&scalarFunction)
//
//		if state == C.DuckDBError {
//			// TODO: return proper error
//			return errors.New("failed to create scalar function")
//		}
//		return nil
//	})
//	return err
//}
