//go:build duckdb_use_static_lib

package duckdb

/*
#cgo CPPFLAGS: -DDUCKDB_STATIC_BUILD
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"
