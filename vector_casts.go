package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"math/big"
	"reflect"
	"strconv"
	"time"
)

func (vec *vector) tryCastUnsafe(val any) (any, error) {
	switch vec.duckdbType {
	case C.DUCKDB_TYPE_LIST:
		// Multi-level nested list.
		switch val.(type) {
		case []bool, []int8, []int16, []int32, []int64, []uint8, []uint16, []uint32,
			[]uint64, []float32, []float64, []time.Time, []Interval, []map[string]any, []Map:
			return val, nil
		default:
			return vec.tryCastList(val, false)
		}

	case C.DUCKDB_TYPE_STRUCT:
		switch val.(type) {
		case map[string]any:
			return val, nil
		default:
			return vec.tryCastStruct(val, false)
		}
	default:
		return val, nil
	}
}

func (vec *vector) tryCast(val any, safe bool) (any, error) {
	if val == nil {
		return val, nil
	}

	if !safe {
		return vec.tryCastUnsafe(val)
	}

	switch vec.duckdbType {
	case C.DUCKDB_TYPE_INVALID:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_BOOLEAN:
		return tryPrimitiveCast[bool](val, reflect.Bool.String())
	case C.DUCKDB_TYPE_TINYINT:
		return tryNumericCast[int8](val, reflect.Int8.String())
	case C.DUCKDB_TYPE_SMALLINT:
		return tryNumericCast[int16](val, reflect.Int16.String())
	case C.DUCKDB_TYPE_INTEGER:
		return tryNumericCast[int32](val, reflect.Int32.String())
	case C.DUCKDB_TYPE_BIGINT:
		return tryNumericCast[int64](val, reflect.Int64.String())
	case C.DUCKDB_TYPE_UTINYINT:
		return tryNumericCast[uint8](val, reflect.Uint8.String())
	case C.DUCKDB_TYPE_USMALLINT:
		return tryNumericCast[uint16](val, reflect.Uint16.String())
	case C.DUCKDB_TYPE_UINTEGER:
		return tryNumericCast[uint32](val, reflect.Uint32.String())
	case C.DUCKDB_TYPE_UBIGINT:
		return tryNumericCast[uint64](val, reflect.Uint64.String())
	case C.DUCKDB_TYPE_FLOAT:
		return tryNumericCast[float32](val, reflect.Float32.String())
	case C.DUCKDB_TYPE_DOUBLE:
		return tryNumericCast[float64](val, reflect.Float64.String())
	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS,
		C.DUCKDB_TYPE_TIMESTAMP_NS, C.DUCKDB_TYPE_TIMESTAMP_TZ, C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIME:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_INTERVAL:
		return tryPrimitiveCast[Interval](val, reflect.TypeOf(Interval{}).String())
	case C.DUCKDB_TYPE_HUGEINT:
		// Note that this expects *big.Int.
		return tryPrimitiveCast[*big.Int](val, reflect.TypeOf(big.Int{}).String())
	case C.DUCKDB_TYPE_UHUGEINT:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_VARCHAR:
		return tryPrimitiveCast[string](val, reflect.String.String())
	case C.DUCKDB_TYPE_BLOB:
		return tryPrimitiveCast[[]byte](val, reflect.TypeOf([]byte{}).String())
	case C.DUCKDB_TYPE_DECIMAL:
		return vec.tryCastDecimal(val)
	case C.DUCKDB_TYPE_ENUM:
		return vec.tryCastEnum(val)
	case C.DUCKDB_TYPE_LIST:
		return vec.tryCastList(val, safe)
	case C.DUCKDB_TYPE_STRUCT:
		return vec.tryCastStruct(val, safe)
	case C.DUCKDB_TYPE_MAP:
		return tryPrimitiveCast[Map](val, reflect.TypeOf(Map{}).String())
	case C.DUCKDB_TYPE_ARRAY:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_UUID:
		return tryPrimitiveCast[UUID](val, reflect.TypeOf(UUID{}).String())
	case C.DUCKDB_TYPE_UNION:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_BIT:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_TIME_TZ:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	default:
		return nil, unsupportedTypeError("unknown type")
	}
}

func (*vector) canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	}
	return false
}

func tryPrimitiveCast[T any](val any, expected string) (T, error) {
	v, ok := val.(T)
	if ok {
		return v, nil
	}

	goType := reflect.TypeOf(val)
	return v, castError(goType.String(), expected)
}

func tryNumericCast[T numericType](val any, expected string) (T, error) {
	v, ok := val.(T)
	if ok {
		return v, nil
	}

	// JSON unmarshalling uses float64 for numbers.
	// We might want to add more implicit casts here.
	switch value := val.(type) {
	case float64:
		return convertNumericType[float64, T](value), nil
	}

	goType := reflect.TypeOf(val)
	return v, castError(goType.String(), expected)
}

func (vec *vector) tryCastDecimal(val any) (Decimal, error) {
	v, ok := val.(Decimal)
	if !ok {
		goType := reflect.TypeOf(val)
		return v, castError(goType.String(), reflect.TypeOf(Decimal{}).String())
	}

	if v.Width != vec.width || v.Scale != vec.scale {
		d := Decimal{Width: vec.width, Scale: vec.scale}
		return v, castError(d.toString(), v.toString())
	}
	return v, nil
}

func (vec *vector) tryCastEnum(val any) (string, error) {
	v, ok := val.(string)
	if !ok {
		goType := reflect.TypeOf(val)
		return v, castError(goType.String(), reflect.String.String())
	}

	_, ok = vec.dict[v]
	if !ok {
		return v, castError(v, "ENUM value")
	}
	return v, nil
}

func (vec *vector) tryCastList(val any, safe bool) ([]any, error) {
	goType := reflect.TypeOf(val)
	if goType.Kind() != reflect.Slice {
		return nil, castError(goType.String(), reflect.Slice.String())
	}

	v := reflect.ValueOf(val)
	list := make([]any, v.Len())
	childVector := vec.childVectors[0]

	for i := 0; i < v.Len(); i++ {
		idx := v.Index(i)
		if vec.canNil(idx) && idx.IsNil() {
			list[i] = nil
			continue
		}

		var err error
		list[i], err = childVector.tryCast(idx.Interface(), safe)
		if err != nil {
			return nil, err
		}
	}
	return list, nil
}

func (vec *vector) tryCastStruct(val any, safe bool) (map[string]any, error) {
	m, isMap := val.(map[string]any)

	// Transform the struct into map[string]any.
	if !isMap {
		// Catch mismatching types.
		goType := reflect.TypeOf(val)
		if reflect.TypeOf(val).Kind() != reflect.Struct {
			return nil, castError(goType.String(), reflect.Struct.String())
		}

		m = make(map[string]any)
		v := reflect.ValueOf(val)
		structType := v.Type()

		for i := 0; i < structType.NumField(); i++ {
			fieldName := structType.Field(i).Name
			m[fieldName] = v.Field(i).Interface()
		}
	}

	// Catch mismatching field count.
	if len(m) != len(vec.childNames) {
		return nil, structFieldError(strconv.Itoa(len(m)), strconv.Itoa(len(vec.childNames)))
	}

	// Cast child entries and return the map.
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := vec.childVectors[i]
		childName := vec.childNames[i]
		v, ok := m[childName]

		// Catch mismatching field names.
		if !ok {
			return nil, structFieldError("missing field", childName)
		}

		var err error
		m[childName], err = childVector.tryCast(v, safe)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
