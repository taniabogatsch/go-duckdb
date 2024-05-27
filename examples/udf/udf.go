package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/marcboeker/go-duckdb"
)

func checkError(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		log.Fatal(err)
	}
}

var db *sql.DB

type user struct {
	name     string
	age      int
	height   float32
	awesome  bool
	birthday time.Time
}

type tableUDF struct {
	n     int64
	count int64
}

func (d *tableUDF) Config() duckdb.TableFunctionConfig {
	return duckdb.TableFunctionConfig{
		Arguments: []duckdb.Type{
			duckdb.NewDuckdbType[int64](),
		},
		Pushdownprojection: false,
	}
}

func (d *tableUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (duckdb.TableFunction, []duckdb.ColumnMetaData, error) {
	d.count = 0
	d.n = args[0].(int64)
	return d, []duckdb.ColumnMetaData{
		{Name: "result", T: duckdb.NewDuckdbType[int64]()},
	}, nil
}

func (d *tableUDF) Init() duckdb.TableFunctionInitData {
	return duckdb.TableFunctionInitData{
		MaxThreads: 1,
	}
}

func (d *tableUDF) FillRow(row duckdb.Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}
	d.count++
	err := duckdb.SetRowValue(row, 0, d.count)
	return true, err
}

func (d *tableUDF) Cardinality() *duckdb.CardinalityData {
	return &duckdb.CardinalityData{
		Cardinality: uint(d.n),
		IsExact:     true,
	}
}

func main() {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")
	checkError(err)
	checkError(db.Ping())
	defer checkError(db.Close())

	conn, _ := db.Conn(context.Background())
	var fun tableUDF
	err = duckdb.RegisterTableUDF(conn, "my_table_udf", &fun)
	checkError(err)

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM my_table_udf(100)")
	checkError(err)
	defer checkError(rows.Close())

	// Get the column names.
	columns, err := rows.Columns()
	checkError(err)

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch the rows.
	for rows.Next() {
		checkError(rows.Scan(scanArgs...))
		for i, value := range values {
			switch value.(type) {
			case nil:
				fmt.Print(columns[i], ": NULL")
			case []byte:
				fmt.Print(columns[i], ": ", string(value.([]byte)))
			default:
				fmt.Print(columns[i], ": ", value)
			}
			fmt.Printf("\nType: %s\n", reflect.TypeOf(value))
		}
		fmt.Println("-----------------------------------")
	}
}
