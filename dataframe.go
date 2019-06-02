package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

// DataFrame is a dataframe
type DataFrame struct {
	rawRecords          []StringCol
	columnsToIdxMapping map[string]int
	idxs                []int
	Cols                StringCol
}

// GetCol returns values of a column
func (df DataFrame) GetCol(col string) StringCol {
	var s []string
	rows, _ := df.Dims()
	for i := 0; i < rows; i++ {
		s = append(s, df.At(i, df.columnsToIdxMapping[col]).(string))
	}

	return s
}

func (df DataFrame) getRow(row int) StringCol {
	return df.rawRecords[row][:]
}

// Dims returns the rows, cols for a the df
func (df DataFrame) Dims() (int, int) {
	cols := len(df.rawRecords[0])
	rows := len(df.rawRecords[0:][:])
	return rows, cols
}

// At returns value at a location
func (df DataFrame) At(i, j int) interface{} {
	return df.rawRecords[i][j]
}

// Slice slices the caller df and returns a new Dataframe
func (df DataFrame) Slice(rows []int, cols []string) DataFrame {
	var records []StringCol
	for _, row := range rows {
		records = append(records, df.getRow(row))
	}

	for idx := range records {
		updatedRow := []string{}
		for _, col := range cols {
			colIdx, ok := df.columnsToIdxMapping[col]
			if !ok {
				panic(fmt.Sprintf("Colum: '%s' not found", col))
			}
			updatedRow = append(updatedRow, records[idx][colIdx])
		}
		records[idx] = updatedRow
	}

	return DataFrame{
		rawRecords: records,
		columnsToIdxMapping: func() map[string]int {
			m := map[string]int{}
			for idx, c := range cols {
				m[c] = idx
			}

			return m
		}(),
		idxs: rows,
		Cols: cols,
	}
}

// Summary returns a breif summary about the dataframe
func (df DataFrame) Summary() {
	rows, cols := df.Dims()
	fmt.Println("==================")
	fmt.Printf("Dimensions:\n(%d, %d)\n\n", rows, cols)
	fmt.Printf("Columns and Mappings:\n%+v\n", df.columnsToIdxMapping)
	fmt.Println("==================")
}

// Head returns number of a rows or a default of 5 rows
func (df DataFrame) Head(numRows ...int) DataFrame {
	rows := []int{1, 2, 3, 4, 5}
	if numRows != nil {
		rows = []int{}
		for i := 0; i < numRows[0]; i++ {
			rows = append(rows, i)
		}
	}
	var cols []string
	for k := range df.columnsToIdxMapping {
		cols = append(cols, k)
	}

	return df.Slice(rows, cols)
}

// ReadCSV reads a csv and returns a dataframe object
func ReadCSV(path string) DataFrame {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(f)
	var records []StringCol
	for {
		record, err := r.Read()
		if err != nil && err != io.EOF {
			os.Exit(1)
		}

		if err == io.EOF {
			break
		}

		records = append(records, StringCol(record))

	}

	cols := records[0]
	m := map[string]int{}
	for idx, colName := range cols {
		m[colName] = idx
	}

	return DataFrame{
		rawRecords:          records[1:],
		columnsToIdxMapping: m,
		idxs: func() []int {
			idxs := []int{}
			for i := range records[1:][:] {
				idxs = append(idxs, i)
			}
			return idxs
		}(),
		Cols: records[0],
	}
}

// GetRecords gets the records
func (df DataFrame) GetRecords() []StringCol {
	return df.rawRecords
}

// StringCol is string column
type StringCol []string

// Float64Col is float64 column
type Float64Col []float64

// ToFloat64Go Converts fs to golangs type []float64
func (fs Float64Col) ToFloat64Go() []float64 {
	var ns []float64
	for i := range fs {
		ns = append(ns, fs[i])
	}

	return ns
}

// ToFloat64 takes a stringColumn and returns a float64 column
func (sc StringCol) ToFloat64() Float64Col {
	var fs Float64Col
	for i := 0; i < len(sc); i++ {
		f, _ := strconv.ParseFloat(sc[i], 64)
		fs = append(fs, f)
	}
	return fs
}
