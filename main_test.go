package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"testing"
)

func Test_OneFileWith25Tuples_success(t *testing.T) {
	rows := make([]Row, 0, 25)
	for i := 0; i < 25; i++ {
		row := Row{
			ID:        20,
			Name:      "Hotel",
			Condition: "cond1",
			Price:     int64(i * 100),
			State:     "state",
		}
		rows = append(rows, row)
	}
	createInputFile("file.csv", rows)
	process([]string{"main.exe", "1000", "20", "file.csv"})
	checkFile("result.csv", t)
}

func checkFile(name string, t *testing.T) {
	file, _ := os.Open(name)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = ';'
	line, err := reader.Read()
	rows := make([]Row, 0, 1000)
	for err != io.EOF {
		ID, _ := strconv.ParseInt(line[0], 10, 64)
		Name := line[1]
		Condition := line[2]
		State := line[3]
		Price, _ := strconv.ParseInt(line[4], 10, 64)
		row := Row{
			ID:        ID,
			Name:      Name,
			Condition: Condition,
			State:     State,
			Price:     Price,
		}
		rows = append(rows, row)
		line, err = reader.Read()
	}
	for i := 0; i < len(rows)-1; i++ {
		if rows[i].Price >= rows[i+1].Price {
			t.FailNow()
		}
	}
}

func createInputFile(name string, rows []Row) {
	file, createFileErr := os.Create(name)
	if createFileErr != nil {
		log.Panicln("Creating file error")
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()
	for _, row := range rows {
		record := []string{strconv.FormatInt(row.ID, 10), row.Name, row.Condition, row.State, strconv.FormatInt(row.Price, 10)}
		writer.Write(record)
	}
}
