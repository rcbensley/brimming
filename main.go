package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var sessionSQL string = "SET SESSION sql_log_bin=0"

type table struct {
	name string
	rows int
}

type brim struct {
	db            *sql.DB
	rowCountTotal int
	rowsPerTable  int
	batchSize     int
	tableCount    int
	databaseName  string
	tableBaseName string
	tableNames    []table
	threads       int
}

func randString(length int) string {
	var digits = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, length)
	for i := range s {
		s[i] = digits[rand.Intn(len(digits))]
	}
	return string(s)
}

func genRow() string {
	// 1000000000 x genRow ~= 1TB
	rand.Seed(time.Now().UnixNano())
	b := rand.Intn(2147483647)
	c := randString(255)
	d := randString(255)
	e := randString(255)
	f := randString(255)
	return fmt.Sprintf("(%d,'%s','%s','%s','%s')", b, c, d, e, f)
}

func (b *brim) createDatabase() error {
	log.Printf("Creating database %s\n", b.databaseName)
	create := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", b.databaseName)
	err := b.exec(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTable(name string) error {
	log.Printf("Creating table %s.%s\n", b.databaseName, name)
	create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
	a bigint(20) NOT NULL AUTO_INCREMENT,
	b int(11) NOT NULL,
	c char(255) NOT NULL,
	d char(255) NOT NULL,
	e char(255) NOT NULL,
	f char(255) NOT NULL,
	PRIMARY KEY (a),
	INDEX (b)) ENGINE=InnoDB;`, b.databaseName, name)
	err := b.exec(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTables() error {
	for i := range b.tableNames {
		err := b.createTable(b.tableNames[i].name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *brim) exec(query string) error {
	q := fmt.Sprintf("%s; %s;", sessionSQL, query)
	_, err := b.db.Exec(q)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) load(table string) {

	for i := b.batchSize; i <= b.rowsPerTable; i = i + b.batchSize {
		batch := make([]string, b.batchSize)
		for a := range batch {
			r := genRow()
			batch[a] = r
		}
		joinedBatch := strings.Join(batch, ",")
		row := fmt.Sprintf("INSERT INTO %s.%s (b,c,d,e,f) VALUES %s", b.databaseName, table, joinedBatch)
		err := b.exec(row)
		if err != nil {
			log.Fatal(err)
		}

		// if (i + batchSize) >= b.rowsPerTable {
		// 	batchSize = b.rowsPerTable - i
		// }

		// i = i + batchSize
		// fmt.Printf("i is %d rows are %d batch is %d\n", i, b.rowsPerTable, batchSize)
	}
}

func main() {
	var rowsTotal int = 1000000000
	var threads int = 20
	var batchSize int = 1000
	var err error

	// With an limit of 1b rows, and a max of 100 tables, the largest table can be 10m rows.
	if len(os.Args) >= 2 {
		rowsTotal, err = strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		if rowsTotal > 1000000000 {
			log.Println("Max of 1,000,000,000 rows.")
			rowsTotal = 1000000000
		}
		if rowsTotal <= 1 {
			rowsTotal = 1
		}
	}

	if len(os.Args) >= 3 {
		threads, err = strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		if threads > 100 {
			log.Println("Max of 100 threads")
			threads = 100
		}
		if threads <= 1 {
			threads = 1
		}
	}

	if len(os.Args) >= 4 {
		batchSize, err = strconv.Atoi(os.Args[3])
		if err != nil {
			log.Fatal(err)
		}
		if batchSize > rowsTotal {
			log.Printf("Batch size cannot be larger can the total rows, using row limit of %d.", rowsTotal)
			batchSize = rowsTotal
		}
		if batchSize <= 1 {
			batchSize = 1
		}
	}

	b := brim{
		rowCountTotal: rowsTotal,
		databaseName:  "brim",
		tableBaseName: "brim",
		threads:       threads,
		batchSize:     batchSize,
	}

	b.rowsPerTable = b.rowCountTotal / b.threads
	b.tableCount = b.threads

	tableNames := make([]table, b.threads)
	for i := 0; i <= b.tableCount-1; i++ {
		t := table{name: fmt.Sprintf("%s%d", b.tableBaseName, i), rows: b.rowsPerTable}
		tableNames[i] = t
	}
	b.tableNames = tableNames

	db, err := sql.Open("mysql", "root@unix(/var/lib/mysql/mysql.sock)/?multiStatements=true&autocommit=true")
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	b.db = db

	err = b.createDatabase()
	if err != nil {
		log.Fatal(err)
	}

	err = b.createTables()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting load of %d rows over %d table(s) with %d rows each,\n", b.rowCountTotal, len(b.tableNames), b.rowsPerTable)
	log.Printf("Batch size is %d, with %d max rows per-table.\n", b.batchSize, b.rowsPerTable)

	jobCount := len(b.tableNames)
	jobs := make(chan string, jobCount)
	jobResults := make(chan string, jobCount)

	for worker := 1; worker <= b.threads; worker++ {
		go b.loadTable(worker, jobs, jobResults)
	}

	for j := 0; j <= jobCount-1; j++ {
		jobs <- b.tableNames[j].name
	}
	close(jobs)

	for r := 0; r <= jobCount-1; r++ {
		<-jobResults
	}
}

func (b *brim) loadTable(id int, jobs <-chan string, results chan<- string) {
	for t := range jobs {
		log.Printf("Worker %d loading %s with %d rows\n", id, t, b.rowsPerTable)
		b.load(t)
		results <- t
	}
}
