package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	sessionSQL string = "SET SESSION sql_log_bin=0"
	socket = flag.String("socket", "/var/lib/mysql/mysql.sock", "Path to socket file")
	port = flag.Int("port", 3306, "MariaDB server port")
	user = flag.String("user", "root", "MariaDB server user")
	password = flag.String("password", "", "MariaDB server password")
	batchSize = flag.Int("batch", 1000, "Number of rows to insert per-batch")
	threads = flag.Int("theads", 100, "Number of concurrent threads to insert row batches")
	rowsTotal = flag.Int("rows", 1000000000000, "Total number of rows to be inserted. Max of 1,000,000,000 rows.")
	tables = flag.Int("tables", 1, "Number of tables to distribute inserts between")
	database = flag.String("database", "brim", "Database schema")
)

type brim struct {
	db            *sql.DB
	rowCountTotal int
	rowsPerTable  int
	batchSize     int
	tableCount    int
	database  string
	tableBaseName string
	tableNames    []string
	threads       int
}

func randString(length int) string {
	var characters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, length)
	for i := range s {
		s[i] = characters[rand.Intn(len(characters))]
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
	log.Printf("Creating database %s\n", b.database)
	create := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", b.database)
	err := b.exec(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTable(name string) error {
	log.Printf("Creating table %s.%s\n", b.database, name)
	create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
	a bigint(20) NOT NULL AUTO_INCREMENT,
	b int(11) NOT NULL,
	c char(255) NOT NULL,
	d char(255) NOT NULL,
	e char(255) NOT NULL,
	f char(255) NOT NULL,
	PRIMARY KEY (a),
	INDEX (b)) ENGINE=InnoDB;`, b.database, name)
	err := b.exec(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTables() error {
	for i := range b.tableNames {
		err := b.createTable(b.tableNames[i])
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
		r := genRow()
		for a := range batch {
			batch[a] = r
		}
		joinedBatch := strings.Join(batch, ",")
		row := fmt.Sprintf("INSERT INTO %s.%s (b,c,d,e,f) VALUES %s", b.database, table, joinedBatch)
		err := b.exec(row)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func init() {
	// With an limit of 1b rows, and a max of 100 tables, the largest table can be 10m rows.
	if *rowsTotal > 1000000000000 {
		*rowsTotal = 1000000000000
	}
	if *rowsTotal <= 1 {
		*rowsTotal = 1
	}

	if *threads > 100 {
		*threads = 100
	}
	if *threads <= 1 {
		*threads = 1
	}

	if *batchSize > *rowsTotal {
		log.Printf("Batch size cannot be larger can the total rows, using row limit of %d.", rowsTotal)
		*batchSize = *rowsTotal
	}
	if *batchSize <= 1 {
		*batchSize = 1
	}	
}

func main() {
	var err error

	dsn := fmt.Sprintf("%s@unix(%s)/?multiStatements=true&autocommit=true", user, socket)

	b := brim{
		rowCountTotal: *rowsTotal,
		database:  *database,
		tableBaseName: "brim",
		threads:       *threads,
		batchSize:     *batchSize,
	}

	b.rowsPerTable = b.rowCountTotal / b.threads
	b.tableCount = b.threads

	tableNames := make([]string, b.threads)
	for i := 0; i <= b.tableCount-1; i++ {
		tableNames[i] = fmt.Sprintf("%s%d", b.tableBaseName, i)
	}
	b.tableNames = tableNames

	db, err := sql.Open("mysql", dsn)
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
		jobs <- b.tableNames[j]
	}
	close(jobs)

	for r := 0; r <= jobCount-1; r++ {
		<-jobResults
	}
}

func (b *brim) loadTable(id int, jobs <-chan string, results chan<- string) {
	for t := range jobs {
		log.Printf("Worker %d is loading %s with %d rows\n", id, t, b.rowsPerTable)
		b.load(t)
		results <- t
	}
}
