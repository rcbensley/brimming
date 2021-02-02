package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	minBatch = 2
	minThreads = 2
	minRows = 1000
	minTables = 1
	sessionSQL = "SET SESSION sql_log_bin=0"
	socket     = flag.String("socket", "/var/lib/mysql/mysql.sock", "Path to socket file")
	host       = flag.String("host", "", "MariaDB hostname or IP address")
	port       = flag.Int("port", 3306, "MariaDB server port")
	user       = flag.String("user", "root", "MariaDB server user")
	password   = flag.String("password", "", "MariaDB server password")
	batchSize  = flag.Int("batch", 16000, "Number of rows to insert per-batch, 1 row is roughly 1KB. Pay attention to max_allowed_packet. Min=1000")
	threads    = flag.Int("threads", 100, "Number of concurrent threads to insert row batches. Min=2")
	rows       = flag.Int("rows", 100000, "Total number of rows to be inserted. Each row is roughly 1KB. Default is 100,000 rows (100MB). Min=1000")
	tables     = flag.Int("tables", 1, "Number of tables to distribute inserts between. Min=1")
	database   = flag.String("database", "brim", "Database schema, default is 'brim'")
)

type brim struct {
	db            *sql.DB
	rowCountTotal int
	rowsPerTable  int
	batchSize     int
	tableCount    int
	database      string
	tableBaseName string
	tableNames    []string
	threads       int
}

func randomString(length int) string {
	var characters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, length)
	for i := range s {
		s[i] = characters[rand.Intn(len(characters))]
	}
	return string(s)
}

func generateRow() string {
	// 1000000000 x genRow ~= 1TB
	rand.Seed(time.Now().UnixNano())
	b := rand.Intn(2147483647)
	c := randomString(255)
	d := randomString(255)
	e := randomString(255)
	f := randomString(255)
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

// Load table will generate a batch of data using generateRow and load the target table.
func (b *brim) loadTable(table string) {

	for i := b.batchSize; i <= b.rowsPerTable; i = i + b.batchSize {
		batch := make([]string, b.batchSize)
		r := generateRow()
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

func databaseSetup(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	return db, nil
}

func (b *brim) run() {
	log.Printf("Starting load of %d rows into %d table(s) with %d rows each,\n", b.rowCountTotal, len(b.tableNames), b.rowsPerTable)
	log.Printf("Batch size is %d, with %d max rows per-table.\n", b.batchSize, b.rowsPerTable)

	jobCount := len(b.tableNames)
	jobs := make(chan string, jobCount)
	jobResults := make(chan string, jobCount)

	for worker := 1; worker <= b.threads; worker++ {
		go func(id int, jobs <-chan string, results chan<- string) {
			for t := range jobs {
				log.Printf("Worker %d is loading %s with %d rows\n", id, t, b.rowsPerTable)
				b.loadTable(t)
				results <- t
			}
		}(worker, jobs, jobResults)
	}

	for j := 0; j <= jobCount-1; j++ {
		jobs <- b.tableNames[j]
	}
	close(jobs)

	for r := 0; r <= jobCount-1; r++ {
		<-jobResults
	}
}

func init() {
	if *rows < minRows {
		*rows = minRows
	}

	if *threads < minThreads {
		*threads = minThreads
	}

	if *batchSize < minBatch{
		*batchSize = minBatch
	}

	if *tables < minTables {
		*tables = minTables
	}

	log.Printf(`Brimming is using %d thread(s)
 to insert %d rows
 into %d table(s),
 with a batch size of %d per-thread.`, *threads, *rows, *tables, *batchSize)
}

func main() {
	var (
		err         error
		dsnUser     = user
		dsnProtocol = "unix"
		dsnAddress  = socket
		dsnOptions  = "?multiStatements=true&autocommit=true&maxAllowedPacket=0"
	)

	if *host != "" {
		dsnProtocol = "tcp"
		*dsnAddress = fmt.Sprintf("%s:%d", *host, *port)
	}

	if *password != "" {
		*dsnUser = fmt.Sprintf("%s:%s", *user, *password)
	}

	dsn := fmt.Sprintf("%s@%s(%s)/%s", *dsnUser, dsnProtocol, *dsnAddress, dsnOptions)

	b := brim{
		rowCountTotal: *rows,
		database:      *database,
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

	b.db, err = databaseSetup(dsn)
	if err != nil {
		log.Fatal(err)
	}

	err = b.createDatabase()
	if err != nil {
		log.Fatal(err)
	}

	err = b.createTables()
	if err != nil {
		log.Fatal(err)
	}

}
