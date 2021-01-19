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
	sessionSQL = "SET SESSION sql_log_bin=0"
	socket     = flag.String("socket", "/var/lib/mysql/mysql.sock", "Path to socket file")
	host       = flag.String("host", "", "MariaDB hostname or IP address")
	port       = flag.Int("port", 3306, "MariaDB server port")
	user       = flag.String("user", "root", "MariaDB server user")
	password   = flag.String("password", "", "MariaDB server password")
	batchSize  = flag.Int("batch", 1000, "Number of rows to insert per-batch")
	threads    = flag.Int("theads", 100, "Number of concurrent threads to insert row batches")
	rows       = flag.Int("rows", 0, "Total number of rows to be inserted. Each row is 1 Kilobyte")
	tables     = flag.Int("tables", 1, "Number of tables to distribute inserts between")
	database   = flag.String("database", "brim", "Database schema")
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
	err := b.insertRow(create)
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
	err := b.insertRow(create)
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

func (b *brim) insertRow(query string) error {
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
		err := b.insertRow(row)
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
	if *rows <= 0 {
		log.Fatalln("Specify at least 1 row to be inserted ...")
	}

	if *threads <= 0 {
		log.Fatalln("Specify at least 1 thread ...")
	}

	if *batchSize > *rows {
		log.Fatalln("Batch size cannot be larger than the total rows ...")
	}

	if *batchSize <= 1 {
		log.Fatalln("Batch size needs to be greater than 1 ...")
	}

	if *tables <= 0 {
		log.Fatalln("At least 1 table needs to be specified, so the data can go somewhere ...")
	}
}

func main() {
	var (
		err         error
		dsnUser     = user
		dnsProtocol = "unix"
		dnsAddress  = socket
		dnsOptions  = "?multiStatements=true&autocommit=true&maxAllowedPacket=0"
	)

	if *host != "" {
		dnsProtocol = "tcp"
		*dnsAddress = fmt.Sprintf("%s:%d", *host, *port)
	}

	if *password != "" {
		*dsnUser = fmt.Sprintf("%s:%s", *user, *password)
	}

	dsn := fmt.Sprintf("%s@%s(%s)/%s", *dsnUser, dnsProtocol, *dnsAddress, dnsOptions)

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
