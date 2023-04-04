package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"strings"

	"github.com/alecthomas/kingpin"
	_ "github.com/go-sql-driver/mysql"
)

var (
	Version string = ""
)

type brim struct {
	dsn           string
	db            *sql.DB
	database      string
	rows          int
	batch         int
	tables        int
	threads       int
	jobs          [][]int
	tableBaseName string
	engine        string
}

func sizeToFloat(s string) (float64, error) {
	ns := s[:len(s)-2]
	n, err := strconv.ParseFloat(ns, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func sizeToRows(s string) (int, error) {
	var m float64 = 1
	re := regexp.MustCompile("(?i)[0-9]+[A-Za-z]+")
	if !re.MatchString(s) {
		return 0, fmt.Errorf("-size must be in format [number][size], e.g. 123gb")
	}
	size := strings.ToLower(s[len(s)-2:])

	switch size {
	case "mb":
		m = 1000
	case "gb":
		m = 1000000
	case "tb":
		m = 1000000000
	default:
		return 0, fmt.Errorf("unknown -size %s. I can do mb, gb, and tb", s)
	}

	rows, err := sizeToFloat(s)
	if err != nil {
		return int(rows), err
	}

	rows = rows * m

	return int(rows), nil
}

func randomString(r *rand.Rand) string {
	var length int = 255
	var characters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, length)
	for i := range s {
		s[i] = characters[r.Intn(len(characters))]
	}
	return string(s)
}

func generateRow() []string {
	var limit = 2147483647
	// 1000000000 x genRow ~= 1TB
	r := rand.New(rand.NewSource(64))
	b := strconv.Itoa(r.Intn(limit))
	c := randomString(r)
	d := randomString(r)
	e := randomString(r)
	f := randomString(r)
	return []string{b, c, d, e, f}
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
	INDEX (b)) ENGINE=%s;`, b.database, name, b.engine)
	err := b.insertRow(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTables() error {
	for i := 1; i <= b.tables; i++ {
		err := b.createTable(fmt.Sprintf("%s%d", b.tableBaseName, i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *brim) insertRow(query string) error {
	_, err := b.db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func generateBatch(rows int) [][]string {
	batch := make([][]string, rows)
	for i := range batch {
		r := generateRow()
		batch[i] = r
	}
	return batch
}

// Load table will generate a batch of data using generateRow and load the target table.
func (b *brim) loadTable(stmt *sql.Stmt, rows int) error {
	data := generateBatch(rows)
	if _, err := stmt.Exec(data); err != nil {
		return err
	}
	return nil
}

func NewBrim(username, password string, host string, port int, socket, database, engine, size string, rows, batch, tables, threads int) (*brim, error) {
	var (
		defaultRows    int    = 1000000
		defaultBatch   int    = 1000
		defaultTables  int    = 10
		defaultThreads int    = 100
		protocol       string = "unix"
		dsnOptions     string = "?multiStatements=true&autocommit=true&maxAllowedPacket=0"
		hostAndPort    string
	)
	if username == "" {
		u, err := user.Current()
		if err == nil {
			username = u.Username
		}
	}

	if host != "localhost" {
		protocol = "tcp"
		hostAndPort = fmt.Sprintf("%s:%d", host, port)
	} else {
		hostAndPort = socket
		password = ""
	}

	if password != "" {
		password = ":" + password
	}

	if threads <= 0 {
		threads = defaultThreads
	}

	if batch <= 0 {
		batch = defaultBatch
	}

	if tables <= 0 {
		tables = defaultTables
	}

	b := brim{
		dsn:           fmt.Sprintf("%s%s@%s(%s)/%s%s", username, password, protocol, hostAndPort, database, dsnOptions),
		database:      database,
		tableBaseName: "brim",
		threads:       threads,
		batch:         batch,
		tables:        tables,
		engine:        engine,
	}

	if size != "" {
		r, err := sizeToRows(size)
		if err != nil {
			log.Fatalln(err.Error())
		}
		rows = r
	} else if rows <= 0 {
		rows = defaultRows
	}
	b.rows = rows
	var err error

	if b.batch > b.rows {
		return nil, fmt.Errorf("batch size, %d cannot be larger than the total rows %d", b.batch, b.rows)
	}

	b.db, err = sql.Open("mysql", b.dsn)
	if err != nil {
		return nil, err
	}

	if err = b.db.Ping(); err != nil {
		return nil, err
	}

	jobs := [][]int{}
	var (
		j int = 1
		k int = b.tables
	)
	for i := b.rows - 1; i >= 0; i = i - batch {
		if batch > i {
			jobs = append(jobs, []int{j, i})
			break
		} else {
			jobs = append(jobs, []int{j, batch})
		}
		if j >= k {
			j = 1
		} else {

			j++
		}
	}
	b.jobs = jobs

	return &b, nil
}

func (b *brim) run() error {
	err := b.createDatabase()
	if err != nil {
		return err
	}

	err = b.createTables()
	if err != nil {
		return err
	}

	log.Printf("Loading %d rows, into %d table(s), batch size of %d, over %d jobs and %d threads\n", b.rows, b.tables, b.batch, len(b.jobs), b.threads)

	jobCount := len(b.jobs)
	jobs := make(chan int, jobCount)
	jobResults := make(chan int, jobCount)

	for worker := 1; worker <= b.threads; worker++ {
		go func(id int, jobs <-chan int, results chan<- int) {
			for i := range jobs {
				tableName := fmt.Sprintf("%s.%s%d", b.database, b.tableBaseName, id)
				stmt, err := b.db.Prepare("INSERT INTO " + tableName + " (b,c,d,e,f) VALUES (?, ?, ?, ?, ?)")
				if err != nil {
					continue
				}
				defer stmt.Close()
				b.loadTable(stmt, b.jobs[i][1])
				results <- i
			}
		}(worker, jobs, jobResults)
	}

	for j := 0; j <= jobCount-1; j++ {
		jobs <- j
	}
	close(jobs)

	for r := 0; r <= jobCount-1; r++ {
		<-jobResults
	}

	return nil
}

func main() {
	var (
		host     = kingpin.Flag("host", "MariaDB hostname or IP address").Default("localhost").Envar("BRIM_HOST").String()
		port     = kingpin.Flag("port", "MariaDB TCP/IP Port").Envar("BRIM_PORT").Int()
		username = kingpin.Flag("username", "MariaDB username").Default("").Envar("BRIM_USER").String()
		password = kingpin.Flag("password", "MariaDB username").Default("").Envar("BRIM_PASSWORD").String()
		socket   = kingpin.Flag("socket", "Path to MariaDB server socket").Default("/run/mysqld/mysqld.sock").Envar("BRIM_SOCKET").String()
		database = kingpin.Flag("database", "Database to use when creating tables").Default("brim").Envar("BRIM_DB").String()
		engine   = kingpin.Flag("engine", "Engine to use when create tables").Default("INNODB").Envar("BRIM_ENGINE").String()
		size     = kingpin.Flag("size", "Size of the dataset to be loaded across all tables e.g. 100MB, 123GB, 2.4TB").Default("").Envar("BRIM_SIZE").String()
		rows     = kingpin.Flag("rows", "Total number of rows to be inserted across all tables. Each rows is around 1 Kilobyte").Envar("BRIM_ROWS").Int()
		batch    = kingpin.Flag("batch", "Number of rows to insert per-batch").Envar("BRIM_BATCH").Int()
		tables   = kingpin.Flag("tables", "Number of tables to distribute inserts between").Envar("BRIM_TABLES").Int()
		threads  = kingpin.Flag("threads", "Number of concurrent threads to insert row batches").Envar("BRIM_THREADS").Int()
	)

	kingpin.Version(Version)
	kingpin.CommandLine.UsageWriter(os.Stdout)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	b, err := NewBrim(*username, *password, *host, *port, *socket, *database, *engine, *size, *rows, *batch, *tables, *threads)
	if err != nil {
		log.Fatalln(err)
	}
	defer b.db.Close()

	err = b.run()
	if err != nil {
		log.Fatal(err)
	}

}
