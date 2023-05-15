package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	_ "github.com/go-sql-driver/mysql"
)

var (
	Version           string = ""
	defaultSocketPath string
	defaultUserName   string
)

type brim struct {
	dsn           string
	db            *sql.DB
	database      string
	rows          int64
	batch         int64
	tables        int
	threads       int
	tableBaseName string
	engine        string
	start         time.Time
}

func NewBrim(username, password string, host string, port, connections int, socket, database, engine, size string, rows, batch int64, tables, threads int) (*brim, error) {
	var (
		defaultRows    int64  = 10000
		defaultBatch   int64  = 1000
		defaultTables  int    = 10
		defaultThreads int    = 10
		protocol       string = "unix"
		dsnOptions     string = "?multiStatements=true&autocommit=true&maxAllowedPacket=0"
		hostAndPort    string
	)

	if host != "localhost" || socket == "" {
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
		dsn:           fmt.Sprintf("%s%s@%s(%s)/%s", username, password, protocol, hostAndPort, dsnOptions),
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

	b.db.SetMaxOpenConns(connections)

	return &b, nil
}

func sizeToFloat(s string) (float64, error) {
	ns := s[:len(s)-2]
	n, err := strconv.ParseFloat(ns, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func sizeToRows(s string) (int64, error) {
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
		return int64(rows), err
	}

	rows = rows * m

	return int64(rows), nil
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

func generateRow() string {
	var limit = 2147483647
	// 1000000000 x genRow ~= 1TB
	r := rand.New(rand.NewSource(64))
	b := strconv.Itoa(r.Intn(limit))
	c := randomString(r)
	d := randomString(r)
	e := randomString(r)
	f := randomString(r)
	data := fmt.Sprintf("(%s,'%s','%s','%s','%s')", b, c, d, e, f)
	return data
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
	tableName := b.database + "." + name
	create := fmt.Sprintf(`DROP TABLE IF EXISTS %s; CREATE TABLE IF NOT EXISTS %s (
a bigint(20) NOT NULL AUTO_INCREMENT,
b int(11) NOT NULL,
c char(255) NOT NULL,
d char(255) NOT NULL,
e char(255) NOT NULL,
f char(255) NOT NULL,
PRIMARY KEY (a),
	INDEX (b)) ENGINE=%s;`, tableName, tableName, b.engine)
	err := b.insertRow(create)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) createTables() error {
	log.Printf("Creating %d tables\n", b.tables)
	for i := 0; i <= b.tables; i++ {
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

func generateBatch(rows int64) string {
	batch := make([]string, rows)
	for i := range batch {
		r := generateRow()
		batch[i] = r
	}

	joinedBatch := strings.Join(batch, ",")
	return joinedBatch
}

// Load table will generate a batch of data using generateRow and load the target table.
func (b *brim) loadTable(tableID int, batchSize int64, rows <-chan int64, result chan<- int64) {
	tableName := fmt.Sprintf("%s.%s%d", b.database, b.tableBaseName, tableID)
	var query string = "INSERT INTO %s (b,c,d,e,f) VALUES %s"
	data := generateBatch(batchSize)
	row := fmt.Sprintf(query, tableName, data)
	r, err := b.db.Exec(row)
	if err != nil {
		log.Fatal(err)
	}
	inserted, err := r.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	result <- inserted
}

func (b *brim) load() {
	var batch int64 = b.batch

	rows := make(chan int64, b.rows)
	result := make(chan int64, b.rows)

	for t := 1; t <= b.tables; t++ {
		go b.loadTable(t, batch, rows, result)
	}

	for j := int64(0); j <= b.rows; j += batch {
		rows <- j
		if j+batch > b.rows {
			diff := (j + batch) - b.rows
			batch += diff
			log.Printf("Remaining rows for last batch: %d", batch)
		}
	}
	close(rows)

	for r := int64(0); r <= b.rows; r += batch {
		<-result
		if r+batch > b.rows {
			diff := (r + batch) - b.rows
			batch += diff
			log.Printf("Remaining rows for last batch: %d", batch)
		}
	}

}

func (b *brim) countRows() {
	var total int64 = 0
	for t := 1; t <= b.tables; t++ {
		var c int64 = 0
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s%d", b.database, b.tableBaseName, t)
		if err := b.db.QueryRow(q).Scan(&c); err != nil {
			log.Fatal(err)
		}
		fmt.Println(q, c)
		total += c

	}
	log.Printf("Total rows: %d", total)
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

	b.start = time.Now()

	log.Printf("Loading rows: %d, tables: %d, batch: %d, threads: %d\n", b.rows, b.tables, b.batch, b.threads)

	b.load()

	log.Printf("Time: %s", time.Since(b.start))

	return nil
}

func init() {
	if runtime.GOOS == "linux" {
		defaultSocketPath = "/run/mysqld/mysqld.sock"
	} else {
		defaultSocketPath = "/tmp/mysql.sock"
	}

	if _, err := os.Stat(defaultSocketPath); err != nil {
		defaultSocketPath = ""
	}
	u, err := user.Current()
	if err == nil {
		defaultUserName = u.Username
	} else {
		defaultUserName = ""
	}
}

func main() {
	var (
		host        = kingpin.Flag("host", "MariaDB hostname or IP address").Short('h').Default("localhost").Envar("BRIM_HOST").String()
		port        = kingpin.Flag("port", "MariaDB TCP/IP Port").Short('P').Envar("BRIM_PORT").Int()
		username    = kingpin.Flag("user", "MariaDB username").Short('u').Default(defaultUserName).Envar("BRIM_USER").String()
		password    = kingpin.Flag("password", "MariaDB password").Short('p').Default("").Envar("BRIM_PASSWORD").String()
		socket      = kingpin.Flag("socket", "Path to MariaDB server socket").Default(defaultSocketPath).Envar("BRIM_SOCKET").String()
		connections = kingpin.Flag("connections", "Max connections to the MariaDB Database server").Short('c').Envar("BRIM_CONNECTIONS").Int()
		database    = kingpin.Flag("database", "Database to use when creating tables").Short('D').Default("brim").Envar("BRIM_DB").String()
		engine      = kingpin.Flag("engine", "Engine to use when create tables").Default("INNODB").Envar("BRIM_ENGINE").String()
		size        = kingpin.Flag("size", "Size of the dataset to be loaded across all tables e.g. 100MB, 123GB, 2.4TB").Short('s').Default("").Envar("BRIM_SIZE").String()
		rows        = kingpin.Flag("rows", "Total number of rows to be inserted across all tables. Each rows is around 1 Kilobyte").Short('r').Envar("BRIM_ROWS").Int64()
		batch       = kingpin.Flag("batch", "Number of rows to insert per-batch").Short('b').Envar("BRIM_BATCH").Int64()
		tables      = kingpin.Flag("tables", "Number of tables to distribute inserts between").Short('t').Envar("BRIM_TABLES").Int()
		threads     = kingpin.Flag("threads", "Number of concurrent threads to insert row batches").Envar("BRIM_THREADS").Int()
	)

	kingpin.Version(Version)
	kingpin.CommandLine.UsageWriter(os.Stdout)
	//kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	b, err := NewBrim(*username, *password, *host, *port, *connections, *socket, *database, *engine, *size, *rows, *batch, *tables, *threads)
	if err != nil {
		log.Fatalln(err)
	}
	defer b.db.Close()

	if err = b.run(); err != nil {
		log.Fatal(err)
	}

	b.countRows()

}
