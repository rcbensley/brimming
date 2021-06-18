package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/user"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	username      string
	protocol      string
	URI           string
	sessionSQL    string = "SET SESSION sql_log_bin=0"
	dsnOptions    string = "?multiStatements=true&autocommit=true&maxAllowedPacket=0"
	socketFlag           = flag.String("socket", "/tmp/mysql.sock", "Path to socket file")
	hostFlag             = flag.String("host", "localhost", "MariaDB hostname or IP address")
	portFlag             = flag.Int("port", 3306, "MariaDB server port")
	userFlag             = flag.String("user", "", "MariaDB server user")
	passwordFlag         = flag.String("password", "", "MariaDB server password")
	databaseFlag         = flag.String("database", "brim", "Database schema")
	rowsFlag             = flag.Int("rows", 1000000, "Total number of rows to be inserted. Each row is around 1 Kilobyte")
	batchSizeFlag        = flag.Int("batch", 1000, "Number of rows to insert per-batch")
	tablesFlag           = flag.Int("tables", 1, "Number of tables to distribute inserts between")
	threadsFlag          = flag.Int("threads", 100, "Number of concurrent threads to insert row batches")
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
	for i := 1; i <= b.tables; i++ {
		err := b.createTable(fmt.Sprintf("%s%d", b.tableBaseName, i))
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

func generateBatch(rows int) string {
	batch := make([]string, rows)
	for i := range batch {
		r := generateRow()
		batch[i] = r
	}

	joinedBatch := strings.Join(batch, ",")
	return joinedBatch
}

// Load table will generate a batch of data using generateRow and load the target table.
func (b *brim) loadTable(table int, rows int) error {
	data := generateBatch(rows)
	tableName := fmt.Sprintf("%s%d", b.tableBaseName, table)
	row := fmt.Sprintf("INSERT INTO %s.%s (b,c,d,e,f) VALUES %s", b.database, tableName, data)
	err := b.insertRow(row)
	if err != nil {
		return err
	}
	return nil
}

func (b *brim) new() error {
	var err error
	b.db, err = sql.Open("mysql", b.dsn)
	if err != nil {
		return err
	}

	if err = b.db.Ping(); err != nil {
		return err
	}

	jobs := [][]int{}
	j := 1
	k := b.tables
	for i := b.rows - 1; i >= 0; i = i - b.batch {
		if b.batch > i {
			jobs = append(jobs, []int{j, i})
			break
		} else {
			jobs = append(jobs, []int{j, b.batch})
		}
		if j >= k {
			j = 1
		} else {

			j++
		}
	}
	b.jobs = jobs

	return nil
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

	log.Printf("Loading %d rows into %d table(s), batch size of %d, over %d jobs and %d threads\n", b.rows, b.tables, b.batch, len(b.jobs), b.threads)

	jobCount := len(b.jobs)
	jobs := make(chan int, jobCount)
	jobResults := make(chan int, jobCount)

	for worker := 1; worker <= b.threads; worker++ {
		go func(id int, jobs <-chan int, results chan<- int) {
			for i := range jobs {
				b.loadTable(b.jobs[i][0], b.jobs[i][1])
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

func init() {
	if *userFlag == "" {
		u, err := user.Current()
		if err != nil {
			log.Println("Unable to determine username, using root")
			username = "root"
		} else {
			username = u.Username
		}
	} else {
		username = *userFlag
	}

	if *hostFlag != "localhost" {
		protocol = "tcp"
		URI = fmt.Sprintf("%s:%d", *hostFlag, *portFlag)
	} else {
		protocol = "unix"
		URI = *socketFlag
	}

	if *passwordFlag != "" {
		username = fmt.Sprintf("%s:%s", username, *passwordFlag)
	}

	if *rowsFlag <= 0 {
		log.Fatalln("Specify at least 1 row to be inserted ...")
	}

	if *threadsFlag <= 0 {
		log.Fatalln("Specify at least 1 thread ...")
	}

	if *batchSizeFlag > *rowsFlag {
		log.Fatalln("Batch size cannot be larger than the total rows ...")
	}

	if *batchSizeFlag <= 1 {
		log.Fatalln("Batch size needs to be greater than 1 ...")
	}

	if *tablesFlag <= 0 {
		log.Fatalln("At least 1 table needs to be specified, so the data can go somewhere ...")
	}
}

func main() {
	flag.Parse()
	b := brim{
		dsn:           fmt.Sprintf("%s@%s(%s)/%s", username, protocol, URI, dsnOptions),
		rows:          *rowsFlag,
		database:      *databaseFlag,
		tableBaseName: "brim",
		threads:       *threadsFlag,
		batch:         *batchSizeFlag,
		tables:        *tablesFlag,
	}

	err := b.new()
	if err != nil {
		log.Fatal(err)
	}
	defer b.db.Close()

	err = b.run()
	if err != nil {
		log.Fatal(err)
	}
}
