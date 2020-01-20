package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/fperf/fperf"
	// use the mysql driver
	_ "github.com/go-sql-driver/mysql"
)

const seqPlaceHolder = "__seq_int__"
const randPlaceHolder = "__rand_int__"
const randRangePlaceHolder = "__rand_range__" // 0 to rmax

var seq = seqCreater(0)
var random = randCreater(10000000000000000)
var randRange = randCreater(0)

func seqCreater(begin int64) func() string {
	// filled map, filled generated to 16 bytes
	l := []string{
		"",
		"0",
		"00",
		"000",
		"0000",
		"00000",
		"000000",
		"0000000",
		"00000000",
		"000000000",
		"0000000000",
		"00000000000",
		"000000000000",
		"0000000000000",
		"00000000000000",
		"000000000000000",
	}
	v := begin
	m := &sync.Mutex{}
	return func() string {
		m.Lock()
		s := strconv.FormatInt(v, 10)
		v += 1
		m.Unlock()

		filled := len(l) - len(s)
		if filled <= 0 {
			return s
		}
		return l[filled] + s
	}
}

func randCreater(max int64) func() string {
	// filled map, filled generated to 16 bytes
	l := []string{
		"",
		"0",
		"00",
		"000",
		"0000",
		"00000",
		"000000",
		"0000000",
		"00000000",
		"000000000",
		"0000000000",
		"00000000000",
		"000000000000",
		"0000000000000",
		"00000000000000",
		"000000000000000",
	}
	return func() string {
		s := strconv.FormatInt(rand.Int63n(max), 10)
		filled := len(l) - len(s)
		if filled <= 0 {
			return s
		}
		return l[filled] + s
	}
}

func replaceSeq(s string) string {
	return strings.Replace(s, seqPlaceHolder, seq(), -1)
}
func replaceRand(s string) string {
	return strings.Replace(s, randPlaceHolder, random(), -1)
}
func replaceRandRange(s string) string {
	return strings.Replace(s, randRangePlaceHolder, randRange(), -1)
}

func replace(s string) string {
	if strings.Index(s, seqPlaceHolder) >= 0 {
		s = replaceSeq(s)
	}
	if strings.Index(s, randPlaceHolder) >= 0 {
		s = replaceRand(s)
	}
	if strings.Index(s, randRangePlaceHolder) >= 0 {
		s = replaceRandRange(s)
	}
	return s
}

type Kind string

const (
	Query = Kind("query")
	Exec  = Kind("exec")
)

type Statement struct {
	sql  string
	kind Kind
}

type options struct {
	isolation int
	readonly  bool
	stdin     bool
	rmax      int64
}
type Client struct {
	options

	db *sql.DB
	s  []*Statement
}

func loadStdin() []string {
	var sqls []string
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		clauses := strings.Split(scanner.Text(), ";")
		if len(clauses) == 0 {
			continue
		}
		if clauses[len(clauses)-1] == "" {
			clauses = clauses[0 : len(clauses)-1]
		}
		sqls = append(sqls, clauses...)
	}
	return sqls
}

func New(flag *fperf.FlagSet) fperf.Client {
	c := &Client{}

	flag.Usage = func() {
		fmt.Printf("Usage: mysql <sqls...>\n use __rand_int__ or __seq_int__ to generate random or sequence keys\n")
	}
	flag.IntVar(&c.options.isolation, "isolation", 0, "isolation level")
	flag.BoolVar(&c.options.readonly, "readonly", false, "readonly transaction")
	flag.BoolVar(&c.options.stdin, "stdin", false, "read sqls from stdin")
	flag.Int64Var(&c.options.rmax, "rmax", 0, "max value of __rand_range__")
	flag.Parse()

	if c.options.rmax > 0 {
		randRange = randCreater(c.options.rmax)
	}

	args := flag.Args()
	if len(args) == 0 && !c.options.stdin {
		flag.Usage()
		os.Exit(0)
	}

	var sqls []string
	if len(args) > 0 {
		sqls = strings.Split(strings.TrimSpace(args[0]), ";")
		if sqls[len(sqls)-1] == "" {
			sqls = sqls[0 : len(sqls)-1]
		}
	}
	if c.options.stdin {
		sqls = append(sqls, loadStdin()...)
	}

	for _, sql := range sqls {
		tokens := strings.Fields(sql)
		switch strings.ToLower(tokens[0]) {
		case "select", "show":
			c.s = append(c.s, &Statement{sql: sql, kind: Query})
		case "insert", "delete", "update", "create", "drop":
			c.s = append(c.s, &Statement{sql: sql, kind: Exec})
		default:
			fmt.Println("unkown sql:", sql)
			os.Exit(-1)
		}
	}

	return c
}

func (c *Client) Dial(addr string) error {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return err
	}
	c.db = db
	return nil
}
func (c *Client) Request() error {
	ctx := context.Background()
	txn, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	for _, s := range c.s {
		var err error
		var rows *sql.Rows
		sql := replace(s.sql)
		switch s.kind {
		case Query:
			rows, err = txn.Query(sql)
			if rows != nil {
				for rows.Next() {
				}
				rows.Close()
			}
		case Exec:
			_, err = txn.Exec(sql)
		}
		if err != nil {
			return err
		}
	}
	return txn.Commit()
}

func init() {
	//rand.Seed(time.Now().UnixNano())
	fperf.Register("mysql", New, "mysql performance benchmark")
}
