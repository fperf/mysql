package redis

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shafreeck/fperf"
)

const seqPlaceHolder = "__seq_int__"
const randPlaceHolder = "__rand_int__"

var seq func() string = seqCreater(0)
var random func() string = randCreater(10000000000000000)

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

func replace(s string) string {
	if strings.Index(s, seqPlaceHolder) >= 0 {
		s = replaceSeq(s)
	}
	if strings.Index(s, randPlaceHolder) >= 0 {
		s = replaceRand(s)
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
}
type Client struct {
	options

	db *sql.DB
	s  []*Statement
}

func New(flag *fperf.FlagSet) fperf.Client {
	c := &Client{}

	flag.Usage = func() {
		fmt.Printf("Usage: mysql <sqls...>\n use __rand_int__ or __seq_int__ to generate random or sequence keys\n")
	}
	flag.IntVar(&c.options.isolation, "isolation", 0, "isolation level")
	flag.BoolVar(&c.options.readonly, "readonly", false, "readonly transaction")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(0)
	}

	sqls := strings.Split(strings.TrimSpace(args[0]), ";")

	for _, sql := range sqls {
		tokens := strings.Fields(sql)
		switch strings.ToLower(tokens[0]) {
		case "select", "show":
			c.s = append(c.s, &Statement{sql: sql, kind: Query})
		case "insert", "delete", "update":
			c.s = append(c.s, &Statement{sql: sql, kind: Exec})
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
