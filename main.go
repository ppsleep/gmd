package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/manifoldco/promptui"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"net"
	"os"
	"strconv"
)

// ConfigStruct struct
type ConfigStruct struct {
	Source ConfStruct
	Target ConfStruct
}

// ConfStruct struct
type ConfStruct struct {
	Mode       string
	Host       string
	Port       interface{}
	User       string
	Password   string
	PrivateKey string
	DBHost     string
	DBPort     interface{}
	DBUser     string
	DBPassword string
	Database   string
	Charset    string
}

// Client struct
type Client struct {
	client *ssh.Client
}

// ViaSSHDialer struct
type ViaSSHDialer struct {
	client *ssh.Client
	_      *context.Context
}

// ColumnStruct struct
type ColumnStruct struct {
	Field      string
	Type       string
	Collation  interface{}
	Null       string
	Key        string
	Default    interface{}
	Extra      string
	Privileges string
	Comment    string
}

// Dial ssh dialer
func (v *ViaSSHDialer) Dial(context context.Context, addr string) (net.Conn, error) {
	return v.client.Dial("tcp", addr)
}

var (
	// Config struct
	Config ConfigStruct
	// Source source db
	Source *sql.DB
	// Target target db
	Target *sql.DB

	sourceTables       = map[string]int{}
	targetTables       = map[string]int{}
	sourceColumnTables = map[string]map[string]ColumnStruct{}
	targetColumnTables = map[string]map[string]ColumnStruct{}
	newColumnTables    = map[string]map[string]ColumnStruct{}
)

func usage() {
	fmt.Fprintf(os.Stderr,
		`MySQL Diff
Usage: gmd <configfile>

Options:
`)
	flag.PrintDefaults()
}

func main() {
	h := false
	flag.BoolVar(&h, "h", false, "this help")
	flag.Parse()
	flag.Usage = usage
	if len(flag.Args()) == 0 {
		h = true
	}
	if h {
		flag.Usage()
		return
	}
	file := flag.Args()[0]

	decode(file)
}

func decode(file string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("config file does not exist")
		return
	}
	json.Unmarshal(content, &Config)
	run()
}

func formatPort(port interface{}) string {
	if p, ok := port.(string); ok {
		return p
	}
	if p, ok := port.(float64); ok {
		return strconv.FormatFloat(p, 'f', 0, 64)
	}
	return ""
}

func run() {
	var err error
	Source, err = connect(true)
	if err != nil {
		fmt.Println("Source connect error:", err)
		return
	}
	Target, err = connect(false)
	if err != nil {
		fmt.Println("Target connect error:", err)
		return
	}
	var table string
	rows, err := Source.Query("SHOW TABLES")
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		rows.Scan(&table)
		sourceTables[table] = 1
	}
	// target
	rows, err = Target.Query("SHOW TABLES")
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		rows.Scan(&table)
		targetTables[table] = 1
	}
	// new table in source
	for k := range sourceTables {
		if targetTables[k] != 1 {
			sourceTables[k] = -1
		}
	}
	// redundant table in target
	for k := range targetTables {
		if sourceTables[k] == 0 {
			renameOrDelete(k)
		}
	}
	// diff
	for k, v := range sourceTables {
		if v == -1 {
			create(k)
		} else {
			diff(k)
		}
	}

	fmt.Println("Done")
}

func renameOrDelete(table string) {
	lable := fmt.Sprintf("The table `%s` is existing on the target database but does not exist in the source database. Please select your operation", table)
	prompt := promptui.Select{
		Label: lable,
		Items: []string{
			"Skip",
			"Delete the table `" + table + "`",
			"Rename the table `" + table + "`",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		deleteTable(table)
	} else if index == 2 {
		renameTable(table)
	}
}

func deleteTable(table string) {
	lable := fmt.Sprintf("The table `%s` cannot be recovered after deletion, please confirm:", table)
	prompt := promptui.Select{
		Label: lable,
		Items: []string{
			"Go back to reselect",
			"Delete the table `" + table + "`",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		deleteTableByName(table)
	} else {
		renameOrDelete(table)
	}
}

func deleteTableByName(table string) {
	_, err := Target.Exec("DROP TABLE `" + table + "`")
	if err != nil {
		fmt.Println(err)
		renameOrDelete(table)
	} else {
		fmt.Printf("The table `%s` is deleted\n", table)
	}
}

func renameTable(table string) {
	prompt := promptui.Select{
		Label: "Please select your operation",
		Items: []string{
			"Go back to reselect",
			"Select a table name from the source database",
			"Input a new table name",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		item := []string{
			"Go back to reselect",
		}
		for k, v := range sourceTables {
			if v == -1 {
				item = append(item, k)
			}
		}
		prompt := promptui.Select{
			Label: "Please select a table name or back to reselect",
			Items: item,
		}
		index, rename, _ := prompt.Run()
		if index == 0 {
			renameTable(table)
		} else {
			renameTableByName(table, rename, false)
		}
	} else if index == 2 {
		prompt := promptui.Prompt{
			Label: "Please input a new table name",
		}
		result, err := prompt.Run()
		if err != nil {
			fmt.Println(err)
			renameTable(table)
		} else {
			renameTableByName(table, result, true)
		}
	} else {
		renameOrDelete(table)
	}
}

func renameTableByName(table, rename string, newname bool) {
	sql := fmt.Sprintf("RENAME TABLE `%s` TO `%s`", table, rename)
	_, err := Target.Exec(sql)
	if err != nil {
		fmt.Println(err)
		renameTable(table)
		return
	}
	if !newname {
		sourceTables[rename] = 1
	}
}

func diff(table string) {
	fmt.Printf("Diff table `%s`...\n", table)
	rows, err := Target.Query("SHOW FULL COLUMNS FROM `" + table + "`")
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	targetColumnTables[table] = map[string]ColumnStruct{}
	for rows.Next() {
		var column ColumnStruct
		rows.Scan(
			&column.Field,
			&column.Type,
			&column.Collation,
			&column.Null,
			&column.Key,
			&column.Default,
			&column.Extra,
			&column.Privileges,
			&column.Comment,
		)
		targetColumnTables[table][column.Field] = column
	}
	rows, err = Source.Query("SHOW FULL COLUMNS FROM `" + table + "`")
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	sourceColumnTables[table] = map[string]ColumnStruct{}
	newColumnTables[table] = map[string]ColumnStruct{}
	for rows.Next() {
		var column ColumnStruct
		rows.Scan(
			&column.Field,
			&column.Type,
			&column.Collation,
			&column.Null,
			&column.Key,
			&column.Default,
			&column.Extra,
			&column.Privileges,
			&column.Comment,
		)
		sourceColumnTables[table][column.Field] = column
		if targetColumnTables[table][column.Field].Field == "" {
			newColumnTables[table][column.Field] = column
		}
	}
	for k := range targetColumnTables[table] {
		if sourceColumnTables[table][k].Field == "" {
			renameOrDeleteColumn(k, table)
		}
	}
}

func renameOrDeleteColumn(column, table string) {
	lable := fmt.Sprintf("The field `%s` is existing on the target table `%s` but does not exist in the source database. Please select your operation", column, table)
	prompt := promptui.Select{
		Label: lable,
		Items: []string{
			"Skip",
			"Delete the field `" + column + "`",
			"Rename the field `" + column + "`",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		deleteColumn(column, table)
	} else if index == 2 {
		renameColumn(column, table)
	}
}

func deleteColumn(column, table string) {
	lable := fmt.Sprintf("The field `%s` cannot be recovered after deletion, please confirm:", column)
	prompt := promptui.Select{
		Label: lable,
		Items: []string{
			"Go back to reselect",
			"Delete the field `" + column + "`",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		deleteColumnFromTable(column, table)
	} else {
		renameOrDelete(table)
	}
}

func renameColumn(column, table string) {
	prompt := promptui.Select{
		Label: "Please select your operation",
		Items: []string{
			"Go back to reselect",
			"Select a field name from the source table `" + table + "`",
			"Input a new field name",
		},
	}
	index, _, _ := prompt.Run()
	if index == 1 {
		item := []string{
			"Go back to reselect",
		}
		for k := range newColumnTables[table] {
			item = append(item, k)
		}
		prompt := promptui.Select{
			Label: "Please select a field name or back to reselect",
			Items: item,
		}
		index, rename, _ := prompt.Run()
		if index == 0 {
			renameColumn(column, table)
		} else {
			renameColumnFromTable(column, rename, table, false)
		}
	} else if index == 2 {
		prompt := promptui.Prompt{
			Label: "Please input a new field name",
		}
		result, err := prompt.Run()
		if err != nil {
			fmt.Println(err)
			renameColumn(column, table)
		} else {
			renameColumnFromTable(column, result, table, true)
		}
	} else {
		renameOrDeleteColumn(column, table)
	}
}

func deleteColumnFromTable(column, table string) {
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP `%s`", table, column)
	_, err := Target.Exec(sql)
	if err != nil {
		fmt.Println(err)
		renameOrDeleteColumn(column, table)
	} else {
		fmt.Printf("The field `%s` is deleted\n", column)
	}
}

func renameColumnFromTable(column, rename, table string, newname bool) {
	Type := targetColumnTables[table][column].Type
	if !newname {
		Type = sourceColumnTables[table][rename].Type
	}
	sql := fmt.Sprintf("ALTER TABLE `%s` CHANGE `%s` `%s` %s", table, column, rename, Type)
	_, err := Target.Exec(sql)
	if err != nil {
		fmt.Println(err)
		renameColumn(column, table)
		return
	}
	if !newname {
		delete(newColumnTables[table], rename)
	}
}

func create(table string) {
	fmt.Printf("Table `%s` does not exist, creating...\n", table)
	var name, sql string
	err := Source.QueryRow("SHOW CREATE TABLE `"+table+"`").Scan(&name, &sql)
	if err != nil {
		fmt.Printf("Table `%s` export failed\n", table)
		return
	}
	_, err = Target.Exec(sql)
	if err != nil {
		fmt.Printf("Table `%s` create failed: %s\n", table, err)
		return
	}
	fmt.Printf("Table `%s` create succeed\n", table)
}

func connect(source bool) (*sql.DB, error) {
	conf := Config.Target
	if source {
		conf = Config.Source
	}
	port := formatPort(conf.Port)
	dialer := conf.Mode

	if conf.Mode == "ssh" {
		dialer = "mysql+tcp"
		var client *ssh.Client
		var err error
		if conf.PrivateKey != "" {
			client, err = dialWithPrivateKey(conf.Host+":"+port, conf.User, conf.PrivateKey)
		} else {
			client, err = dialWithPassword(conf.Host+":"+port, conf.User, conf.Password)
		}
		if err != nil {
			return nil, err
		}
		mysql.RegisterDialContext(dialer, (&ViaSSHDialer{client, nil}).Dial)
	}

	dbPort := formatPort(conf.DBPort)
	conStr := conf.DBUser + ":" + conf.DBPassword + "@" + dialer + "(" + conf.DBHost + ":" + dbPort + ")/" + conf.Database + "?charset=" + conf.Charset
	db, err := sql.Open("mysql", conStr)
	if err != nil {
		db.Close()
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func dialWithPassword(addr, user, passwd string) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(passwd),
		},
		HostKeyCallback: ssh.HostKeyCallback(
			func(hostname string, target net.Addr, key ssh.PublicKey) error {
				return nil
			},
		),
	}
	return ssh.Dial("tcp", addr, config)
}

func dialWithPrivateKey(addr, user, keyfile string) (*ssh.Client, error) {
	key, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return nil, err
	}
	signature, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signature),
		},
		HostKeyCallback: ssh.HostKeyCallback(
			func(hostname string, target net.Addr, key ssh.PublicKey) error {
				return nil
			},
		),
	}
	return ssh.Dial("tcp", addr, config)
}
