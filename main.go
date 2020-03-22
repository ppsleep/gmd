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
	sourceTables := map[string]int{}
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
	//targetTables := map[string]int{}
	rows, err = Target.Query("SHOW TABLES")
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		rows.Scan(&table)
		if sourceTables[table] == 1 {
			diff(table)
		} else {
			renameOrDelete(table)
		}
		//targetTables[table] = 1
	}
	// for rows.Next() {
	// 	rows.Scan(&table)
	// 	if targetTables[table] == 1 {
	// 		diff(table)
	// 	} else {
	// 		create(table)
	// 	}
	// }
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
	} else {
		fmt.Printf("The table `%s` is deleted\n", table)
	}
}

func renameTable(table string) {

}

func diff(table string) {
	fmt.Printf("Diff table `%s`...\n", table)
}

func create(table string) {
	fmt.Printf("Table `%s` does not exist, creating...\n", table)
	var name, sql string
	err := Source.QueryRow("SHOW CREATE TABLE "+table).Scan(&name, &sql)
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
