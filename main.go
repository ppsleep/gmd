package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"net"
	"os"
	"strconv"
)

// ConfigStruct struct
type ConfigStruct struct {
	Local  ConfStruct
	Remote ConfStruct
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
}

var (
	// Config struct
	Config ConfigStruct
	// Local local db
	Local *sql.DB
	// Remote local db
	Remote *sql.DB
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
	Local = connect(true)
	Remote = connect(false)
}

func connect(local bool) *sql.DB {
	conf := Config.Remote
	if local {
		conf = Config.Local
	}
	if conf.Mode == "tcp" {
		dbPort := formatPort(conf.DBPort)
		conStr := conf.DBUser + ":" + conf.DBPassword + "@tcp(" + conf.DBHost + ":" + dbPort + ")/" + conf.Database + "?charset=" + conf.Charset
		db, err := sql.Open("mysql", conStr)
		if err != nil {
			fmt.Println("MySQL Error:", err)
			db.Close()
			return nil
		}
		err = db.Ping()
		if err != nil {
			db.Close()
			fmt.Println("MySQL Connect Error:", err)
			return nil
		}
		return db
	} else if conf.Mode == "ssh" {
		port := formatPort(conf.Port)
		// dbPort := formatPort(conf.DBPort)
		var client *ssh.Client
		var err error
		if conf.PrivateKey != "" {
			client, err = dialWithPrivateKey(conf.Host+":"+port, conf.User, conf.PrivateKey)
		} else {
			client, err = dialWithPassword(conf.Host+":"+port, conf.User, conf.Password)
		}
		if err != nil {
			return nil
		}
		session, err := client.NewSession()
		defer session.Close()
		cmd, err := session.CombinedOutput("cd /; ls")
		fmt.Println(string(cmd), err)
	}
	return nil
}

func dialWithPassword(addr, user, passwd string) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(passwd),
		},
		HostKeyCallback: ssh.HostKeyCallback(
			func(hostname string, remote net.Addr, key ssh.PublicKey) error {
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
			func(hostname string, remote net.Addr, key ssh.PublicKey) error {
				return nil
			},
		),
	}
	return ssh.Dial("tcp", addr, config)
}
