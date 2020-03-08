package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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
	Port       string
	User       string
	Password   string
	PrivateKey string
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	Database   string
	Charset    string
}

// Config struct
var Config ConfigStruct

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
}
