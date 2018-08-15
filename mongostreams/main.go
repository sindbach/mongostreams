package main

import (
	"fmt"
	"log"
	"os"

	"github.com/akamensky/argparse"
	"github.com/sindbach/mongostreams"
)

func main() {
	parser := argparse.NewParser("mongostreams", " ... ")

	dirPtr := parser.String("d", "directory", &argparse.Options{Required: true,
		Help: "Specify a working directory"})
	configPtr := parser.String("c", "config", &argparse.Options{Required: true,
		Help: "Specify a configuration JSON file"})
	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	options := mongostreams.Options{Directory: *dirPtr, Config: *configPtr}
	if _, err := os.Stat(options.Directory); os.IsNotExist(err) {
		log.Fatal("Working directory is unreachable.")
	}
	if _, err := os.Stat(options.Config); os.IsNotExist(err) {
		log.Fatal("Configuration file is unreachable.")
	}
	err = mongostreams.Execute(options)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Done")
}
