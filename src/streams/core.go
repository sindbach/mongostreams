package main

import (
	"context"
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/sbinet/go-python"
	"github.com/tkanos/gonfig"
	"log"
	"os"
)

type Options struct {
	Directory string
	Config    string
}

type Configuration struct {
	URI        string
	Database   string
	Collection string
}

func init() {
	err := python.Initialize()
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	parser := argparse.NewParser("mongo-streams", " ... ")

	dirPtr := parser.String("d", "directory", &argparse.Options{Required: true, Help: "Specify a working directory"})
	configPtr := parser.String("c", "config", &argparse.Options{Required: true, Help: "Specify a configuration JSON file"})
	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	options := Options{Directory: *dirPtr, Config: *configPtr}
	err = core(options)
	if err != nil {
		log.Fatal(err)
	}
}

func core(options Options) error {

	fmt.Println("Working directory: ", options.Directory)
	fmt.Println("Configuration file: ", options.Config)

	config := Configuration{}
	err := gonfig.GetConf(options.Config, &config)

	client, err := mongo.NewClient(config.URI)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	database := client.Database(config.Database)
	collection := database.Collection(config.Collection)
	cursor, err := collection.Watch(context.Background(), nil)

	if err != nil {
		log.Fatal(err)
	}
	initiateActionHandler(options.Directory)
	for {
		getNextChange(cursor)
		doc := bson.NewDocument()
		err := cursor.Decode(doc)
		if err != nil {
			log.Fatal(err)
		}
		handlingAction(doc)
	}

	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func getNextChange(cursor mongo.Cursor) {
	for !cursor.Next(context.Background()) {
	}
}

func initiateActionHandler(directory string) {
	sysPath := python.PySys_GetObject("path")
	programName := python.PyString_FromString(directory)
	err := python.PyList_Append(sysPath, programName)
	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < python.PyList_Size(sysPath); i++ {
		item := python.PyList_GetItem(sysPath, i)
		fmt.Println(python.PyString_AsString(item))
	}
}

func handlingAction(doc *bson.Document) {
	fmt.Println("Handling: ", doc.ToExtJSON(true))
	moduleTest := python.PyImport_ImportModule("actions")
	if moduleTest == nil {
		panic("Couldn't import `actions` module.")
	}

	fmt.Println(moduleTest)
	onInsert := moduleTest.GetAttrString("on_insert")
	if onInsert == nil {
		panic("Couldn't find `on_insert()` from module.")
	}

	args := python.PyTuple_New(1)
	python.PyTuple_SetItem(args, 0, python.PyString_FromString(doc.ToExtJSON(true)))
	res := onInsert.Call(args, python.Py_None)
	fmt.Println(python.PyInt_AsLong(res))

	args.DecRef()
	onInsert.DecRef()
	moduleTest.DecRef()
}
