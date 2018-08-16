package mongostreams

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/changestreamopt"
	"github.com/mongodb/mongo-go-driver/mongo/mongoopt"
	"github.com/sbinet/go-python"
	"github.com/tkanos/gonfig"
)

func init() {
	err := python.Initialize()
	if err != nil {
		panic(err.Error())
	}
}

func Execute(options Options) error {
	config := configuration{}
	err := gonfig.GetConf(options.Config, &config)

	cacheFile := path.Join(options.Directory, tokenFile)
	var watchOptions *changestreamopt.ChangeStreamBundle
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		watchOptions = changestreamopt.BundleChangeStream(changestreamopt.FullDocument(mongoopt.Default))
		fmt.Println("Creating resume token cache")
		os.Create(cacheFile)
	} else {
		fmt.Println("Resume token exists, loading ...")
		token := loadResumeToken(cacheFile)
		watchOptions = changestreamopt.BundleChangeStream(
			changestreamopt.ResumeAfter(token),
			changestreamopt.FullDocument(mongoopt.Default),
		)
	}
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
	cursor, err := collection.Watch(context.Background(), nil, watchOptions)

	if err != nil {
		log.Fatal(err)
	}

	pluginModule := initiateActionHandler(options.Directory)

	fmt.Println("Waiting for incoming change streams ...")
	for {
		getNextChange(cursor)
		doc := bson.NewDocument()
		err := cursor.Decode(doc)
		if err != nil {
			log.Fatal(err)
		}
		saveResumeToken(cacheFile, doc)
		actionHandler(doc, *pluginModule)
	}

	return nil
}

func getNextChange(cursor mongo.Cursor) {
	for !cursor.Next(context.Background()) {
	}
}

func initiateActionHandler(directory string) *python.PyObject {
	sysPath := python.PySys_GetObject("path")
	programName := python.PyString_FromString(directory)
	err := python.PyList_Append(sysPath, programName)
	if err != nil {
		panic(err.Error())
	}
	pluginModule := python.PyImport_ImportModule(pluginFile)
	if pluginModule == nil {
		python.PyErr_Print()
		panic("Couldn't import `actions` module.")
	}
	return pluginModule
}

func actionHandler(doc *bson.Document, module python.PyObject) {
	args := python.PyTuple_New(1)
	python.PyTuple_SetItem(args, 0, python.PyString_FromString(doc.ToExtJSON(true)))

	operationType := doc.Lookup("operationType")
	switch event := operationType.StringValue(); event {
	case "insert":
		insert := module.GetAttrString("on_insert")
		if insert == nil {
			panic("Couldn't find `on_insert()` from module.")
		}
		res := insert.Call(args, python.Py_None)
		fmt.Println(python.PyInt_AsLong(res))
	case "update":
		update := module.GetAttrString("on_update")
		if update == nil {
			panic("Couldn't find `on_update()` from module.")
		}
		res := update.Call(args, python.Py_None)
		fmt.Println(python.PyInt_AsLong(res))
	case "replace":
		replace := module.GetAttrString("on_replace")
		if replace == nil {
			panic("Couldn't find `on_replace()` from module.")
		}
		res := replace.Call(args, python.Py_None)
		fmt.Println(python.PyInt_AsLong(res))
	case "delete":
		delete := module.GetAttrString("on_delete")
		if delete == nil {
			panic("Couldn't find `on_delete()` from module.")
		}
		res := delete.Call(args, python.Py_None)
		fmt.Println(python.PyInt_AsLong(res))
	case "invalidate":
		invalidate := module.GetAttrString("on_invalidate")
		if invalidate == nil {
			panic("Couldn't find `on_invalidate()` from module.")
		}
		res := invalidate.Call(args, python.Py_None)
		fmt.Println(python.PyInt_AsLong(res))
	default:
		fmt.Println("Unknown event type:", event)
	}

}
