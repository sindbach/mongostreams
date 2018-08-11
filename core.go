package mongostreams

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

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

	actions := initiateActionHandler(options.Directory)

	config := Configuration{}
	err := gonfig.GetConf(options.Config, &config)

	cacheFile := path.Join(options.Directory, "token.bson")
	var watchOptions *changestreamopt.ChangeStreamBundle
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		watchOptions = changestreamopt.BundleChangeStream(changestreamopt.FullDocument(mongoopt.Default))
		fmt.Println("Creating resume token cache")
		os.Create(cacheFile)
	} else {
		fmt.Println("Resume token exists, loading:")
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

	channelStreams := make(chan bson.Document)

	wg := new(sync.WaitGroup)

	for w := 1; w <= config.Workers; w++ {
		wg.Add(1)
		go actionHandler(channelStreams, wg, actions)
	}

	fmt.Println("Waiting for incoming change streams ...")
	for {
		getNextChange(cursor)
		doc := bson.NewDocument()
		err := cursor.Decode(doc)
		if err != nil {
			log.Fatal(err)
		}

		saveResumeToken(cacheFile, doc)
		channelStreams <- *doc
	}

	wg.Wait()
	close(channelStreams)
	return nil
}

func getNextChange(cursor mongo.Cursor) {
	for !cursor.Next(context.Background()) {
	}
}

func initiateActionHandler(directory string) *PyActions {
	sysPath := python.PySys_GetObject("path")
	programName := python.PyString_FromString(directory)
	err := python.PyList_Append(sysPath, programName)
	if err != nil {
		panic(err.Error())
	}

	moduleTest := python.PyImport_ImportModule("actions")
	if moduleTest == nil {
		python.PyErr_Print()
		panic("Couldn't import `actions` module.")
	}
	actions := PyActions{}
	actions.OnInsert = moduleTest.GetAttrString("on_insert")
	if actions.OnInsert == nil {
		panic("Couldn't find `on_insert()` from module.")
	}
	actions.OnUpdate = moduleTest.GetAttrString("on_update")
	if actions.OnUpdate == nil {
		panic("Couldn't find `on_update()` from module.")
	}
	actions.OnReplace = moduleTest.GetAttrString("on_replace")
	if actions.OnReplace == nil {
		panic("Couldn't find `on_replace()` from module.")
	}
	actions.OnDelete = moduleTest.GetAttrString("on_delete")
	if actions.OnDelete == nil {
		panic("Couldn't find `on_delete()` from module.")
	}
	actions.OnInvalidate = moduleTest.GetAttrString("on_invalidate")
	if actions.OnInvalidate == nil {
		panic("Couldn't find `on_invalidate()` from module.")
	}
	return &actions
}

func actionHandler(channelStreams <-chan bson.Document, wg *sync.WaitGroup, actions *PyActions) {
	defer wg.Done()

	for doc := range channelStreams {
		args := python.PyTuple_New(1)
		python.PyTuple_SetItem(args, 0, python.PyString_FromString(doc.ToExtJSON(true)))

		operationType := doc.Lookup("operationType")
		switch event := operationType.StringValue(); event {
		case "insert":
			res := actions.OnInsert.Call(args, python.Py_None)
			fmt.Println(python.PyInt_AsLong(res))
		case "update":
			res := actions.OnUpdate.Call(args, python.Py_None)
			fmt.Println(python.PyInt_AsLong(res))
		case "replace":
			res := actions.OnReplace.Call(args, python.Py_None)
			fmt.Println(python.PyInt_AsLong(res))
		case "delete":
			res := actions.OnDelete.Call(args, python.Py_None)
			fmt.Println(python.PyInt_AsLong(res))
		case "invalidate":
			res := actions.OnInvalidate.Call(args, python.Py_None)
			fmt.Println(python.PyInt_AsLong(res))
		default:
			fmt.Println("Unknown event type:", event)
		}

		args.DecRef()
	}
}
