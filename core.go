package mongostreams

import (
	"context"
	"log"
	"os"
	"path"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/changestreamopt"
	"github.com/mongodb/mongo-go-driver/mongo/mongoopt"
	"github.com/sbinet/go-python"
	"github.com/tkanos/gonfig"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

func init() {
	err := python.Initialize()
	if err != nil {
		panic(err.Error())
	}
	zlog := zap.NewExample()
	defer zlog.Sync()
	logger = zlog.Sugar()
}

func Execute(options Options) error {

	config := configuration{}
	err := gonfig.GetConf(options.Config, &config)

	cacheFile := path.Join(options.Directory, tokenFile)
	var watchOptions *changestreamopt.ChangeStreamBundle
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		watchOptions = changestreamopt.BundleChangeStream(changestreamopt.FullDocument(mongoopt.Default))
		logger.Info("Creating resume token cache ")
		os.Create(cacheFile)
	} else {
		logger.Info("Resume token cache found")
		token := loadResumeToken(cacheFile)
		watchOptions = changestreamopt.BundleChangeStream(
			changestreamopt.ResumeAfter(token),
			changestreamopt.FullDocument(mongoopt.Default),
		)
	}
	client, err := mongo.NewClient(config.URI)
	if err != nil {
		logger.Infof("Failed to create a new MongoClient",
			"err", err, "uri", config.URI)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		logger.Infof("Failed to connect to MongoDB",
			"err", err, "uri", config.URI)
	}

	database := client.Database(config.Database)
	collection := database.Collection(config.Collection)
	cursor, err := collection.Watch(context.Background(), nil, watchOptions)
	if err != nil {
		logger.Infof("Failed to open ChangeStream",
			"err", err, "collection", config.Collection)
	}

	initiateActionHandler(options.Directory)

	logger.Info("Waiting for incoming change streams ...")
	for {
		getNextChange(cursor)
		doc := bson.NewDocument()
		err := cursor.Decode(doc)
		if err != nil {
			log.Fatal(err)
		}
		saveResumeToken(cacheFile, doc)
		actionHandler(doc)
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
}

func actionHandler(doc *bson.Document) {
	module := python.PyImport_ImportModule(pluginFile)
	if module == nil {
		python.PyErr_Print()
		logger.Infof("Failed to import plugin module",
			"file", pluginFile)
	}

	args := python.PyTuple_New(1)
	python.PyTuple_SetItem(args, 0, python.PyString_FromString(doc.ToExtJSON(true)))

	operationType := doc.Lookup("operationType")
	switch event := operationType.StringValue(); event {
	case "insert":
		insert := module.GetAttrString("on_insert")
		if insert == nil {
			logger.Infof("Failed to find 'on_insert()' from plugin")
		}
		res := insert.Call(args, python.Py_None)
		logger.Info(python.PyInt_AsLong(res))
	case "update":
		update := module.GetAttrString("on_update")
		if update == nil {
			logger.Infof("Failed to find 'on_update()' from plugin")
		}
		res := update.Call(args, python.Py_None)
		logger.Info(python.PyInt_AsLong(res))
	case "replace":
		replace := module.GetAttrString("on_replace")
		if replace == nil {
			logger.Infof("Failed to find 'on_replace()' from plugin")
		}
		res := replace.Call(args, python.Py_None)
		logger.Info(python.PyInt_AsLong(res))
	case "delete":
		delete := module.GetAttrString("on_delete")
		if delete == nil {
			logger.Infof("Failed to find 'on_delete()' from plugin")
		}
		res := delete.Call(args, python.Py_None)
		logger.Info(python.PyInt_AsLong(res))
	case "invalidate":
		invalidate := module.GetAttrString("on_invalidate")
		if invalidate == nil {
			logger.Infof("Failed to find 'on_invalidate()' from plugin")
		}
		res := invalidate.Call(args, python.Py_None)
		logger.Info(python.PyInt_AsLong(res))
	default:
		logger.Infow("Failed to unknown event type encountered",
			"event", event)
	}
}
