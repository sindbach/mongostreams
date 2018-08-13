package mongostreams

import (
	"bytes"
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
	"github.com/robertkrimen/otto"
	"github.com/tkanos/gonfig"
)

func Execute(options Options) error {

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
	actions := initiateActionHandler(options.Directory)

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

func initiateActionHandler(directory string) *otto.Otto {
	f, err := os.Open(path.Join(directory, "actions.js"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Fatal(err)
	}
	defer f.Close()
	buff := bytes.NewBuffer(nil)

	if _, err := buff.ReadFrom(f); err != nil {
		log.Fatal(err)
	}
	runtime := otto.New()

	if _, err := runtime.Run(buff.String()); err != nil {
		log.Fatal(err)
	}
	return runtime

}

func actionHandler(channelStreams <-chan bson.Document, wg *sync.WaitGroup, actions *otto.Otto) {
	defer wg.Done()
	for doc := range channelStreams {
		d, err := actions.ToValue(doc.ToExtJSON(true))
		if err != nil {
			log.Fatal(err)
		}
		operationType := doc.Lookup("operationType")
		switch event := operationType.StringValue(); event {
		case "insert":
			result, err := actions.Call("onInsert", nil, d)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result.ToBoolean())
		case "update":
			result, err := actions.Call("onUpdate", nil, d)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result.ToBoolean())
		case "replace":
			result, err := actions.Call("onReplace", nil, d)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result.ToBoolean())
		case "delete":
			result, err := actions.Call("onDelete", nil, d)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result.ToBoolean())
		case "invalidate":
			result, err := actions.Call("onInvalidate", nil, d)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result.ToBoolean())
		default:
			fmt.Println("Unknown event type:", event)
		}
	}
}
