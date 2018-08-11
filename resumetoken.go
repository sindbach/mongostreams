package mongostreams

import (
	"os"

	"github.com/mongodb/mongo-go-driver/bson"
)

func loadResumeToken(cacheFile string) *bson.Document {
	doc := bson.NewDocument()
	file, err := os.Open(cacheFile)
	if err == nil {
		data := make([]byte, 400)
		_, err := file.Read(data)
		if err != nil {
			panic(err)
		}
		err = doc.UnmarshalBSON(data)
		if err != nil {
			panic(err)
		}
	}
	file.Close()
	return doc
}

func saveResumeToken(cacheFile string, doc *bson.Document) {
	token := doc.Lookup("_id").MutableDocument()
	file, err := os.OpenFile(cacheFile, os.O_WRONLY, os.ModeAppend)
	if err == nil {
		cache, err := token.MarshalBSON()
		_, err = file.Write(cache)
		if err != nil {
			panic(err)
		}
	}
	file.Close()
}
