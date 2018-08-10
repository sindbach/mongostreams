package mongostreams 

type Configuration struct {
	URI        string
	Database   string
	Collection string
	Workers    int
}