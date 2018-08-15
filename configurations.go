package mongostreams

type configuration struct {
	URI        string
	Database   string
	Collection string
	Workers    int
}
