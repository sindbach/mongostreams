# mongostreams

A simple executable to handle MongoDB change streams events. 

### Build 

```sh
cd ./mongostreams/mongostreams
dep ensure -v 
go build -v
```

### Options 

```
usage: mongostreams [-h|--help] -d|--directory "<value>" -c|--config "<value>"

Arguments:

  -h  --help       Print help information
  -d  --directory  Specify a working directory
  -c  --config     Specify a configuration JSON file
```
