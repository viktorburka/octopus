# octopus [![Build Status](https://travis-ci.com/viktorburka/octopus.svg?branch=master)](https://travis-ci.com/viktorburka/octopus)

octopus is a file transfer agent written in Go that supports multiple protocols. It works by polling tasks created by octopus-server from MongoDB.

## Build

The application was written and tested with GoLang 1.11 but other versions of GoLang may work as well. To build: 

```bash
go get -d -v ./...
go test -v ./...
go build
```

## Configure

The following environment variables can be used to configure the application:

| Name                   | Default                   | Description                |
|------------------------|---------------------------|----------------------------|
| OCTOPUS_DBCONNECTION   | mongodb://localhost:27017 | MongoDB connection URI     |
| OCTOPUS_DATABASE       | octopus                   | MongoDB database name      |
| OCTOPUS_COLLECTION     | jobs                      | MongoDB jobs collection    |
| OCTOPUS_DBOPTIMEOUT    | 5s                        | Database operation timeout |
| OCTOPUS_EVENTLOOPSLEEP | 1s                        | Job poll interval          |

## Run

At this point only running the binary manually is supported. Go to the folder with the binary and run:

```bash
./octopus
```
