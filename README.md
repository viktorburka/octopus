# octopus [![Build Status](https://travis-ci.com/viktorburka/octopus.svg?branch=master)](https://travis-ci.com/viktorburka/octopus)

octopus is a concurrent streaming file transfer agent written in Go that supports multiple protocols. It was written to handle large files in the fastest possible manner. Streaming means that it will start uploading concurrently with download process without a need for a whole file download to complete and it will try to perform both download and upload in multiple threads to achieve even faster transfer. It works by polling tasks created by octopus-server from MongoDB.

Currently only s3 protocol supports both multi-threaded download and upload along with the streaming by leveraging such features of s3 as ranged download and multipart upload.

The following protocols are currently supported:

- Download: s3, http, https
- Upload: s3, local file system

local file system is for debugging purposes mostly to put downloaded file in a certain location on local file system for verification or debugging.

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
