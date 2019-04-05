package main

import (
	"context"
	"fmt"
	"github.com/viktorburka/octopus/netio"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "log"
	"time"
)

const (
    created  string = "Created"
    running  string = "Running"
    complete string = "Complete"
    failed   string = "Failed"
)

type job struct {
    Id primitive.ObjectID `json:"_id"         bson:"_id"`
    Status string         `json:"status"      bson:"status"`
    SrcUrl string         `json:"srcUrl"      bson:"srcUrl"`
    DstUrl string         `json:"dstUrl"      bson:"dstUrl"`
    Description string    `json:"description" bson:"description"`
}

type jobStatus struct {
    jobError error
}

func startJob(ctx context.Context, collection *mongo.Collection, t time.Duration, proc chan jobStatus) {

    var transErr error
    defer func() {proc<-jobStatus{jobError:transErr}}()

    timeout, cancel := context.WithTimeout(ctx, t)
    defer cancel()

    docOpt := &options.FindOneAndUpdateOptions{}
	docOpt.SetReturnDocument(options.After)

    var newJob job

	log.Println("Querying new jobs...")
    result := collection.FindOneAndUpdate(timeout,
        bson.M{"status": created},
		map[string]bson.M{"$set": {"status": running}},
		docOpt)

    log.Println("Parsing...")
    if err := result.Decode(&newJob); err != nil {
        // do not consider ErrNoDocuments as error
        if err != mongo.ErrNoDocuments {
            transErr = err
            log.Println("error:", err)
        } else {
            log.Println("No new jobs. Skipping...")
        }
        return
    }

    log.Println("Starting transfer from", newJob.SrcUrl, "to", newJob.DstUrl)

    update := map[string]string{"status": complete}

    // bucket can be 'path-style' or 'virtual-hosted–style'
    // TODO: read options from the db
    opt := map[string]string{"bucketNameStyle": "path-style"}

    if err := netio.Transfer(ctx, newJob.SrcUrl, newJob.DstUrl, opt); err != nil {
        transErr = fmt.Errorf("can't perform transfer: %v", err)
        update["status"] = failed
        update["error"]  = transErr.Error()
    }

    // setup db op timeout again
    timeout, cancel = context.WithTimeout(ctx, t)
    defer cancel()

    result = collection.FindOneAndUpdate(timeout,
        map[string]primitive.ObjectID{"_id": newJob.Id},
        map[string]map[string]string{"$set": update})

    if err := result.Err(); err != nil {
        transErr = fmt.Errorf("error updating job status: %v", err)
    }

    if transErr != nil {
        log.Println("Transfer interrupted")
    } else {
        log.Println("Finished transfer")
    }
}
