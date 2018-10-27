package main

import (
    "context"
    "fmt"
    "github.com/mongodb/mongo-go-driver/bson"
    "github.com/mongodb/mongo-go-driver/bson/objectid"
    "github.com/mongodb/mongo-go-driver/mongo"
    "github.com/mongodb/mongo-go-driver/mongo/findopt"
    "github.com/mongodb/mongo-go-driver/mongo/mongoopt"
	"github.com/viktorburka/octopus/netio"
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
    id objectid.ObjectID
    status string
    srcUrl string
    dstUrl string
    description string
}

type jobStatus struct {
    jobError error
}

func (j *job) init(document *bson.Document) error {

    var val *bson.Value

    if document == nil {
        return fmt.Errorf("can't start from nil document")
    }

    if val = document.Lookup("_id"); val == nil {
        return fmt.Errorf("field _id missing")
    }
    j.id = val.ObjectID()

    if val = document.Lookup("status"); val == nil {
        return fmt.Errorf("field status missing")
    }
    j.status = val.StringValue()

    if val = document.Lookup("srcUrl"); val == nil {
        return fmt.Errorf("field srcUrl missing")
    }
    j.srcUrl = val.StringValue()

    if val = document.Lookup("dstUrl"); val == nil {
        return fmt.Errorf("field dstUrl missing")
    }
    j.dstUrl = val.StringValue()

    return nil
}

func startJob(ctx context.Context, collection *mongo.Collection, t time.Duration, proc chan jobStatus) {

    var transErr error
    defer func() {proc<-jobStatus{jobError:transErr}}()

    timeout, cancel := context.WithTimeout(ctx, t)
    defer cancel()

    doc := bson.NewDocument()

    log.Println("Querying...")
    result := collection.FindOneAndUpdate(timeout,
        map[string]string{"status": created},
        map[string]map[string]string{"$set": {"status": running}},
        findopt.BundleUpdateOne().ReturnDocument(mongoopt.After))
    if err := result.Decode(doc); err != nil {
        // do not consider ErrNoDocuments as error
        if err != mongo.ErrNoDocuments {
            transErr = err
            log.Println("error:", err)
        }
        return
    }
    log.Println("Got one document. Parsing...")

    var newJob job

    if err := newJob.init(doc); err != nil {
        transErr = fmt.Errorf("received invalid json document: %v", err)
        return
    }

    log.Println("Starting transfer from", newJob.srcUrl, "to", newJob.dstUrl, "...")

    update := map[string]string{"status": complete}

    // bucket can be 'path-style' or 'virtual-hosted–style'
    // TODO: read options from the db
    opt := map[string]string{"bucketNameStyle": "path-style"}

    if err := netio.Transfer(ctx, newJob.srcUrl, newJob.dstUrl, opt); err != nil {
        transErr = fmt.Errorf("can't perform transfer: %v", err)
        //status = failed
        update["status"] = failed
        update["error"]  = transErr.Error()
    }

    // setup db op timeout again
    timeout, cancel = context.WithTimeout(ctx, t)
    defer cancel()

    result = collection.FindOneAndUpdate(timeout,
        map[string]objectid.ObjectID{"_id": newJob.id},
        map[string]map[string]string{"$set": update},
        findopt.BundleUpdateOne().ReturnDocument(mongoopt.After))

    if err := result.Decode(doc); err != nil {
        transErr = fmt.Errorf("error updating job status: %v", err)
        return
    }

    log.Println("Finished transfer")
}
