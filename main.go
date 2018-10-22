package main

import (
    "context"
    "fmt"
    "github.com/kelseyhightower/envconfig"
    "github.com/mongodb/mongo-go-driver/bson"
    "github.com/mongodb/mongo-go-driver/mongo"
    "github.com/mongodb/mongo-go-driver/mongo/findopt"
    "github.com/mongodb/mongo-go-driver/mongo/mongoopt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
)

type settings struct {
    Database       string `default:"octopus"`
    Collection     string `default:"jobs"`
    DbConnection   string `default:"mongodb://localhost:27017"`
    EventLoopSleep time.Duration `default:"1s"`
    DbOpTimeout    time.Duration `default:"5s"`
}

type jobStatus struct{
	jobError error
}

func main()  {

    var s settings

    err := envconfig.Process("octopus", &s)
    if err != nil {
        log.Fatal(err.Error())
    }

    sigtermCtx, cancel := context.WithCancel(context.Background())
    go func() {
        sigterm := make(chan os.Signal, 1)
        signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
        <-sigterm
        cancel()
    }()

    ctx, releaseContext := context.WithTimeout(context.Background(), s.DbOpTimeout)
    defer releaseContext()

    client, err := mongo.NewClient(s.DbConnection)
    if err != nil {
        log.Fatal(err)
    }

    err = client.Connect(ctx)
    if err != nil {
        log.Fatal(err)
    }

    eventLoop(sigtermCtx, client, s)
}

func eventLoop(ctx context.Context, client *mongo.Client, s settings) {

    const MaxJobs = 3

    collection := client.Database(s.Database).Collection(s.Collection)
    ticker := time.NewTicker(s.EventLoopSleep)

    jobs := 0
    proc := make(chan jobStatus)

    for {
        select {
        case <-ticker.C:
            if jobs < MaxJobs {
                jobs++
                go startJob(ctx, collection, s.DbOpTimeout, proc)
            }
        case status := <-proc:
            jobs--
            if status.jobError != nil {
            	log.Println("error:", status.jobError)
			}
        case <-ctx.Done():
            log.Println("Done")
            return
        }
    }
}

func startJob(ctx context.Context, collection *mongo.Collection, t time.Duration, proc chan jobStatus) {

    var transErr error
    defer func() {proc<-jobStatus{jobError:transErr}}()

    timeout, cancel := context.WithTimeout(ctx, t)
    defer cancel()

    doc := bson.NewDocument()

    log.Println("Querying...")
    result := collection.FindOneAndUpdate(timeout,
                                map[string]string{"status": "Created"},
                                map[string]map[string]string{"$set": {"status": "Running"}},
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

    if err := transfer(ctx, newJob.srcUrl, newJob.dstUrl); err != nil {
        transErr = fmt.Errorf("can't perform transfer: %v", err)
        return
    }

    log.Println("Finished transfer")
}
