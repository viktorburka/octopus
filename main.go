package main

import (
	"context"
	"github.com/kelseyhightower/envconfig"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Settings struct {
	Database       string `default:"octopus"`
	Collection     string `default:"jobs"`
	DbConnection   string `default:"mongodb://localhost:27017"`
	EventLoopSleep time.Duration `default:"1s"`
	DbOpTimeout    time.Duration `default:"5s"`
}

func main()  {

	var s Settings

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

func eventLoop(ctx context.Context, client *mongo.Client, s Settings) {

	const MaxProcessing = 10

	collection := client.Database(s.Database).Collection(s.Collection)
	ticker := time.NewTicker(s.EventLoopSleep)

	numProcessing := 0
	proc := make(chan struct{err error})

	for {
		select {
		case <-ticker.C:
			if numProcessing < MaxProcessing {
				numProcessing++
				go process(ctx, collection, s.DbOpTimeout, proc)
			}
		case <-proc:
			numProcessing--
		case <-ctx.Done():
			log.Println("Done")
			return
		}
	}
}

func process(ctx context.Context, collection *mongo.Collection, t time.Duration, proc chan struct{err error}) {

	var processError error
	defer func() {proc<-struct{err error}{err:processError}}()

	timeout, cancel := context.WithTimeout(ctx, t)
	defer cancel()

	cur, err := collection.Find(timeout, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer cur.Close(timeout)

	for cur.Next(context.Background()) {
		elem := bson.NewDocument()
		if err := cur.Decode(elem); err != nil {
			log.Println(err)
			return
		}
		log.Println("Id:", elem.Lookup("_id").ObjectID(), ", srcUrl:", elem.Lookup("srcUrl").StringValue())
	}
}
