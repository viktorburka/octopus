package main

import (
	"context"
	"github.com/kelseyhightower/envconfig"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type settings struct {
	Database       string        `default:"octopus"`
	Collection     string        `default:"jobs"`
	DbConnection   string        `default:"mongodb://localhost:27017"`
	EventLoopSleep time.Duration `default:"1s"`
	DbOpTimeout    time.Duration `default:"5s"`
}

func main() {

	var s settings

	err := envconfig.Process("octopus", &s)
	if err != nil {
		log.Fatal(err)
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

	client, err := mongo.NewClient(options.Client().ApplyURI(s.DbConnection))
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
	defer ticker.Stop() // to prevent ticker goroutine leak

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
