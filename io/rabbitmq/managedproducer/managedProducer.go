package managedproducer

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/queues/rabbitmq"
	"github.com/roncewind/go-util/util"
	"github.com/sourcegraph/conc/pool"
)

var clientPool chan *rabbitmq.Client

type ManagedProducerError struct {
	error
}

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	id          int
	newClientFn func() (*rabbitmq.Client, error)
	record      queues.Record
}

// ----------------------------------------------------------------------------

// read a record from the record channel and push it to the RabbitMQ queue
func processRecord(ctx context.Context, record queues.Record, newClientFn func() (*rabbitmq.Client, error)) (err error) {
	client := <-clientPool
	err = client.Push(record)
	if err != nil {
		// on error, create a new RabbitMQ client
		err = ManagedProducerError{util.WrapError(err, err.Error())}
		//put a new client in the pool, dropping the current one
		newClient, newClientErr := newClientFn()
		if newClientErr != nil {
			err = ManagedProducerError{util.WrapError(newClientErr, newClientErr.Error())}
		} else {
			clientPool <- newClient
		}
		// make sure to close the old client
		client.Close()
		return
	}
	// return the client to the pool when done
	clientPool <- client
	return
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan chan queues.Record) {

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println(time.Now(), "Number of producer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(ctx)

	clientPool = make(chan *rabbitmq.Client, numberOfWorkers)
	newClientFn := func() (*rabbitmq.Client, error) { return rabbitmq.NewClient(urlString) }

	// populate an initial client pool
	go createClients(ctx, numberOfWorkers, newClientFn)

	p := pool.New().WithMaxGoroutines(numberOfWorkers)
	for record := range recordchan {
		record := record
		p.Go(func() {
			err := processRecord(ctx, record, newClientFn)
			if err != nil {
				// TODO:  on error should the record get put back into the record channel?
				fmt.Println("Worker error:", err)
				fmt.Println("Failed to move record:", record.GetMessageId())
			}
		})
	}

	// Wait for all the records in the record channel to be processed
	p.Wait()

	// clean up after ourselves
	cancel()
	fmt.Println(time.Now(), "Cleaup job queue and client pool.")
	close(clientPool)
	// drain the client pool, closing rabbit mq connections
	for len(clientPool) > 0 {
		client, ok := <-clientPool
		if ok && client != nil {
			client.Close()
		}
	}

}

// ----------------------------------------------------------------------------

// create a number of clients and put them into the client queue
func createClients(ctx context.Context, numOfClients int, newClientFn func() (*rabbitmq.Client, error)) error {
	countOfClientsCreated := 0
	var errorStack error = nil
	for i := 0; i < numOfClients; i++ {
		client, err := newClientFn()
		if err != nil {
			errorStack = ManagedProducerError{util.WrapError(err, err.Error())}
		} else {
			countOfClientsCreated++
			clientPool <- client
		}
	}
	fmt.Println(time.Now(), countOfClientsCreated, "rabbitMQ clients created,", numOfClients, "requested")
	return errorStack
}
