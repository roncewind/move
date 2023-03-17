package managedproducer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/queues/rabbitmq"
	"github.com/roncewind/go-util/util"
	"github.com/roncewind/workerpool"
)

var clientPool chan *rabbitmq.Client
var jobQ chan workerpool.Job

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

// make sure RabbitJob implements the Job interface
var _ workerpool.Job = (*RabbitJob)(nil)

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *RabbitJob) Execute(ctx context.Context) (err error) {
	client := <-clientPool
	err = client.Push(j.record)
	if err != nil {
		err = ManagedProducerError{util.WrapError(err, err.Error())}
		//put a new client in the pool, dropping the current one
		newClient, newClientErr := j.newClientFn()
		if newClientErr != nil {
			err = ManagedProducerError{util.WrapError(newClientErr, newClientErr.Error())}
		} else {
			clientPool <- newClient
		}
		client.Close()
		return
	}
	// return the client to the pool when done
	clientPool <- client
	return
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *RabbitJob) OnError(err error) {
	fmt.Println(time.Now(), j.record.GetMessageId(), "error", err)
	jobQ <- j
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan chan queues.Record) chan struct{} {

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

	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	jobQ = make(chan workerpool.Job, numberOfWorkers)
	go loadJobQueue(ctx, clientPool, newClientFn, recordchan)

	// create and start up the workerpool
	wp, _ := workerpool.NewWorkerPool(numberOfWorkers, jobQ)
	wp.Start(ctx)

	// clean up after ourselves
	cleanup := func() {
		cancel()
		fmt.Println(time.Now(), "Cleaup job queue and client pool.")
		close(jobQ)
		close(clientPool)
		// drain the client pool, closing rabbit mq connections
		for len(clientPool) > 0 {
			client, ok := <-clientPool
			if ok && client != nil {
				client.Close()
			}
		}
	}

	// when shutdown signalled by OS signal, wait for 5 seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cleanup, 5*time.Second)

	// return blocking channel
	return sigShutdown
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

// ----------------------------------------------------------------------------

// create Jobs and put them into the job queue
func loadJobQueue(ctx context.Context, clientPool chan *rabbitmq.Client, newClientFn func() (*rabbitmq.Client, error), recordchan chan queues.Record) {
	jobCount := 0
	for record := range util.OrDone(ctx, recordchan) {
		jobQ <- &RabbitJob{
			id:          jobCount,
			newClientFn: newClientFn,
			record:      record,
		}
		jobCount++
	}
	fmt.Println(time.Now(), "Total number of jobs:", jobCount)
}

// ----------------------------------------------------------------------------

// gracefulShutdown waits for terminating syscalls then signals workers to shutdown
func gracefulShutdown(cleanup func(), timeout time.Duration) chan struct{} {
	wait := make(chan struct{})

	go func() {
		defer close(wait)
		sig := make(chan os.Signal, 1)
		defer close(sig)

		// PONDER: add any other syscalls?
		// SIGHUP - hang up, lost controlling terminal
		// SIGINT - interrupt (ctrl-c)
		// SIGQUIT - quit (ctrl-\)
		// SIGTERM - request to terminate
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
		killsig := <-sig
		switch killsig {
		case syscall.SIGINT:
			fmt.Println(time.Now(), "Killed with ctrl-c")
		case syscall.SIGTERM:
			fmt.Println(time.Now(), "Killed with request to terminate")
		case syscall.SIGQUIT:
			fmt.Println(time.Now(), "Killed with ctrl-\\")
		case syscall.SIGHUP:
			fmt.Println(time.Now(), "Killed with hang up")
		}

		// set timeout for the cleanup to be done to prevent system hang
		timeoutSignal := make(chan struct{})
		timeoutFunc := time.AfterFunc(timeout, func() {
			fmt.Printf("Timeout %.1fs have elapsed, force exit\n", timeout.Seconds())
			close(timeoutSignal)
		})

		defer timeoutFunc.Stop()

		// clean-up time
		fmt.Println(time.Now(), "Shutdown signalled, time to clean-up")
		cleanup()

		// wait for timeout to finish
		<-timeoutSignal
		wait <- struct{}{}
	}()

	return wait
}
