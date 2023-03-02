package managedproducer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/roncewind/go-util/util"
	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/workerpool"
)

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	clientPool  chan *rabbitmq.Client
	id          string
	jobQ        chan<- workerpool.Job //used to return jobs to the queue if they fail
	newClientFn func() *rabbitmq.Client
	record      rabbitmq.Record
}

// ----------------------------------------------------------------------------

// make sure RabbitJob implements the Job interface
var _ workerpool.Job = (*RabbitJob)(nil)

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *RabbitJob) Execute(ctx context.Context) error {
	client := <-j.clientPool
	err := client.Push(j.record)
	if err != nil {
		//put a new client in the pool, dropping the current one
		j.clientPool <- j.newClientFn()
		client.Close()
		return err
	}
	// return the client to the pool when done
	j.clientPool <- client
	return nil
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *RabbitJob) OnError(err error) {
	fmt.Println(j.id, "error", err)
	j.jobQ <- j
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedProducer(urlString string, numberOfWorkers int, recordchan chan rabbitmq.Record) chan struct{} {

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println(time.Now(), "Number of producer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(context.Background())

	clientPool := make(chan *rabbitmq.Client, numberOfWorkers)
	newClientFn := func() *rabbitmq.Client { return rabbitmq.NewClient(urlString) }

	// populate an initial client pool
	go createClients(ctx, clientPool, numberOfWorkers, newClientFn)

	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	jobQ := make(chan workerpool.Job, numberOfWorkers)
	go loadJobQueue(ctx, clientPool, newClientFn, jobQ, recordchan)

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
func createClients(ctx context.Context, rabbitmqClients chan *rabbitmq.Client, numOfClients int, newClientFn func() *rabbitmq.Client) {
	for i := 0; i < numOfClients; i++ {
		rabbitmqClients <- newClientFn()
	}
	fmt.Println(time.Now(), numOfClients, "rabbitMQ clients created")
}

// ----------------------------------------------------------------------------

// create Jobs and put them into the job queue
func loadJobQueue(ctx context.Context, clientPool chan *rabbitmq.Client, newClientFn func() *rabbitmq.Client, jobQ chan workerpool.Job, recordchan chan rabbitmq.Record) {
	jobCount := 0
	for record := range util.OrDone(ctx, recordchan) {
		jobQ <- &RabbitJob{
			clientPool:  clientPool,
			id:          record.GetMessageId(),
			jobQ:        jobQ,
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
