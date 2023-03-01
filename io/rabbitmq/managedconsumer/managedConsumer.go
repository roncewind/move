package managedconsumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roncewind/go-util/util"
	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/szrecord"
	"github.com/roncewind/workerpool"
	"github.com/senzing/g2-sdk-go/g2api"
)

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	ctx      context.Context
	delivery amqp.Delivery
	engine   g2api.G2engine
	id       string
	withInfo bool
}

// ----------------------------------------------------------------------------

// make sure RabbitJob implements the Job interface
var _ workerpool.Job = (*RabbitJob)(nil)

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *RabbitJob) Execute() error {
	j.id = j.delivery.MessageId
	// fmt.Printf("Received a message- msgId: %s, msgCnt: %d, ConsumerTag: %s\n", j.id, j.delivery.MessageCount, j.delivery.ConsumerTag)
	record, newRecordErr := szrecord.NewRecord(string(j.delivery.Body))
	if newRecordErr == nil {
		// fmt.Printf("Processing record: %s\n", record.Id)
		loadID := "Load"
		if j.withInfo {
			var flags int64 = 0
			_, withInfoErr := j.engine.AddRecordWithInfo(j.ctx, record.DataSource, record.Id, record.Json, loadID, flags)
			if withInfoErr != nil {
				fmt.Println(time.Now(), "Error adding record withInfo:", j.id, "error:", withInfoErr)
				return withInfoErr
			} else {
				//TODO:  what do we do with the record here?
				// fmt.Printf("Record added: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
				// fmt.Printf("WithInfo: %s\n", withInfo)
			}
		} else {
			addRecordErr := j.engine.AddRecord(j.ctx, record.DataSource, record.Id, record.Json, loadID)
			if addRecordErr != nil {
				fmt.Println(time.Now(), "Error adding record:", j.id, "error:", addRecordErr)
				return addRecordErr
				// } else {
				//TODO: log a positive result?
				// fmt.Printf("Record added: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
			}
		}

		// when we successfully process a delivery, acknowledge it.
		j.delivery.Ack(false)
	} else {
		// logger.LogMessageFromError(MessageIdFormat, 2001, "create new szRecord", newRecordErr)
		fmt.Println(time.Now(), "Invalid delivery from RabbitMQ:", j.id)
		// when we get an invalid delivery, negatively acknowledge and send to the dead letter queue
		j.delivery.Nack(false, false)
	}
	return nil
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *RabbitJob) OnError(err error) {
	fmt.Println(j.id, "error", err)
	// when there's an error, negatively acknowledge and requeue
	j.delivery.Nack(false, true)
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedConsumer(ctx context.Context, urlString string, numberOfWorkers int, engine g2api.G2engine, withInfo bool) chan struct{} {

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println("Number of consumer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(ctx)

	// clientPool := make(chan *rabbitmq.Client, numberOfWorkers)
	newClientFn := func() *rabbitmq.Client { return rabbitmq.NewClient(urlString) }

	// // populate an initial client pool
	// go createClients(ctx, clientPool, numberOfWorkers, newClientFn)

	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	jobQ := make(chan workerpool.Job, numberOfWorkers)
	go loadJobQueue(ctx, newClientFn, jobQ, numberOfWorkers, engine, withInfo)

	// create and start up the workerpool
	wp, _ := workerpool.NewWorkerPool(numberOfWorkers, jobQ)
	wp.Start(ctx)

	// clean up after ourselves
	cleanup := func() {
		cancel()
		close(jobQ)
		// close(clientPool)
		// // drain the client pool, closing rabbit mq connections
		// for len(clientPool) > 0 {
		// 	client := <-clientPool
		// 	client.Close()
		// }
	}

	// when shutdown signalled by OS signal, wait for 5 seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cleanup, 5*time.Second)

	// return blocking channel
	return sigShutdown
}

// // ----------------------------------------------------------------------------

// // create a number of clients and put them into the client queue
// func createClients(ctx context.Context, rabbitmqClients chan *rabbitmq.Client, numOfClients int, newClientFn func() *rabbitmq.Client) {
// 	for i := 0; i < numOfClients; i++ {
// 		rabbitmqClients <- newClientFn()
// 	}
// }

// ----------------------------------------------------------------------------

// create Jobs and put them into the job queue
func loadJobQueue(ctx context.Context, newClientFn func() *rabbitmq.Client, jobQ chan workerpool.Job, prefetch int, engine g2api.G2engine, withInfo bool) {
	client := newClientFn()
	defer client.Close()
	deliveries, err := client.Consume(prefetch)
	if err != nil {
		fmt.Println("Error getting delivery channel:", err)
		return
	}

	//PONDER: what if something fails here?  how can we recover?
	for delivery := range util.OrDone(ctx, deliveries) {
		jobQ <- &RabbitJob{
			ctx:      ctx,
			delivery: delivery,
			engine:   engine,
			withInfo: withInfo,
		}
	}
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
			fmt.Println("Killed with ctrl-c")
		case syscall.SIGTERM:
			fmt.Println("Killed with request to terminate")
		case syscall.SIGQUIT:
			fmt.Println("Killed with ctrl-\\")
		case syscall.SIGHUP:
			fmt.Println("Killed with hang up")
		}

		// set timeout for the cleanup to be done to prevent system hang
		timeoutSignal := make(chan struct{})
		timeoutFunc := time.AfterFunc(timeout, func() {
			fmt.Printf("Timeout %.1fs have elapsed, force exit\n", timeout.Seconds())
			close(timeoutSignal)
		})

		defer timeoutFunc.Stop()

		// clean-up time
		fmt.Println("Shutdown signalled, time to clean-up")
		cleanup()

		// wait for timeout to finish
		<-timeoutSignal
		wait <- struct{}{}
	}()

	return wait
}
