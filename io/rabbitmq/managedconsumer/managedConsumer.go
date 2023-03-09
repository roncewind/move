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

var jobPool chan *RabbitJob

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	delivery  amqp.Delivery
	engine    *g2api.G2engine
	id        int
	usedCount int
	withInfo  bool
}

// ----------------------------------------------------------------------------

// make sure RabbitJob implements the Job interface
var _ workerpool.Job = (*RabbitJob)(nil)

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *RabbitJob) Execute(ctx context.Context) error {
	// increment the number of times this job struct was used and return to the pool
	defer func() {
		j.usedCount++
		jobPool <- j
	}()
	// fmt.Printf("Received a message- msgId: %s, msgCnt: %d, ConsumerTag: %s\n", id, j.delivery.MessageCount, j.delivery.ConsumerTag)
	record, newRecordErr := szrecord.NewRecord(string(j.delivery.Body))
	if newRecordErr == nil {
		loadID := "Load"
		if j.withInfo {
			var flags int64 = 0
			_, withInfoErr := (*j.engine).AddRecordWithInfo(ctx, record.DataSource, record.Id, record.Json, loadID, flags)
			if withInfoErr != nil {
				fmt.Println(time.Now(), "Error adding record:", j.delivery.MessageId, "error:", withInfoErr)
				fmt.Printf("Record in error: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
				return withInfoErr
			}
			//TODO:  what do we do with the "withInfo" data here?
			// fmt.Printf("Record added: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
			// fmt.Printf("WithInfo: %s\n", withInfo)
		} else {
			time.Sleep(1 * time.Millisecond)
			// addRecordErr := (*j.engine).AddRecord(ctx, record.DataSource, record.Id, record.Json, loadID)
			// if addRecordErr != nil {
			// 	fmt.Println(time.Now(), "Error adding record:", j.delivery.MessageId, "error:", addRecordErr)
			// 	fmt.Printf("Record in error: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
			// 	return addRecordErr
			// }
		}

		// when we successfully process a delivery, acknowledge it.
		j.delivery.Ack(false)
	} else {
		// logger.LogMessageFromError(MessageIdFormat, 2001, "create new szRecord", newRecordErr)
		fmt.Println(time.Now(), "Invalid delivery from RabbitMQ:", j.delivery.MessageId)
		// when we get an invalid delivery, negatively acknowledge and send to the dead letter queue
		j.delivery.Nack(false, false)
	}
	return nil
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *RabbitJob) OnError(err error) {
	// TODO: look at the error codes and only requeue when they are retryable
	// for now, just requeue if they haven't been requeued before
	if j.delivery.Redelivered {
		j.delivery.Nack(false, false)
	} else {
		j.delivery.Nack(false, true)
	}
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine *g2api.G2engine, withInfo bool) chan struct{} {

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println(time.Now(), "Number of consumer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(ctx)

	newClientFn := func() *rabbitmq.Client { return rabbitmq.NewClient(urlString) }

	jobPool = make(chan *RabbitJob, numberOfWorkers)
	for i := 0; i < numberOfWorkers; i++ {
		jobPool <- &RabbitJob{
			engine:    g2engine,
			id:        i,
			usedCount: 0,
			withInfo:  withInfo,
		}
	}

	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	jobQ := make(chan workerpool.Job, numberOfWorkers)
	go loadJobQueue(ctx, newClientFn, jobQ, numberOfWorkers)

	// create and start up the workerpool
	wp, _ := workerpool.NewWorkerPool(numberOfWorkers, jobQ)
	wp.Start(ctx)

	// clean up after ourselves
	cleanup := func() {
		cancel()
		close(jobPool)
		close(jobQ)
		// drain the job pool
		var job *RabbitJob
		ok := true
		for ok {
			job, ok = <-jobPool
			fmt.Println("Job:", job.id, "used:", job.usedCount)
		}
	}

	// when shutdown signalled by OS signal, wait for 5 seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cleanup, 5*time.Second)

	// return blocking channel
	return sigShutdown
}

// ----------------------------------------------------------------------------

// create Jobs and put them into the job queue
func loadJobQueue(ctx context.Context, newClientFn func() *rabbitmq.Client, jobQ chan workerpool.Job, prefetch int) {
	client := newClientFn()
	defer client.Close()

	deliveries, err := client.Consume(prefetch)
	if err != nil {
		fmt.Println(time.Now(), "Error getting delivery channel:", err)
		return
	}

	jobCount := 0
	//PONDER: what if something fails here?  how can we recover?
	for delivery := range util.OrDone(ctx, deliveries) {
		job := <-jobPool
		job.delivery = delivery
		jobQ <- job
		jobCount++
		if jobCount%10000 == 0 {
			fmt.Println(time.Now(), "Jobs added to job queue:", jobCount)
		}
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
